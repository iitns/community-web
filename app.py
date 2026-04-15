import os
import logging
import json
from datetime import datetime

import psycopg2
import psycopg2.extras
from elasticsearch import Elasticsearch
from flask import Flask, render_template, request
from redis import Redis
from redis.exceptions import RedisError

app = Flask(__name__)
logger = logging.getLogger(__name__)

PAGE_SIZE = 30
SITE_ORDER = [
    'SLR클럽',
    '뽐뿌',
    '웃대',
    '루리웹(유게)',
    '인벤',
    '보배드림',
    '펨코',
]

SITE_DISPLAY_NAMES = {
    '루리웹(유게)': '루리(육)',
}
RETIRED_SITE_NAMES = frozenset({'루리웹(유머)'})

PG_CONN = {
    'host':     os.environ.get('PG_HOST', 'postgresql-service'),
    'port':     int(os.environ.get('PG_PORT', 5432)),
    'dbname':   os.environ.get('PG_DB', 'community'),
    'user':     os.environ.get('PG_USER', 'community'),
    'password': os.environ.get('PG_PASSWORD', 'community_pass'),
}
ES_HOST = os.environ.get('ES_HOST', 'http://elasticsearch-service:9200')
ES_INDEX = 'community_articles'
REDIS_HOST = os.environ.get('REDIS_HOST', '')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
CACHE_PREFIX = 'community-web:recent'

_redis_client = None


def pg():
    return psycopg2.connect(**PG_CONN)


def es():
    return Elasticsearch(ES_HOST)


def cache():
    global _redis_client

    if not REDIS_HOST:
        return None

    if _redis_client is None:
        _redis_client = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
            socket_connect_timeout=1,
            socket_timeout=1,
        )

    return _redis_client


def site_cache_slug(site: str) -> str:
    return site or 'all'


def is_retired_site(site: str) -> bool:
    return site in RETIRED_SITE_NAMES


def filter_site_names(site_names: list[str]) -> list[str]:
    return [site_name for site_name in site_names if not is_retired_site(site_name)]


def filter_articles(articles: list[dict]) -> list[dict]:
    return [
        article
        for article in articles
        if not is_retired_site(article.get('site_name', ''))
    ]


def cached_page_has_retired_articles(payload: dict) -> bool:
    site = payload.get('site', '')
    if is_retired_site(site):
        return True

    return any(is_retired_site(article.get('site_name', '')) for article in payload.get('articles', []))


def cache_active_version_key() -> str:
    return f'{CACHE_PREFIX}:active-version'


def cache_key(version: str, *parts: str) -> str:
    return ':'.join([CACHE_PREFIX, 'v', version, *parts])


def serialize_article(article: dict) -> dict:
    serialized = dict(article)

    for field in ('published_at', 'collected_at'):
        value = serialized.get(field)
        if isinstance(value, datetime):
            serialized[field] = value.isoformat()

    return serialized


def fetch_site_names_db() -> list[str]:
    with pg() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT DISTINCT site_name FROM articles')
            site_names = {
                row[0]
                for row in cur.fetchall()
                if row[0] and not is_retired_site(row[0])
            }

    ordered = [name for name in SITE_ORDER if name in site_names]
    extras = sorted(site_names - set(ordered))
    return ordered + extras


def get_site_names() -> list[str]:
    client = cache()

    if client:
        try:
            version = client.get(cache_active_version_key())
            if version:
                cached = client.get(cache_key(version, 'site-names'))
                if cached:
                    return filter_site_names(json.loads(cached))
        except RedisError as exc:
            logger.warning('Redis site_names read failed: %s', exc)

    site_names = fetch_site_names_db()
    return site_names


def fetch_articles_page(site: str, page: int, since: datetime | None = None) -> tuple[list[dict], int]:
    if is_retired_site(site):
        return [], 0

    offset = (page - 1) * PAGE_SIZE
    conditions = []
    params = []

    if RETIRED_SITE_NAMES:
        conditions.append('NOT (site_name = ANY(%s))')
        params.append(list(RETIRED_SITE_NAMES))

    if site:
        conditions.append('site_name = %s')
        params.append(site)

    if since:
        conditions.append('COALESCE(published_at, collected_at) >= %s')
        params.append(since)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ''

    with pg() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT site_name, article_id, url, title, published_at, collected_at
                FROM articles
                {where}
                ORDER BY COALESCE(published_at, collected_at) DESC
                LIMIT %s OFFSET %s
                """,
                (*params, PAGE_SIZE, offset),
            )
            articles = filter_articles([serialize_article(dict(r)) for r in cur.fetchall()])

            cur.execute(
                f'SELECT COUNT(*) FROM articles {where}',
                params,
            )
            total = cur.fetchone()['count']

    return articles, total


def get_cached_recent_page(site: str, page: int) -> dict | None:
    if is_retired_site(site):
        return {
            'articles': [],
            'page': page,
            'site': site,
            'total': 0,
            'total_pages': 0,
            'recent_only': True,
        }

    client = cache()
    if client is None:
        return None

    try:
        version = client.get(cache_active_version_key())
        if not version:
            return None

        page_key = cache_key(version, 'index', site_cache_slug(site), 'page', str(page))
        meta_key = cache_key(version, 'index', site_cache_slug(site), 'meta')
        cached = client.get(page_key)
        if cached:
            payload = json.loads(cached)
            if cached_page_has_retired_articles(payload):
                return None
            return payload

        meta_raw = client.get(meta_key)
        if not meta_raw:
            return None

        meta = json.loads(meta_raw)
        total_pages = meta.get('total_pages', 0)
        total = meta.get('total', 0)

        if page > total_pages and not (page == 1 and total == 0):
            return None

        if page == 1 and total == 0:
            return {
                'articles': [],
                'page': 1,
                'site': site,
                'total': 0,
                'total_pages': 0,
                'recent_only': True,
            }

        return None
    except RedisError as exc:
        logger.warning('Redis recent page read failed: %s', exc)
        return None


@app.route('/')
def index():
    page = max(1, int(request.args.get('page', 1)))
    site = request.args.get('site', '').strip()
    site_names = get_site_names()
    cached = get_cached_recent_page(site, page)

    if cached:
        articles = cached['articles']
        total_pages = cached['total_pages']
    else:
        articles, total = fetch_articles_page(site=site, page=page)
        total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE

    return render_template(
        'index.html',
        articles=articles,
        page=page,
        total_pages=total_pages,
        query='',
        site=site,
        sort_order='published_desc',
        site_names=site_names,
        site_display_names=SITE_DISPLAY_NAMES,
    )


@app.route('/search')
def search():
    query = request.args.get('q', '').strip()
    site = request.args.get('site', '').strip()
    page = max(1, int(request.args.get('page', 1)))
    sort_order = request.args.get('sort', 'published_desc').strip() or 'published_desc'
    site_names = get_site_names()

    if not query:
        return index()

    client = es()
    must = [{'multi_match': {'query': query, 'fields': ['title']}}]
    if site:
        must.append({'term': {'site_name': site}})
    must_not = []
    if RETIRED_SITE_NAMES:
        must_not.append({'terms': {'site_name': list(RETIRED_SITE_NAMES)}})

    sort = [
        {'published_at': {'order': 'desc', 'missing': '_last'}},
        {'collected_at': {'order': 'desc', 'missing': '_last'}},
        {'_score': 'desc'},
    ]

    if sort_order == 'relevance':
        sort = [
            {'_score': 'desc'},
            {'published_at': {'order': 'desc', 'missing': '_last'}},
            {'collected_at': {'order': 'desc', 'missing': '_last'}},
        ]

    body = {
        'query': {'bool': {'must': must, 'must_not': must_not}},
        'sort': sort,
        'from': (page - 1) * PAGE_SIZE,
        'size': PAGE_SIZE,
    }

    try:
        resp = client.search(index=ES_INDEX, body=body)
        hits = resp['hits']['hits']
        total = resp['hits']['total']['value']
        articles = filter_articles([h['_source'] for h in hits])
    except Exception as exc:
        logger.error('ES search error: %s', exc)
        articles = []
        total = 0

    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    return render_template(
        'index.html',
        articles=articles,
        page=page,
        total_pages=total_pages,
        query=query,
        site=site,
        sort_order=sort_order,
        site_names=site_names,
        site_display_names=SITE_DISPLAY_NAMES,
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
