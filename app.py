import os
import logging
import json
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from elasticsearch import Elasticsearch
from flask import Flask, jsonify, render_template, request
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
    '루리웹(유머)',
    '인벤',
    '보배드림',
    '펨코',
]

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
RECENT_CACHE_DAYS = int(os.environ.get('RECENT_CACHE_DAYS', 7))
CACHE_PREFIX = 'community-web:recent:v1'
CACHE_REFRESH_TOKEN = os.environ.get('CACHE_REFRESH_TOKEN', '')

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


def recent_cutoff() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=RECENT_CACHE_DAYS)


def site_cache_slug(site: str) -> str:
    return site or 'all'


def cache_key(*parts: str) -> str:
    return ':'.join([CACHE_PREFIX, *parts])


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
            site_names = {row[0] for row in cur.fetchall()}

    ordered = [name for name in SITE_ORDER if name in site_names]
    extras = sorted(site_names - set(ordered))
    return ordered + extras


def get_site_names(force_refresh: bool = False) -> list[str]:
    client = cache()
    key = cache_key('site-names')

    if client and not force_refresh:
        try:
            cached = client.get(key)
            if cached:
                return json.loads(cached)
        except RedisError as exc:
            logger.warning('Redis site_names read failed: %s', exc)

    site_names = fetch_site_names_db()

    if client:
        try:
            client.set(key, json.dumps(site_names, ensure_ascii=False))
        except RedisError as exc:
            logger.warning('Redis site_names write failed: %s', exc)

    return site_names


def fetch_articles_page(site: str, page: int, since: datetime | None = None) -> tuple[list[dict], int]:
    offset = (page - 1) * PAGE_SIZE
    conditions = []
    params = []

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
            articles = [serialize_article(dict(r)) for r in cur.fetchall()]

            cur.execute(
                f'SELECT COUNT(*) FROM articles {where}',
                params,
            )
            total = cur.fetchone()['count']

    return articles, total


def page_payload(site: str, page: int, recent_only: bool) -> dict:
    articles, total = fetch_articles_page(
        site=site,
        page=page,
        since=recent_cutoff() if recent_only else None,
    )
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    return {
        'articles': articles,
        'page': page,
        'site': site,
        'total': total,
        'total_pages': total_pages,
        'recent_only': recent_only,
    }


def clear_site_page_cache(client: Redis, site: str) -> None:
    pattern = cache_key('index', site_cache_slug(site), 'page', '*')
    keys = list(client.scan_iter(match=pattern))
    if keys:
        client.delete(*keys)


def warm_site_recent_cache(client: Redis, site: str) -> dict:
    clear_site_page_cache(client, site)

    first_page = page_payload(site, 1, recent_only=True)
    total_pages = max(1, first_page['total_pages'])

    client.set(
        cache_key('index', site_cache_slug(site), 'meta'),
        json.dumps(
            {
                'total': first_page['total'],
                'total_pages': first_page['total_pages'],
                'refreshed_at': datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
        ),
    )
    client.set(
        cache_key('index', site_cache_slug(site), 'page', '1'),
        json.dumps(first_page, ensure_ascii=False),
    )

    for page in range(2, total_pages + 1):
        client.set(
            cache_key('index', site_cache_slug(site), 'page', str(page)),
            json.dumps(page_payload(site, page, recent_only=True), ensure_ascii=False),
        )

    return {
        'site': site,
        'total': first_page['total'],
        'total_pages': first_page['total_pages'],
    }


def refresh_recent_cache() -> dict:
    client = cache()
    if client is None:
        return {'status': 'disabled', 'reason': 'redis_not_configured'}

    site_names = get_site_names(force_refresh=True)
    refreshed = []

    try:
        refreshed.append(warm_site_recent_cache(client, ''))
        for site in site_names:
            refreshed.append(warm_site_recent_cache(client, site))
    except RedisError as exc:
        logger.error('Redis refresh failed: %s', exc)
        raise

    return {
        'status': 'ok',
        'recent_cache_days': RECENT_CACHE_DAYS,
        'site_names': site_names,
        'refreshed': refreshed,
    }


def get_cached_recent_page(site: str, page: int) -> dict | None:
    client = cache()
    if client is None:
        return None

    page_key = cache_key('index', site_cache_slug(site), 'page', str(page))
    meta_key = cache_key('index', site_cache_slug(site), 'meta')

    try:
        cached = client.get(page_key)
        if cached:
            return json.loads(cached)

        meta_raw = client.get(meta_key)
        if not meta_raw:
            return None

        meta = json.loads(meta_raw)
        total_pages = meta.get('total_pages', 0)
        total = meta.get('total', 0)

        if page > total_pages and not (page == 1 and total == 0):
            return None

        payload = page_payload(site, page, recent_only=True)
        client.set(page_key, json.dumps(payload, ensure_ascii=False))
        return payload
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
        site_names=site_names,
    )


@app.route('/search')
def search():
    query = request.args.get('q', '').strip()
    site = request.args.get('site', '').strip()
    page = max(1, int(request.args.get('page', 1)))
    site_names = get_site_names()

    if not query:
        return index()

    client = es()
    must = [{'multi_match': {'query': query, 'fields': ['title']}}]
    if site:
        must.append({'term': {'site_name': site}})

    body = {
        'query': {'bool': {'must': must}},
        'sort': [{'_score': 'desc'}, {'collected_at': 'desc'}],
        'from': (page - 1) * PAGE_SIZE,
        'size': PAGE_SIZE,
    }

    try:
        resp = client.search(index=ES_INDEX, body=body)
        hits = resp['hits']['hits']
        total = resp['hits']['total']['value']
        articles = [h['_source'] for h in hits]
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
        site_names=site_names,
    )


@app.post('/internal/cache/refresh')
def refresh_cache_endpoint():
    if CACHE_REFRESH_TOKEN:
        auth_header = request.headers.get('X-Cache-Refresh-Token', '')
        if auth_header != CACHE_REFRESH_TOKEN:
            return jsonify({'status': 'error', 'reason': 'unauthorized'}), 401

    try:
        return jsonify(refresh_recent_cache())
    except RedisError as exc:
        logger.error('Cache refresh endpoint failed: %s', exc)
        return jsonify({'status': 'error', 'reason': 'redis_failure'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
