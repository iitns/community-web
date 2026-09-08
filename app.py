import os
import logging
import json
import threading
from datetime import datetime, timezone
from urllib.parse import urlencode

import psycopg2
import psycopg2.extras
from elasticsearch import Elasticsearch
from flask import Flask, make_response, render_template, request
from redis import Redis
from redis.exceptions import RedisError

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    _kafka_available = True
except ImportError:
    _kafka_available = False

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
    'host':     os.environ.get('PG_HOST', 'postgres-service.storage.svc.cluster.local'),
    'port':     int(os.environ.get('PG_PORT', 5432)),
    'dbname':   os.environ.get('PG_DB', 'community'),
    'user':     os.environ.get('PG_USER', 'community'),
    'password': os.environ.get('PG_PASSWORD', 'community_pass'),
}
ES_HOST = os.environ.get('ES_HOST', 'http://elasticsearch-service.storage.svc.cluster.local:9200')
ES_INDEX = 'community_articles'
REDIS_HOST = os.environ.get('REDIS_HOST', '')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
CACHE_PREFIX = 'community-web:recent'
FILTER_COOKIE_NAME = 'community-web-filters'
FILTER_COOKIE_MAX_AGE = 60 * 60 * 24 * 365
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '')
KAFKA_UA_TOPIC = 'community-web.user-agents'

_redis_client = None
_kafka_producer = None
_kafka_producer_lock = threading.Lock()


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


def kafka_producer():
    global _kafka_producer

    if not _kafka_available or not KAFKA_BOOTSTRAP_SERVERS:
        return None

    with _kafka_producer_lock:
        if _kafka_producer is None:
            try:
                _kafka_producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    request_timeout_ms=3000,
                    retries=0,
                )
            except KafkaError as exc:
                logger.warning('Kafka producer init failed: %s', exc)
                return None

    return _kafka_producer


def emit_user_agent(user_agent: str) -> None:
    producer = kafka_producer()
    if not producer or not user_agent:
        return

    payload = {
        'user_agent': user_agent,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'source': 'community-web',
    }

    def _send():
        try:
            producer.send(KAFKA_UA_TOPIC, value=payload)
        except Exception as exc:
            logger.debug('Kafka send failed: %s', exc)

    threading.Thread(target=_send, daemon=True).start()


def site_cache_slug(sites: list[str]) -> str:
    return '__'.join(sites) if sites else 'all'


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


def parse_bool(value: str | bool | None, default: bool = False) -> bool:
    if value is None:
        return default

    if isinstance(value, bool):
        return value

    return value.strip().lower() in {'1', 'true', 'yes', 'on'}


def parse_page_arg(default: int = 1) -> int:
    raw = request.args.get('page', default)

    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return default


def load_filter_preferences() -> dict:
    raw = request.cookies.get(FILTER_COOKIE_NAME, '')
    if not raw:
        return {}

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}

    if not isinstance(data, dict):
        return {}

    return data


def normalize_selected_sites(site_names: list[str], selected_sites: list[str]) -> list[str]:
    seen = set()
    normalized = []

    for site in selected_sites:
        site = site.strip()
        if not site or site not in site_names or site in seen:
            continue
        seen.add(site)
        normalized.append(site)

    return normalized


def resolve_filters(site_names: list[str]) -> tuple[list[str], bool]:
    saved = load_filter_preferences()
    filters_applied = request.args.get('filters_applied') == '1'
    selected_sites = request.args.getlist('site')
    if not selected_sites:
        single_site = request.args.get('site')
        if single_site is not None:
            selected_sites = [single_site]

    if not selected_sites and not filters_applied:
        saved_sites = saved.get('sites', [])
        if isinstance(saved_sites, list):
            selected_sites = [str(site) for site in saved_sites]
        else:
            legacy_site = str(saved.get('site', '')).strip()
            selected_sites = [legacy_site] if legacy_site else []

    selected_sites = normalize_selected_sites(site_names, selected_sites)

    include_nsfw_arg = request.args.get('include_nsfw')
    if include_nsfw_arg is None:
        include_nsfw = parse_bool(saved.get('include_nsfw'), default=False)
    else:
        include_nsfw = parse_bool(include_nsfw_arg, default=False)

    return selected_sites, include_nsfw


def render_with_filter_cookie(template_name: str, *, selected_sites: list[str], include_nsfw: bool, **context):
    filter_query = urlencode({'site': selected_sites, 'include_nsfw': '1' if include_nsfw else '0'}, doseq=True)
    response = make_response(
        render_template(
            template_name,
            selected_sites=selected_sites,
            include_nsfw=include_nsfw,
            filter_query=filter_query,
            **context,
        )
    )
    response.set_cookie(
        FILTER_COOKIE_NAME,
        json.dumps({'sites': selected_sites, 'include_nsfw': include_nsfw}, separators=(',', ':')),
        max_age=FILTER_COOKIE_MAX_AGE,
        httponly=False,
        samesite='Lax',
    )
    return response


def cached_page_has_retired_articles(payload: dict) -> bool:
    for site in payload.get('sites', []):
        if is_retired_site(site):
            return True

    legacy_site = payload.get('site', '')
    if is_retired_site(legacy_site):
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


def fetch_articles_page(selected_sites: list[str], page: int, since: datetime | None = None, include_nsfw: bool = False) -> tuple[list[dict], int]:
    if any(is_retired_site(site) for site in selected_sites):
        return [], 0

    offset = (page - 1) * PAGE_SIZE
    conditions = []
    params = []

    if RETIRED_SITE_NAMES:
        conditions.append('NOT (site_name = ANY(%s))')
        params.append(list(RETIRED_SITE_NAMES))

    if selected_sites:
        conditions.append('site_name = ANY(%s)')
        params.append(selected_sites)

    if since:
        conditions.append('COALESCE(published_at, collected_at) >= %s')
        params.append(since)

    if not include_nsfw:
        conditions.append('NOT COALESCE(is_nsfw, FALSE)')

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


def get_cached_recent_page(selected_sites: list[str], page: int, include_nsfw: bool = False) -> dict | None:
    if any(is_retired_site(site) for site in selected_sites):
        return {
            'articles': [],
            'page': page,
            'sites': selected_sites,
            'total': 0,
            'total_pages': 0,
            'recent_only': True,
        }

    if len(selected_sites) > 1:
        return None

    if not include_nsfw:
        return None

    client = cache()
    if client is None:
        return None

    try:
        version = client.get(cache_active_version_key())
        if not version:
            return None

        page_key = cache_key(version, 'index', site_cache_slug(selected_sites), 'page', str(page))
        meta_key = cache_key(version, 'index', site_cache_slug(selected_sites), 'meta')
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
                'sites': selected_sites,
                'total': 0,
                'total_pages': 0,
                'recent_only': True,
            }

        return None
    except RedisError as exc:
        logger.warning('Redis recent page read failed: %s', exc)
        return None


@app.after_request
def collect_user_agent(response):
    ua = request.headers.get('User-Agent', '')
    if ua:
        emit_user_agent(ua)
    return response


@app.route('/')
def index():
    page = parse_page_arg()
    site_names = get_site_names()
    selected_sites, include_nsfw = resolve_filters(site_names)
    cached = get_cached_recent_page(selected_sites, page, include_nsfw=include_nsfw)

    if cached:
        articles = cached['articles']
        total_pages = cached['total_pages']
    else:
        articles, total = fetch_articles_page(selected_sites=selected_sites, page=page, include_nsfw=include_nsfw)
        total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE

    return render_with_filter_cookie(
        'index.html',
        articles=articles,
        page=page,
        total_pages=total_pages,
        query='',
        current_endpoint='index',
        sort_order='published_desc',
        site_names=site_names,
        site_display_names=SITE_DISPLAY_NAMES,
        selected_sites=selected_sites,
        include_nsfw=include_nsfw,
    )


@app.route('/search')
def search():
    query = request.args.get('q', '').strip()
    page = parse_page_arg()
    sort_order = request.args.get('sort', 'published_desc').strip() or 'published_desc'
    site_names = get_site_names()
    selected_sites, include_nsfw = resolve_filters(site_names)

    if not query:
        return index()

    client = es()
    must = [{'multi_match': {'query': query, 'fields': ['title']}}]
    filters = []
    if selected_sites:
        filters.append({'terms': {'site_name': selected_sites}})
    must_not = []
    if RETIRED_SITE_NAMES:
        must_not.append({'terms': {'site_name': list(RETIRED_SITE_NAMES)}})
    if not include_nsfw:
        must_not.append({'term': {'is_nsfw': True}})

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
        'query': {'bool': {'must': must, 'filter': filters, 'must_not': must_not}},
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
    return render_with_filter_cookie(
        'index.html',
        articles=articles,
        page=page,
        total_pages=total_pages,
        query=query,
        current_endpoint='search',
        sort_order=sort_order,
        site_names=site_names,
        site_display_names=SITE_DISPLAY_NAMES,
        selected_sites=selected_sites,
        include_nsfw=include_nsfw,
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
