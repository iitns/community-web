import os
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
from elasticsearch import Elasticsearch
from flask import Flask, render_template, request

app = Flask(__name__)
logger = logging.getLogger(__name__)

PAGE_SIZE = 30

PG_CONN = {
    'host':     os.environ.get('PG_HOST', 'postgresql-service'),
    'port':     int(os.environ.get('PG_PORT', 5432)),
    'dbname':   os.environ.get('PG_DB', 'community'),
    'user':     os.environ.get('PG_USER', 'community'),
    'password': os.environ.get('PG_PASSWORD', 'community_pass'),
}
ES_HOST = os.environ.get('ES_HOST', 'http://elasticsearch-service:9200')
ES_INDEX = 'community_articles'


def pg():
    return psycopg2.connect(**PG_CONN)


def es():
    return Elasticsearch(ES_HOST)


@app.route('/')
def index():
    page = max(1, int(request.args.get('page', 1)))
    offset = (page - 1) * PAGE_SIZE

    with pg() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT site_name, article_id, url, title, published_at, collected_at
                FROM articles
                ORDER BY COALESCE(published_at, collected_at) DESC
                LIMIT %s OFFSET %s
                """,
                (PAGE_SIZE, offset),
            )
            articles = [dict(r) for r in cur.fetchall()]

            cur.execute('SELECT COUNT(*) FROM articles')
            total = cur.fetchone()['count']

    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    return render_template(
        'index.html',
        articles=articles,
        page=page,
        total_pages=total_pages,
        query='',
    )


@app.route('/search')
def search():
    query = request.args.get('q', '').strip()
    site = request.args.get('site', '').strip()
    page = max(1, int(request.args.get('page', 1)))

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
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
