import { PAGE_SIZE, RETIRED_SITE_NAMES, filterArticles } from './constants';
import type { Article, Env, Filters } from './types';

export async function searchArticles(
  env: Env,
  query: string,
  { selectedSites, includeNsfw }: Filters,
  page: number,
  sortOrder: string,
): Promise<{ articles: Article[]; total: number }> {
  const must: unknown[] = [{ multi_match: { query, fields: ['title'] } }];
  const filter: unknown[] = [];
  const mustNot: unknown[] = [];

  if (selectedSites.length > 0) {
    filter.push({ terms: { site_name: selectedSites } });
  }
  if (RETIRED_SITE_NAMES.size > 0) {
    mustNot.push({ terms: { site_name: [...RETIRED_SITE_NAMES] } });
  }
  if (!includeNsfw) {
    mustNot.push({ term: { is_nsfw: true } });
  }

  const sort =
    sortOrder === 'relevance'
      ? [
          { _score: 'desc' },
          { published_at: { order: 'desc', missing: '_last' } },
          { collected_at: { order: 'desc', missing: '_last' } },
        ]
      : [
          { published_at: { order: 'desc', missing: '_last' } },
          { collected_at: { order: 'desc', missing: '_last' } },
          { _score: 'desc' },
        ];

  const body = {
    query: { bool: { must, filter, must_not: mustNot } },
    sort,
    from: (page - 1) * PAGE_SIZE,
    size: PAGE_SIZE,
  };

  try {
    const response = await env.ES_SERVICE.fetch(`${env.ES_URL.replace(/\/$/, '')}/${env.ES_INDEX}/_search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      throw new Error(`ES search failed with status ${response.status}`);
    }

    const payload = await response.json<{
      hits: { hits: { _source: Article }[]; total: { value: number } };
    }>();

    return {
      articles: filterArticles(payload.hits.hits.map((hit) => hit._source)),
      total: payload.hits.total.value,
    };
  } catch (error) {
    console.error('ES search error', error);
    return { articles: [], total: 0 };
  }
}
