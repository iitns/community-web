import { Client } from 'pg';
import { PAGE_SIZE, RETIRED_SITE_NAMES, filterArticles, orderSiteNames } from './constants';
import type { Article, Env, Filters } from './types';

async function withClient<T>(env: Env, run: (client: Client) => Promise<T>): Promise<T> {
  const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
  await client.connect();
  try {
    return await run(client);
  } finally {
    await client.end();
  }
}

export async function fetchSiteNames(env: Env): Promise<string[]> {
  return withClient(env, async (client) => {
    const result = await client.query<{ site_name: string }>('SELECT DISTINCT site_name FROM articles');
    const names = result.rows
      .map((row) => row.site_name)
      .filter((name): name is string => Boolean(name) && !RETIRED_SITE_NAMES.has(name));
    return orderSiteNames(names);
  });
}

export async function fetchArticlesPage(
  env: Env,
  { selectedSites, includeNsfw }: Filters,
  page: number,
): Promise<{ articles: Article[]; total: number }> {
  if (selectedSites.some((site) => RETIRED_SITE_NAMES.has(site))) {
    return { articles: [], total: 0 };
  }

  const conditions: string[] = [];
  const params: unknown[] = [];

  if (RETIRED_SITE_NAMES.size > 0) {
    params.push([...RETIRED_SITE_NAMES]);
    conditions.push(`NOT (site_name = ANY($${params.length}))`);
  }

  if (selectedSites.length > 0) {
    params.push(selectedSites);
    conditions.push(`site_name = ANY($${params.length})`);
  }

  if (!includeNsfw) {
    conditions.push('NOT COALESCE(is_nsfw, FALSE)');
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  const offset = (page - 1) * PAGE_SIZE;

  return withClient(env, async (client) => {
    const articlesResult = await client.query<Article>(
      `
      SELECT site_name, article_id, url, title, published_at, collected_at
      FROM articles
      ${where}
      ORDER BY COALESCE(published_at, collected_at) DESC
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}
      `,
      [...params, PAGE_SIZE, offset],
    );

    const countResult = await client.query<{ count: string }>(
      `SELECT COUNT(*) FROM articles ${where}`,
      params,
    );

    return {
      articles: filterArticles(articlesResult.rows),
      total: Number(countResult.rows[0]?.count ?? 0),
    };
  });
}
