import { PAGE_SIZE, SITE_DISPLAY_NAMES } from './constants';
import { fetchArticlesPage, fetchSiteNames } from './db';
import { searchArticles } from './search';
import { corsHeaders, jsonResponse, preflightResponse } from './cors';
import type { Article, Env, Filters } from './types';

function parsePage(url: URL): number {
  const raw = url.searchParams.get('page');
  const parsed = Number.parseInt(raw ?? '1', 10);
  return Number.isFinite(parsed) && parsed >= 1 ? parsed : 1;
}

function parseBool(value: string | null, defaultValue: boolean): boolean {
  if (value === null) return defaultValue;
  return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
}

function parseFilters(url: URL, siteNames: string[]): Filters {
  const seen = new Set<string>();
  const selectedSites = url.searchParams
    .getAll('site')
    .map((site) => site.trim())
    .filter((site) => site && siteNames.includes(site) && !seen.has(site) && seen.add(site));

  return {
    selectedSites,
    includeNsfw: parseBool(url.searchParams.get('include_nsfw'), false),
  };
}

function buildPayload(
  articles: Article[],
  total: number,
  page: number,
  query: string,
  sortOrder: string,
  siteNames: string[],
  filters: Filters,
) {
  return {
    articles,
    pagination: {
      page,
      page_size: PAGE_SIZE,
      total,
      total_pages: Math.ceil(total / PAGE_SIZE),
    },
    query,
    sort_order: sortOrder,
    filters: {
      selected_sites: filters.selectedSites,
      include_nsfw: filters.includeNsfw,
    },
    site_names: siteNames,
    site_display_names: SITE_DISPLAY_NAMES,
  };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return preflightResponse(request, env);
    }

    if (request.method !== 'GET') {
      return jsonResponse(request, env, { error: 'method not allowed' }, 405);
    }

    if (url.pathname === '/healthz') {
      return jsonResponse(request, env, { status: 'ok' });
    }

    if (url.pathname === '/api/v1/sites') {
      const siteNames = await fetchSiteNames(env);
      return jsonResponse(request, env, {
        site_names: siteNames,
        site_display_names: SITE_DISPLAY_NAMES,
      });
    }

    if (url.pathname === '/api/v1/articles') {
      const siteNames = await fetchSiteNames(env);
      const filters = parseFilters(url, siteNames);
      const page = parsePage(url);
      const { articles, total } = await fetchArticlesPage(env, filters, page);
      return jsonResponse(
        request,
        env,
        buildPayload(articles, total, page, '', 'published_desc', siteNames, filters),
      );
    }

    if (url.pathname === '/api/v1/search') {
      const siteNames = await fetchSiteNames(env);
      const filters = parseFilters(url, siteNames);
      const page = parsePage(url);
      const query = (url.searchParams.get('q') ?? '').trim();
      const sortOrder = url.searchParams.get('sort') === 'relevance' ? 'relevance' : 'published_desc';

      if (!query) {
        const { articles, total } = await fetchArticlesPage(env, filters, page);
        return jsonResponse(
          request,
          env,
          buildPayload(articles, total, page, query, sortOrder, siteNames, filters),
        );
      }

      const { articles, total } = await searchArticles(env, query, filters, page, sortOrder);
      return jsonResponse(
        request,
        env,
        buildPayload(articles, total, page, query, sortOrder, siteNames, filters),
      );
    }

    return jsonResponse(request, env, { error: 'not found' }, 404);
  },
} satisfies ExportedHandler<Env>;
