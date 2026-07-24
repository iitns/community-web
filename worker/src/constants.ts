export const PAGE_SIZE = 30;

export const SITE_ORDER = [
  'SLR클럽',
  '뽐뿌',
  '웃대',
  '루리웹(유게)',
  '인벤',
  '보배드림',
  '펨코',
];

export const SITE_DISPLAY_NAMES: Record<string, string> = {
  '루리웹(유게)': '루리(육)',
};

export const RETIRED_SITE_NAMES = new Set(['루리웹(유머)']);

export function isRetiredSite(site: string): boolean {
  return RETIRED_SITE_NAMES.has(site);
}

export function filterArticles<T extends { site_name?: string }>(articles: T[]): T[] {
  return articles.filter((article) => !isRetiredSite(article.site_name ?? ''));
}

export function orderSiteNames(siteNames: Iterable<string>): string[] {
  const set = new Set(siteNames);
  const ordered = SITE_ORDER.filter((name) => set.has(name));
  const extras = [...set].filter((name) => !SITE_ORDER.includes(name)).sort();
  return [...ordered, ...extras];
}
