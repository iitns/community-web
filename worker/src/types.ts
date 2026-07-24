export interface Env {
  HYPERDRIVE: Hyperdrive;
  ES_SERVICE: Fetcher;
  ES_URL: string;
  ES_INDEX: string;
  CORS_ALLOWED_ORIGINS: string;
}

export interface Article {
  site_name: string;
  article_id: string;
  url: string;
  title: string;
  published_at: string | null;
  collected_at: string | null;
}

export interface Filters {
  selectedSites: string[];
  includeNsfw: boolean;
}
