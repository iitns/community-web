const THEME_STORAGE_KEY = 'community-web-theme';
const FILTER_STORAGE_KEY = 'community-web-filters';
const PULL_REFRESH_MAX = 96;
const PULL_REFRESH_THRESHOLD = 72;
const DEFAULT_SORT_ORDER = 'published_desc';

let currentState = null;
let currentPayload = null;

function getApiBaseUrl() {
  const configured = window.COMMUNITY_WEB_CONFIG && window.COMMUNITY_WEB_CONFIG.apiBaseUrl;
  return (configured || '').replace(/\/$/, '');
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function parseBool(value, defaultValue) {
  if (value === null || value === undefined || value === '') {
    return defaultValue;
  }

  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

function parsePage(value) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return 1;
  }

  return parsed;
}

function getPreferredTheme() {
  const currentTheme = document.documentElement.getAttribute('data-theme');
  if (currentTheme === 'light' || currentTheme === 'dark') {
    return currentTheme;
  }

  return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function applyTheme(theme) {
  const nextTheme = theme === 'dark' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', nextTheme);
  document.documentElement.setAttribute('data-bs-theme', nextTheme);

  const themeToggleBtn = document.getElementById('themeToggleBtn');
  if (themeToggleBtn) {
    const nextLabel = nextTheme === 'dark' ? '라이트모드 켜기' : '다크모드 켜기';
    themeToggleBtn.setAttribute('aria-label', nextLabel);
    themeToggleBtn.setAttribute('title', nextLabel);
  }
}

function toggleTheme() {
  const nextTheme = getPreferredTheme() === 'dark' ? 'light' : 'dark';
  try {
    localStorage.setItem(THEME_STORAGE_KEY, nextTheme);
  } catch (error) {
  }
  applyTheme(nextTheme);
}

function toggleSearch() {
  const form = document.getElementById('searchForm');
  const navCenter = document.getElementById('navCenter');
  const toggleBtn = document.getElementById('searchToggleBtn');

  closeFilterPanel();
  form.classList.add('show');
  navCenter.style.display = 'none';
  toggleBtn.style.display = 'none';
  document.getElementById('searchInput').focus();
}

function toggleFilterPanel() {
  const panel = document.getElementById('filterPanel');
  const toggleBtn = document.getElementById('filterToggleBtn');

  if (!panel || !toggleBtn) {
    return;
  }

  const nextState = !panel.classList.contains('show');
  panel.classList.toggle('show', nextState);
  toggleBtn.setAttribute('aria-expanded', nextState ? 'true' : 'false');
}

function closeFilterPanel() {
  const panel = document.getElementById('filterPanel');
  const toggleBtn = document.getElementById('filterToggleBtn');

  if (!panel || !toggleBtn) {
    return;
  }

  panel.classList.remove('show');
  toggleBtn.setAttribute('aria-expanded', 'false');
}

function updateClearButton() {
  const input = document.getElementById('searchInput');
  const clearBtn = document.getElementById('clearSearchBtn');

  if (!input || !clearBtn) {
    return;
  }

  clearBtn.classList.toggle('show', input.value.length > 0);
}

function clearSearch() {
  const input = document.getElementById('searchInput');
  if (!input) {
    return;
  }

  input.value = '';
  updateClearButton();
  input.focus();
}

function loadFilterPreferences() {
  try {
    const raw = localStorage.getItem(FILTER_STORAGE_KEY);
    if (!raw) {
      return { sites: [], include_nsfw: false };
    }

    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') {
      return { sites: [], include_nsfw: false };
    }

    return {
      sites: Array.isArray(parsed.sites) ? parsed.sites.map(String) : [],
      include_nsfw: Boolean(parsed.include_nsfw),
    };
  } catch (error) {
    return { sites: [], include_nsfw: false };
  }
}

function saveFilterPreferences(selectedSites, includeNsfw) {
  try {
    localStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify({
      sites: selectedSites,
      include_nsfw: includeNsfw,
    }));
  } catch (error) {
  }
}

function captureEvent(name, properties) {
  if (!window.posthog || typeof window.posthog.capture !== 'function') {
    return;
  }

  window.posthog.capture(name, properties || {});
}

function readLocationState() {
  const params = new URLSearchParams(window.location.search);
  const query = (params.get('q') || '').trim();
  const mode = window.location.pathname === '/search' && query ? 'search' : 'index';

  return {
    mode: mode,
    query: query,
    page: parsePage(params.get('page')),
    sortOrder: params.get('sort') === 'relevance' ? 'relevance' : DEFAULT_SORT_ORDER,
    selectedSites: params.getAll('site').map(function(site) {
      return site.trim();
    }).filter(Boolean),
    includeNsfw: parseBool(params.get('include_nsfw'), false),
    filtersApplied: params.get('filters_applied') === '1',
    hasExplicitFilters: params.has('site') || params.has('include_nsfw') || params.get('filters_applied') === '1',
  };
}

function applySavedFilters(state) {
  if (state.hasExplicitFilters) {
    return state;
  }

  const saved = loadFilterPreferences();
  return {
    mode: state.mode,
    query: state.query,
    page: state.page,
    sortOrder: state.sortOrder,
    selectedSites: saved.sites,
    includeNsfw: saved.include_nsfw,
    filtersApplied: false,
    hasExplicitFilters: false,
  };
}

function buildFrontendHref(state) {
  const params = new URLSearchParams();
  const pathname = state.mode === 'search' && state.query ? '/search' : '/';

  if (state.mode === 'search' && state.query) {
    params.set('q', state.query);
    if (state.sortOrder && state.sortOrder !== DEFAULT_SORT_ORDER) {
      params.set('sort', state.sortOrder);
    }
  }

  if (state.page > 1 || state.filtersApplied || pathname === '/search') {
    params.set('page', String(state.page));
  }

  state.selectedSites.forEach(function(site) {
    params.append('site', site);
  });

  if (state.includeNsfw) {
    params.set('include_nsfw', '1');
  } else if (state.filtersApplied) {
    params.set('include_nsfw', '0');
  }

  if (state.filtersApplied) {
    params.set('filters_applied', '1');
  }

  const query = params.toString();
  return query ? pathname + '?' + query : pathname;
}

function buildApiUrl(state) {
  const endpoint = state.mode === 'search' && state.query ? '/api/v1/search' : '/api/v1/articles';
  const configuredBase = getApiBaseUrl();
  const url = configuredBase
    ? new URL(configuredBase + endpoint, window.location.origin)
    : new URL(endpoint, window.location.origin);

  url.searchParams.set('page', String(state.page));
  if (state.mode === 'search' && state.query) {
    url.searchParams.set('q', state.query);
    url.searchParams.set('sort', state.sortOrder || DEFAULT_SORT_ORDER);
  }

  state.selectedSites.forEach(function(site) {
    url.searchParams.append('site', site);
  });

  if (state.includeNsfw) {
    url.searchParams.set('include_nsfw', '1');
  }

  return url.toString();
}

async function fetchPayload(state) {
  const response = await fetch(buildApiUrl(state), {
    headers: {
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error('API request failed with status ' + response.status);
  }

  return response.json();
}

function getSelectedSiteLabel(selectedSites, siteDisplayNames) {
  if (!selectedSites.length) {
    return '전체 커뮤니티';
  }

  if (selectedSites.length === 1) {
    return siteDisplayNames[selectedSites[0]] || selectedSites[0];
  }

  return selectedSites.length + '개 커뮤니티';
}

function buildNavCenterLabel(state, payload) {
  const selectedSites = payload.filters.selected_sites;
  const includeNsfw = payload.filters.include_nsfw;
  const selectedSiteLabel = getSelectedSiteLabel(selectedSites, payload.site_display_names);

  if (state.query && selectedSites.length && includeNsfw) {
    return '검색: "' + state.query + '" · ' + selectedSiteLabel + ' · 19금 포함';
  }
  if (state.query && selectedSites.length) {
    return '검색: "' + state.query + '" · ' + selectedSiteLabel;
  }
  if (state.query && includeNsfw) {
    return '검색: "' + state.query + '" · 19금 포함';
  }
  if (state.query) {
    return '검색: "' + state.query + '"';
  }
  if (selectedSites.length && includeNsfw) {
    return '필터: ' + selectedSiteLabel + ' · 19금 포함';
  }
  if (selectedSites.length) {
    return '필터: ' + selectedSiteLabel;
  }
  if (includeNsfw) {
    return '필터: 19금 포함';
  }

  return '';
}

function renderSearchState(state) {
  const form = document.getElementById('searchForm');
  const navCenter = document.getElementById('navCenter');
  const toggleBtn = document.getElementById('searchToggleBtn');
  const input = document.getElementById('searchInput');
  const sortSelect = document.getElementById('searchSortSelect');

  input.value = state.query || '';
  sortSelect.value = state.sortOrder || DEFAULT_SORT_ORDER;
  updateClearButton();

  if (state.query) {
    form.classList.add('show');
    navCenter.style.display = 'none';
    toggleBtn.style.display = 'none';
  } else {
    form.classList.remove('show');
    navCenter.style.display = '';
    toggleBtn.style.display = '';
  }
}

function renderNavCenter(state, payload) {
  const label = buildNavCenterLabel(state, payload);
  document.getElementById('navCenterLabel').textContent = label;
}

function renderFilterSites(payload) {
  const filterSiteList = document.getElementById('filterSiteList');
  const selectedSiteSet = new Set(payload.filters.selected_sites);

  filterSiteList.innerHTML = payload.site_names.map(function(siteName, index) {
    const checked = selectedSiteSet.has(siteName) ? ' checked' : '';
    const label = payload.site_display_names[siteName] || siteName;
    const inputId = 'siteFilter' + (index + 1);

    return '' +
      '<label class="filter-site-option" for="' + inputId + '">' +
      '<input class="form-check-input mt-0" type="checkbox" name="site" id="' + inputId + '" value="' + escapeHtml(siteName) + '"' + checked + '>' +
      '<span>' + escapeHtml(label) + '</span>' +
      '</label>';
  }).join('');

  document.getElementById('includeNsfwCheckbox').checked = payload.filters.include_nsfw;
}

function renderArticles(payload) {
  const articleList = document.getElementById('articleList');
  const articles = payload.articles;

  if (!articles.length) {
    articleList.innerHTML = '<p class="text-center text-muted py-4">결과가 없습니다.</p>';
    return;
  }

  articleList.innerHTML = articles.map(function(article) {
    const siteName = article.site_name || '';
    const articleId = article.article_id || '';
    const label = payload.site_display_names[siteName] || siteName;
    const rawDateTime = article.published_at || article.collected_at || '';
    const hasTime = article.published_at ? 'true' : 'false';
    const dateAttrs = rawDateTime
      ? ' data-datetime="' + escapeHtml(rawDateTime) + '" data-has-time="' + hasTime + '"'
      : '';

    return '' +
      '<div class="article-row">' +
      '<span class="site-badge">' + escapeHtml(label) + '</span>' +
      '<a class="article-title" href="' + escapeHtml(article.url || '#') + '"' +
      ' data-article-id="' + escapeHtml(siteName + '-' + articleId) + '"' +
      ' data-site-name="' + escapeHtml(siteName) + '">' + escapeHtml(article.title || '') + '</a>' +
      '<span class="article-date"' + dateAttrs + '></span>' +
      '</div>';
  }).join('');

  bindArticleTracking();
  localizeArticleDates();
}

function buildPageRange(page, totalPages) {
  const start = Math.max(1, page - 4);
  const end = Math.min(totalPages, page + 4);
  const pages = [];

  for (let value = start; value <= end; value += 1) {
    pages.push(value);
  }

  return pages;
}

function renderPagination(payload) {
  const paginationArea = document.getElementById('paginationArea');
  const paginationList = document.getElementById('paginationList');
  const page = payload.pagination.page;
  const totalPages = payload.pagination.total_pages;

  if (totalPages < 1) {
    paginationArea.hidden = true;
    paginationList.innerHTML = '';
    return;
  }

  const items = [];
  if (page > 1) {
    items.push(buildPaginationItem('이전', page - 1, false));
  }

  buildPageRange(page, totalPages).forEach(function(value) {
    items.push(buildPaginationItem(String(value), value, value === page));
  });

  if (page < totalPages) {
    items.push(buildPaginationItem('다음', page + 1, false));
  }

  paginationList.innerHTML = items.join('');
  paginationArea.hidden = false;
}

function buildPaginationItem(label, page, active) {
  const nextState = {
    mode: currentState.mode,
    query: currentState.query,
    page: page,
    sortOrder: currentState.sortOrder,
    selectedSites: currentState.selectedSites,
    includeNsfw: currentState.includeNsfw,
    filtersApplied: currentState.filtersApplied || currentState.selectedSites.length > 0 || currentState.includeNsfw,
  };
  const href = buildFrontendHref(nextState);

  return '' +
    '<li class="page-item' + (active ? ' active' : '') + '">' +
    '<a class="page-link" href="' + escapeHtml(href) + '">' + escapeHtml(label) + '</a>' +
    '</li>';
}

function bindArticleTracking() {
  document.querySelectorAll('.article-title[data-article-id]').forEach(function(link) {
    link.addEventListener('click', function() {
      captureEvent('article_click', {
        article_id: link.dataset.articleId,
        site_name: link.dataset.siteName,
        title: link.textContent.trim(),
      });
    });
  });
}

function localizeArticleDates() {
  const clientTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;

  document.querySelectorAll('.article-date[data-datetime]').forEach(function(el) {
    const raw = el.dataset.datetime;
    const hasTime = el.dataset.hasTime === 'true';

    if (!raw) {
      return;
    }

    const normalized = /(?:Z|[+-]\d{2}:\d{2})$/.test(raw) ? raw : raw + 'Z';
    const date = new Date(normalized);

    if (Number.isNaN(date.getTime())) {
      return;
    }

    if (hasTime) {
      const datePart = new Intl.DateTimeFormat(undefined, {
        month: '2-digit',
        day: '2-digit',
        timeZone: clientTimeZone,
      }).format(date);
      const timePart = new Intl.DateTimeFormat(undefined, {
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
        timeZone: clientTimeZone,
      }).format(date);
      el.innerHTML = '<span class="date-line">' + datePart + '</span><span class="time-line">' + timePart + '</span>';
      el.title = new Intl.DateTimeFormat(undefined, {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        timeZone: clientTimeZone,
        timeZoneName: 'short',
      }).format(date);
    } else {
      const datePart = new Intl.DateTimeFormat(undefined, {
        month: '2-digit',
        day: '2-digit',
        timeZone: clientTimeZone,
      }).format(date);
      el.innerHTML = '<span class="date-line">' + datePart + '</span>';
    }
  });
}

function updateResetLink() {
  const resetState = {
    mode: currentState.mode,
    query: currentState.query,
    page: 1,
    sortOrder: currentState.sortOrder,
    selectedSites: [],
    includeNsfw: false,
    filtersApplied: true,
  };
  document.getElementById('resetFiltersLink').setAttribute('href', buildFrontendHref(resetState));
}

function syncStateFromPayload(payload) {
  currentState = {
    mode: payload.query ? 'search' : 'index',
    query: payload.query || '',
    page: payload.pagination.page,
    sortOrder: payload.sort_order || DEFAULT_SORT_ORDER,
    selectedSites: payload.filters.selected_sites || [],
    includeNsfw: Boolean(payload.filters.include_nsfw),
    filtersApplied: currentState.filtersApplied || currentState.selectedSites.length > 0 || currentState.includeNsfw,
  };
}

function renderPage(payload) {
  currentPayload = payload;
  syncStateFromPayload(payload);
  renderNavCenter(currentState, payload);
  renderSearchState(currentState);
  renderFilterSites(payload);
  renderArticles(payload);
  renderPagination(payload);
  updateResetLink();
}

function renderError(error) {
  const articleList = document.getElementById('articleList');
  const paginationArea = document.getElementById('paginationArea');

  paginationArea.hidden = true;
  articleList.innerHTML = '<p class="text-center text-muted py-4">데이터를 불러오지 못했습니다. 잠시 후 다시 시도해주세요.</p>';
  console.error(error);
}

async function loadPage() {
  const state = applySavedFilters(readLocationState());
  currentState = state;
  renderSearchState(state);
  renderNavCenter(state, {
    filters: {
      selected_sites: state.selectedSites,
      include_nsfw: state.includeNsfw,
    },
    query: state.query,
    site_display_names: currentPayload ? currentPayload.site_display_names : {},
  });

  try {
    const payload = await fetchPayload(state);
    renderPage(payload);
  } catch (error) {
    renderError(error);
  }
}

function navigateToState(state) {
  window.location.assign(buildFrontendHref(state));
}

function setupPullToRefresh() {
  if (!window.matchMedia || !window.matchMedia('(pointer: coarse)').matches) {
    return;
  }

  const indicator = document.getElementById('pullRefreshIndicator');
  if (!indicator) {
    return;
  }

  let startY = 0;
  let currentOffset = 0;
  let dragging = false;
  let readyToRefresh = false;
  let refreshing = false;

  function resetIndicator() {
    currentOffset = 0;
    dragging = false;
    readyToRefresh = false;
    indicator.classList.remove('visible', 'ready');
    indicator.style.transform = 'translate(-50%, -100%)';
    indicator.textContent = '아래로 당겨 새로고침';
    document.body.classList.remove('pull-refresh-active');
  }

  function shouldIgnoreGesture(target) {
    if (!target) {
      return false;
    }

    return Boolean(target.closest('input, textarea, select, button, a, .filter-panel'));
  }

  window.addEventListener('touchstart', function(event) {
    if (refreshing || window.scrollY > 0 || shouldIgnoreGesture(event.target)) {
      dragging = false;
      return;
    }

    startY = event.touches[0].clientY;
    dragging = true;
    readyToRefresh = false;
  }, { passive: true });

  window.addEventListener('touchmove', function(event) {
    if (!dragging || refreshing) {
      return;
    }

    const deltaY = event.touches[0].clientY - startY;
    if (deltaY <= 0 || window.scrollY > 0) {
      resetIndicator();
      return;
    }

    currentOffset = Math.min(PULL_REFRESH_MAX, deltaY * 0.45);
    readyToRefresh = currentOffset >= PULL_REFRESH_THRESHOLD;

    indicator.classList.add('visible');
    indicator.classList.toggle('ready', readyToRefresh);
    indicator.style.transform = 'translate(-50%, ' + (currentOffset - 100) + '%)';
    indicator.textContent = readyToRefresh ? '놓으면 새로고침' : '아래로 당겨 새로고침';
    document.body.classList.add('pull-refresh-active');
    event.preventDefault();
  }, { passive: false });

  window.addEventListener('touchend', function() {
    if (!dragging || refreshing) {
      return;
    }

    dragging = false;

    if (readyToRefresh) {
      refreshing = true;
      indicator.classList.add('visible');
      indicator.classList.remove('ready');
      indicator.style.transform = 'translate(-50%, 0)';
      indicator.textContent = '새로고침 중...';
      window.location.reload();
      return;
    }

    resetIndicator();
  }, { passive: true });

  window.addEventListener('touchcancel', resetIndicator, { passive: true });
}

function bindStaticEvents() {
  document.getElementById('themeToggleBtn').addEventListener('click', toggleTheme);
  document.getElementById('filterToggleBtn').addEventListener('click', toggleFilterPanel);
  document.getElementById('searchToggleBtn').addEventListener('click', toggleSearch);
  document.getElementById('clearSearchBtn').addEventListener('click', clearSearch);

  const searchInput = document.getElementById('searchInput');
  searchInput.addEventListener('input', updateClearButton);

  document.getElementById('searchForm').addEventListener('submit', function(event) {
    event.preventDefault();

    const query = document.getElementById('searchInput').value.trim();
    const nextState = {
      mode: query ? 'search' : 'index',
      query: query,
      page: 1,
      sortOrder: document.getElementById('searchSortSelect').value === 'relevance' ? 'relevance' : DEFAULT_SORT_ORDER,
      selectedSites: currentState.selectedSites,
      includeNsfw: currentState.includeNsfw,
      filtersApplied: true,
    };

    saveFilterPreferences(nextState.selectedSites, nextState.includeNsfw);
    if (query) {
      captureEvent('search', {
        query: query,
        sites: nextState.selectedSites,
        include_nsfw: nextState.includeNsfw,
      });
    }

    navigateToState(nextState);
  });

  document.getElementById('filterForm').addEventListener('submit', function(event) {
    event.preventDefault();

    const selectedSites = Array.from(document.querySelectorAll('#filterSiteList input[name="site"]:checked')).map(function(input) {
      return input.value;
    });
    const includeNsfw = document.getElementById('includeNsfwCheckbox').checked;
    const nextState = {
      mode: currentState.mode,
      query: currentState.query,
      page: 1,
      sortOrder: currentState.sortOrder,
      selectedSites: selectedSites,
      includeNsfw: includeNsfw,
      filtersApplied: true,
    };

    saveFilterPreferences(selectedSites, includeNsfw);
    selectedSites.forEach(function(site) {
      captureEvent('filter-' + site);
    });
    if (includeNsfw) {
      captureEvent('filter-nsfw');
    }

    navigateToState(nextState);
  });

  document.getElementById('resetFiltersLink').addEventListener('click', function(event) {
    event.preventDefault();

    const nextState = {
      mode: currentState.mode,
      query: currentState.query,
      page: 1,
      sortOrder: currentState.sortOrder,
      selectedSites: [],
      includeNsfw: false,
      filtersApplied: true,
    };

    saveFilterPreferences([], false);
    navigateToState(nextState);
  });

  document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape') {
      closeFilterPanel();
    }

    if (event.key === '/' && document.activeElement.tagName !== 'INPUT') {
      event.preventDefault();
      toggleSearch();
    }
  });

  document.addEventListener('click', function(event) {
    const panel = document.getElementById('filterPanel');
    const toggleBtn = document.getElementById('filterToggleBtn');

    if (!panel || !toggleBtn || !panel.classList.contains('show')) {
      return;
    }

    if (panel.contains(event.target) || toggleBtn.contains(event.target)) {
      return;
    }

    closeFilterPanel();
  });
}

document.addEventListener('DOMContentLoaded', function() {
  applyTheme(getPreferredTheme());
  setupPullToRefresh();
  bindStaticEvents();
  loadPage();
});
