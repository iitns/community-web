# Frontend Deployment

Cloudflare Pages settings:

- Production branch: `master`
- Root directory: `frontend`
- Build command: none
- Build output directory: `.`

Runtime endpoints:

- Frontend: `https://beobe.amolant.net`, `https://beobe.janghanpark.com`
- API: separate Cloudflare Worker (`../worker`), called cross-origin from the browser

Frontend and API are fully decoupled: this Pages project only serves static
assets. There is no Pages Function proxy. `frontend/config.js` sets
`window.COMMUNITY_WEB_CONFIG.apiBaseUrl` to the Worker's URL (workers.dev
subdomain or a custom domain), and `frontend/app.js` calls that origin directly
with `fetch()`.

The static frontend expects the API to expose:

- `GET /api/v1/articles`
- `GET /api/v1/search`
- `GET /api/v1/sites`
- `GET /healthz`

and to return `Access-Control-Allow-Origin` for this Pages origin (see
`../worker/wrangler.jsonc` -> `CORS_ALLOWED_ORIGINS`).

See `../worker/CLAUDE.md` for the API deployment.
