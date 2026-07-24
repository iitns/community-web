## community-web

Target architecture is fully Cloudflare-native and decoupled:

- **Frontend** (`frontend/`): static site on Cloudflare Pages
- **API** (`worker/`): Cloudflare Worker reading Postgres (via Hyperdrive) and
  Elasticsearch (via `fetch()`) directly — no app server in between
- **Data** (Postgres, Elasticsearch): stay on homelab k3s, reachable from
  Cloudflare only through a Cloudflare Tunnel (`cloudflared` running on homelab)

Both the Pages project and the Worker project are connected to this same
GitHub repo, each with a different "Root directory" (`frontend` and `worker`
respectively), so a push to `master` builds and deploys both independently.
See `worker/CLAUDE.md` for the full setup (Hyperdrive, tunnel, secrets,
dashboard steps) and `frontend/README.md` for the Pages side.

### Repository layout

- `frontend/`: static frontend for Cloudflare Pages (no Pages Function proxy —
  calls the Worker cross-origin)
- `worker/`: Cloudflare Worker serving the JSON API
- `app.py`, `templates/`, `Dockerfile`, `requirements.txt`: legacy Flask app,
  currently still what's actually deployed on Oracle VM
  (`beobe.janghanpark.com` / `beobe.amolant.net` via Traefik + cloudflared on
  Oracle). Kept until the Worker + Pages setup above is verified end-to-end,
  then this and the Oracle-side k8s manifests can be retired.

### API routes (same shape on both the legacy Flask app and the new Worker)

- `GET /api/v1/articles`
- `GET /api/v1/search`
- `GET /api/v1/sites`
- `GET /healthz`

Example:

```text
GET /api/v1/articles?page=1&site=보배드림&include_nsfw=1
GET /api/v1/search?q=정치&page=1&sort=relevance&site=펨코
```
