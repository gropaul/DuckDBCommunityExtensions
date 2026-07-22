# DuckDB Community Extensions — daily stats

Scrapes the [DuckDB community extensions](https://duckdb.org/community_extensions/)
ecosystem once a day and accumulates a time series into a single DuckDB file,
`community_extensions.duckdb`, which is committed back to this repo by a GitHub
Action. Clone the repo and open the file to get the full history immediately.

```bash
duckdb community_extensions.duckdb
```

## Query it without cloning

DuckDB can attach the database directly over HTTPS from the raw GitHub content
URL — no clone, no download step. A remote database must be attached read-only
(`httpfs` autoloads):

```sql
ATTACH 'https://raw.githubusercontent.com/gropaul/DuckDBCommunityExtensions/master/community_extensions.duckdb'
  AS ce (READ_ONLY);

USE ce;
SELECT extension, stars FROM v_latest_github ORDER BY stars DESC NULLS LAST LIMIT 10;
```

## Data sources

1. **Download stats** — the official weekly cumulative counts behind
   `community-extensions.duckdb.org/download-stats-weekly`, pivoted per your query.
2. **Catalog metadata** — every `extensions/*/description.yml` in
   `duckdb/community-extensions` (name, description, version, language, license,
   maintainers, and the source GitHub repo).
3. **GitHub repo stats** — snapshotted daily from each source repo (stars, forks,
   watchers, issues, releases, activity), so time series such as *stars over time*
   build up one row per day.

## Tables

| Table | What | Update strategy |
|-------|------|-----------------|
| `downloads` | `(extension, day, downloads)` cumulative counts | Recomputed in full each run (idempotent / self-healing) |
| `catalog` | Latest `description.yml` metadata per extension | Overwritten each run; `first_seen` preserved |
| `catalog_history` | Changelog of metadata (version bumps, description edits) | Append-on-change, deduped by content hash |
| `github_snapshots` | Daily GitHub stats per source repo | Append-only, one row per extension per day |

## Convenience views

- `v_latest_github` — newest GitHub snapshot per extension, joined to the catalog
- `v_stars_over_time` — the accruing daily stars/forks/watchers/issues series
- `v_download_growth` — week-over-week download deltas
- `v_activity` — repo staleness (`days_since_push`), latest release
- `v_catalog_history` — version changes with the previous value alongside
- `v_ecosystem_daily` — one ecosystem-wide totals row per snapshot day

## Running locally

```bash
pip install -r requirements.txt
export GITHUB_TOKEN=$(gh auth token)   # optional but recommended (60→5000 req/h)
python scrape.py                       # writes community_extensions.duckdb
```

Flags: `--db PATH`, `--no-github` (downloads + catalog only), `--workers N`.

## Automation

`.github/workflows/scrape.yml` runs `scrape.py` daily at 06:00 UTC (and on manual
dispatch), then commits the refreshed database back to `main`.
