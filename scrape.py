#!/usr/bin/env python3
"""
Scrape the DuckDB Community Extensions ecosystem into a DuckDB database.

Designed to run once a day (cron / GitHub Action). Every run:

  * Recomputes the full weekly download matrix (self-healing, idempotent).
  * Re-reads every extension's `description.yml` catalog metadata.
  * Takes a fresh GitHub snapshot of every source repo (stars, forks,
    issues, watchers, releases, activity) and *appends* it, so a time
    series of stars-over-time (etc.) builds up one row per day.

Everything lands in a single DuckDB file (default: community_extensions.duckdb).

Tables
  catalog            one row per extension  — latest description.yml metadata
  downloads          (extension, day, downloads) long-form cumulative counts
  github_snapshots   (snapshot_date, extension, ...) append-only daily GitHub stats

Convenience views
  v_latest_github, v_stars_over_time, v_download_growth,
  v_activity, v_ecosystem_daily

Environment
  GITHUB_TOKEN   strongly recommended — raises the API limit from 60/h to 5000/h.

Usage
  python scrape.py [--db community_extensions.duckdb] [--no-github] [--workers 8]
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import os
import sys
import time
from typing import Any, Optional

import duckdb
import requests
import yaml

REPO = "duckdb/community-extensions"
RAW_BASE = f"https://raw.githubusercontent.com/{REPO}/main"
API = "https://api.github.com"

# The download-stats query you provided, verbatim. Produces a wide matrix
# (one row per extension, one column per weekly snapshot day).
DOWNLOAD_PIVOT = """
PIVOT (
    UNPIVOT (
        FROM read_json([
            printf(
                'https://community-extensions.duckdb.org/download-stats-weekly/%s/%s.json',
                week.strftime('%Y'),
                week.strftime('%V').regexp_replace('^0', '')
            )
            FOR week
            IN range(TIMESTAMP '2026-01-04', now()::TIMESTAMP, INTERVAL 1 WEEK)
            IF strftime(week, '%V') != '53'
        ], union_by_name := true)
    )
    ON COLUMNS(* EXCLUDE _last_update)
    INTO NAME extension VALUE downloads
)
ON date_trunc('day', _last_update::TIMESTAMP)
USING any_value(downloads)
ORDER BY extension
"""


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "duckdb-community-extensions-scraper"
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
        s.headers["Accept"] = "application/vnd.github+json"
    return s


def gh_get(session: requests.Session, url: str, *, tolerate_404: bool = False,
           retries: int = 4) -> Optional[requests.Response]:
    """GET with rate-limit awareness and exponential backoff."""
    for attempt in range(retries):
        r = session.get(url, timeout=30)
        # Primary/secondary rate limit handling.
        if r.status_code == 403 and r.headers.get("X-RateLimit-Remaining") == "0":
            reset = int(r.headers.get("X-RateLimit-Reset", "0"))
            wait = max(reset - int(time.time()), 1)
            if wait > 900:  # don't block for absurd amounts of time
                print(f"  ! rate limited, reset in {wait}s — giving up on {url}",
                      file=sys.stderr)
                return None
            print(f"  … rate limited, sleeping {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if r.status_code == 404 and tolerate_404:
            return None
        if r.status_code >= 500 or r.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        if r.ok:
            return r
        print(f"  ! {r.status_code} for {url}", file=sys.stderr)
        return None
    return None


# --------------------------------------------------------------------------- #
# Catalog: discover extensions and parse description.yml
# --------------------------------------------------------------------------- #
def list_extension_files(session: requests.Session) -> list[str]:
    """Return every extensions/<name>/description.yml path via one tree call."""
    url = f"{API}/repos/{REPO}/git/trees/main?recursive=1"
    r = gh_get(session, url)
    if r is None:
        raise RuntimeError("could not list repository tree")
    tree = r.json().get("tree", [])
    return sorted(
        node["path"]
        for node in tree
        if node["type"] == "blob"
        and node["path"].startswith("extensions/")
        and node["path"].endswith("/description.yml")
    )


def parse_description(session: requests.Session, path: str) -> Optional[dict[str, Any]]:
    r = gh_get(session, f"{RAW_BASE}/{path}", tolerate_404=True)
    if r is None:
        return None
    try:
        data = yaml.safe_load(r.text) or {}
    except yaml.YAMLError as e:
        print(f"  ! bad yaml in {path}: {e}", file=sys.stderr)
        return None
    if not isinstance(data, dict):
        return None
    ext = data.get("extension") if isinstance(data.get("extension"), dict) else {}
    repo = data.get("repo") if isinstance(data.get("repo"), dict) else {}
    docs = data.get("docs") if isinstance(data.get("docs"), dict) else {}
    name = ext.get("name") or path.split("/")[1]
    maintainers = ext.get("maintainers") or []
    if isinstance(maintainers, str):
        maintainers = [maintainers]
    return {
        "extension": name,
        "description": ext.get("description"),
        "version": str(ext.get("version")) if ext.get("version") is not None else None,
        "language": ext.get("language"),
        "build": ext.get("build"),
        "license": ext.get("license"),
        "maintainers": [str(m) for m in maintainers],
        "excluded_platforms": ext.get("excluded_platforms"),
        "requires_toolchains": ext.get("requires_toolchains"),
        "github_repo": repo.get("github"),
        "ref": repo.get("ref"),
        "hello_world": docs.get("hello_world"),
        "extended_description": docs.get("extended_description"),
    }


# --------------------------------------------------------------------------- #
# GitHub snapshot per source repo
# --------------------------------------------------------------------------- #
def github_snapshot(session: requests.Session, github_repo: str) -> dict[str, Any]:
    """Fetch a rich point-in-time snapshot of a source repo."""
    snap: dict[str, Any] = {"github_repo": github_repo}
    r = gh_get(session, f"{API}/repos/{github_repo}", tolerate_404=True)
    if r is None:
        snap["fetch_ok"] = False
        return snap
    d = r.json()
    lic = d.get("license") or {}
    snap.update(
        fetch_ok=True,
        stars=d.get("stargazers_count"),
        forks=d.get("forks_count"),
        watchers=d.get("subscribers_count"),  # true "watching" count
        open_issues=d.get("open_issues_count"),  # issues + PRs
        size_kb=d.get("size"),
        network_count=d.get("network_count"),
        primary_language=d.get("language"),
        repo_license=lic.get("spdx_id"),
        topics=d.get("topics") or [],
        is_archived=d.get("archived"),
        is_fork=d.get("fork"),
        created_at=d.get("created_at"),
        pushed_at=d.get("pushed_at"),
        updated_at=d.get("updated_at"),
        default_branch=d.get("default_branch"),
        homepage=d.get("homepage"),
    )
    # Latest release (tolerate repos with no releases).
    rel = gh_get(session, f"{API}/repos/{github_repo}/releases/latest",
                 tolerate_404=True)
    if rel is not None:
        rd = rel.json()
        snap["latest_release_tag"] = rd.get("tag_name")
        snap["latest_release_at"] = rd.get("published_at")
    return snap


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #
def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS catalog (
        extension            VARCHAR PRIMARY KEY,
        description          VARCHAR,
        version              VARCHAR,
        language             VARCHAR,
        build                VARCHAR,
        license              VARCHAR,
        maintainers          VARCHAR[],
        excluded_platforms   VARCHAR,
        requires_toolchains  VARCHAR,
        github_repo          VARCHAR,
        ref                  VARCHAR,
        hello_world          VARCHAR,
        extended_description VARCHAR,
        first_seen           DATE,
        last_seen            DATE
    );

    -- Append-only changelog: a new row only when an extension's metadata
    -- actually changes (version bump, description edit, repo move, …).
    CREATE TABLE IF NOT EXISTS catalog_history (
        changed_date  DATE,
        extension     VARCHAR,
        version       VARCHAR,
        description   VARCHAR,
        language      VARCHAR,
        license       VARCHAR,
        maintainers   VARCHAR[],
        github_repo   VARCHAR,
        ref           VARCHAR,
        content_hash  VARCHAR,
        PRIMARY KEY (extension, content_hash)
    );

    CREATE TABLE IF NOT EXISTS downloads (
        extension  VARCHAR,
        day        DATE,
        downloads  BIGINT,
        PRIMARY KEY (extension, day)
    );

    CREATE TABLE IF NOT EXISTS github_snapshots (
        snapshot_date      DATE,
        extension          VARCHAR,
        github_repo        VARCHAR,
        fetch_ok           BOOLEAN,
        stars              BIGINT,
        forks              BIGINT,
        watchers           BIGINT,
        open_issues        BIGINT,
        size_kb            BIGINT,
        network_count      BIGINT,
        primary_language   VARCHAR,
        repo_license       VARCHAR,
        topics             VARCHAR[],
        is_archived        BOOLEAN,
        is_fork            BOOLEAN,
        created_at         TIMESTAMP,
        pushed_at          TIMESTAMP,
        updated_at         TIMESTAMP,
        default_branch     VARCHAR,
        homepage           VARCHAR,
        latest_release_tag VARCHAR,
        latest_release_at  TIMESTAMP,
        PRIMARY KEY (snapshot_date, extension)
    );
    """)


def store_downloads(con: duckdb.DuckDBPyConnection) -> int:
    """Run the PIVOT, then unpivot back to long form and upsert."""
    # Dynamic PIVOT can't back a view, so materialize it into a temp table.
    con.execute("DROP TABLE IF EXISTS _dl_wide")
    con.execute(f"CREATE TEMP TABLE _dl_wide AS {DOWNLOAD_PIVOT}")
    con.execute("""
        INSERT OR REPLACE INTO downloads
        SELECT extension,
               strptime(day, '%Y-%m-%d %H:%M:%S')::DATE AS day,
               downloads
        FROM (
            UNPIVOT _dl_wide
            ON COLUMNS(* EXCLUDE extension)
            INTO NAME day VALUE downloads
        )
        WHERE downloads IS NOT NULL
    """)
    con.execute("DROP TABLE _dl_wide")
    return con.execute("SELECT count(*) FROM downloads").fetchone()[0]


def store_catalog(con: duckdb.DuckDBPyConnection, rows: list[dict], today: dt.date) -> None:
    con.execute("""
        CREATE TEMP TABLE _cat AS SELECT * FROM catalog LIMIT 0;
    """)
    con.executemany(
        """INSERT INTO _cat
           (extension, description, version, language, build, license, maintainers,
            excluded_platforms, requires_toolchains, github_repo, ref,
            hello_world, extended_description, first_seen, last_seen)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (
                r["extension"], r["description"], r["version"], r["language"],
                r["build"], r["license"], r["maintainers"],
                str(r["excluded_platforms"]) if r["excluded_platforms"] is not None else None,
                str(r["requires_toolchains"]) if r["requires_toolchains"] is not None else None,
                r["github_repo"], r["ref"], r["hello_world"], r["extended_description"],
                today, today,
            )
            for r in rows
        ],
    )
    # Upsert current-latest: preserve the earliest first_seen we've recorded.
    con.execute("""
        INSERT OR REPLACE INTO catalog
        SELECT n.* REPLACE (COALESCE(o.first_seen, n.first_seen) AS first_seen)
        FROM _cat n LEFT JOIN catalog o USING (extension)
    """)

    # Append a history row only when the metadata content hash is new for
    # that extension (PRIMARY KEY (extension, content_hash) dedupes repeats).
    con.execute("""
        INSERT OR IGNORE INTO catalog_history
        SELECT ?, extension, version, description, language, license, maintainers,
               github_repo, ref,
               md5(concat_ws('|',
                   coalesce(version, ''), coalesce(description, ''),
                   coalesce(language, ''), coalesce(license, ''),
                   coalesce(maintainers::VARCHAR, ''),
                   coalesce(github_repo, ''), coalesce(ref, ''))) AS content_hash
        FROM _cat
    """, [today])
    con.execute("DROP TABLE _cat")


def store_github(con: duckdb.DuckDBPyConnection, rows: list[dict], today: dt.date) -> None:
    if not rows:
        return
    con.execute("CREATE TEMP TABLE _gh AS SELECT * FROM github_snapshots LIMIT 0")
    con.executemany(
        """INSERT INTO _gh VALUES
           (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (
                today, r["extension"], r.get("github_repo"), r.get("fetch_ok"),
                r.get("stars"), r.get("forks"), r.get("watchers"), r.get("open_issues"),
                r.get("size_kb"), r.get("network_count"), r.get("primary_language"),
                r.get("repo_license"), r.get("topics"), r.get("is_archived"),
                r.get("is_fork"), r.get("created_at"), r.get("pushed_at"),
                r.get("updated_at"), r.get("default_branch"), r.get("homepage"),
                r.get("latest_release_tag"), r.get("latest_release_at"),
            )
            for r in rows
        ],
    )
    con.execute("INSERT OR REPLACE INTO github_snapshots SELECT * FROM _gh")
    con.execute("DROP TABLE _gh")


def create_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
    -- Most recent GitHub snapshot per extension, joined to catalog.
    CREATE OR REPLACE VIEW v_latest_github AS
    SELECT c.extension, c.description, c.language, c.license, c.maintainers,
           g.github_repo, g.stars, g.forks, g.watchers, g.open_issues,
           g.latest_release_tag, g.latest_release_at, g.pushed_at, g.created_at,
           g.is_archived, g.snapshot_date
    FROM catalog c
    LEFT JOIN github_snapshots g
      ON g.extension = c.extension
     AND g.snapshot_date = (SELECT max(snapshot_date) FROM github_snapshots
                            WHERE extension = c.extension);

    -- Stars (and other metrics) over time — the accruing daily time series.
    CREATE OR REPLACE VIEW v_stars_over_time AS
    SELECT snapshot_date, extension, github_repo, stars, forks, watchers, open_issues
    FROM github_snapshots
    ORDER BY extension, snapshot_date;

    -- Week-over-week download deltas from the cumulative counts.
    CREATE OR REPLACE VIEW v_download_growth AS
    SELECT extension, day, downloads,
           downloads - lag(downloads) OVER wd AS new_downloads,
           day - lag(day) OVER wd AS days_since_prev
    FROM downloads
    WINDOW wd AS (PARTITION BY extension ORDER BY day)
    ORDER BY extension, day;

    -- Repo activity / staleness from the latest snapshot.
    CREATE OR REPLACE VIEW v_activity AS
    SELECT extension, github_repo, stars,
           pushed_at, latest_release_tag, latest_release_at,
           date_diff('day', pushed_at, now()) AS days_since_push
    FROM v_latest_github
    ORDER BY days_since_push;

    -- Metadata changelog: successive recorded versions per extension, with
    -- the previous value alongside so version bumps are easy to spot.
    CREATE OR REPLACE VIEW v_catalog_history AS
    SELECT extension, changed_date, version, license, github_repo, ref,
           lag(version)     OVER wh AS prev_version,
           lag(changed_date) OVER wh AS prev_changed_date
    FROM catalog_history
    WINDOW wh AS (PARTITION BY extension ORDER BY changed_date)
    ORDER BY extension, changed_date;

    -- One ecosystem-wide row per snapshot day: totals to watch trend lines.
    CREATE OR REPLACE VIEW v_ecosystem_daily AS
    SELECT snapshot_date,
           count(*)                    AS n_extensions,
           sum(stars)                  AS total_stars,
           sum(forks)                  AS total_forks,
           sum(open_issues)            AS total_open_issues,
           count(*) FILTER (is_archived) AS n_archived
    FROM github_snapshots
    GROUP BY snapshot_date
    ORDER BY snapshot_date;
    """)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="community_extensions.duckdb",
                    help="output DuckDB file (default: community_extensions.duckdb)")
    ap.add_argument("--no-github", action="store_true",
                    help="skip GitHub repo snapshots (downloads + catalog only)")
    ap.add_argument("--workers", type=int, default=8,
                    help="concurrent GitHub requests (default: 8)")
    args = ap.parse_args()

    today = dt.datetime.now().date()
    session = make_session()
    authed = "Authorization" in session.headers
    print(f"→ scraping into {args.db}  (github={'on' if not args.no_github else 'off'}, "
          f"auth={'yes' if authed else 'no — 60 req/h limit'})")

    con = duckdb.connect(args.db)
    init_schema(con)

    # 1) Download matrix — recomputed in full every run (idempotent, self-healing).
    print("• download stats …", end=" ", flush=True)
    n_dl = store_downloads(con)
    print(f"{n_dl} (extension, day) rows")

    # 2) Catalog metadata from every description.yml.
    print("• catalog (description.yml) …", end=" ", flush=True)
    paths = list_extension_files(session)
    catalog_rows: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for row in pool.map(lambda p: parse_description(session, p), paths):
            if row:
                catalog_rows.append(row)
    store_catalog(con, catalog_rows, today)
    print(f"{len(catalog_rows)} extensions")

    # 3) GitHub snapshot per source repo (append-only → time series).
    if not args.no_github:
        repos = [(r["extension"], r["github_repo"]) for r in catalog_rows
                 if r.get("github_repo")]
        print(f"• github snapshots for {len(repos)} repos …", end=" ", flush=True)
        gh_rows: list[dict] = []

        def snap(item):
            ext, repo = item
            s = github_snapshot(session, repo)
            s["extension"] = ext
            return s

        with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
            for s in pool.map(snap, repos):
                gh_rows.append(s)
        store_github(con, gh_rows, today)
        ok = sum(1 for r in gh_rows if r.get("fetch_ok"))
        total_stars = sum(r.get("stars") or 0 for r in gh_rows)
        print(f"{ok}/{len(gh_rows)} ok, {total_stars:,} total stars")

    create_views(con)
    con.close()
    print(f"✓ done — open with:  duckdb {args.db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
