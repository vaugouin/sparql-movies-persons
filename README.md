# sparql-movies-persons

A Wikidata SPARQL crawler that incrementally harvests movies, TV series (including their seasons and episodes), fictional characters, and persons (notably film/TV professionals) from the public Wikidata endpoint and persists them into a MariaDB / MySQL database for use by the Citizen Phil data platform.

The crawler is the "alternative" SPARQL-based pipeline that complements the main Wikidata bulk crawler: instead of scanning Wikidata dumps, it queries `https://query.wikidata.org/sparql` year by year (and item by item, for known IDs already present in the staging table) and upserts the results into versioned `T_WC_WIKIDATA_*_V1` tables.

---

## Features

- **Sixteen processing scopes** orchestrated in a single run, driven by the `arrwikidatascope` map in [sparql-movies-persons.py](sparql-movies-persons.py):
  - `101` — Movies, year by year (current year + 5 → 1875)
  - `102` — Persons / humans, year by year (current year → 1000)
  - `103` — Items → persons: resolves staged `T_WC_WIKIDATA_ITEM_V1` rows whose `INSTANCE_OF` matches a configured person class, batched 500 ids per SPARQL call
  - `104` — Items → movies: same pattern for movie instance-of classes, batched 500 ids per SPARQL call
  - `105` — Series, year by year (current year + 4 → 1925)
  - `106` — Items → series: same pattern for series instance-of classes, batched 500 ids per SPARQL call
  - `107` — Items → characters: same pattern for fictional-character instance-of classes, batched 500 ids per SPARQL call
  - `108` — Characters, year by year using publication date P577 *or* date of first appearance P4584 (UNION) — current year + 5 → 1800. P4584 was added because many fictional characters carry only P4584 and would otherwise be skipped by the year crawl entirely.
  - `115` — Work (movie + serie) → characters: iterates every row of `T_WC_WIKIDATA_SERIE_V1` and `T_WC_WIKIDATA_MOVIE_V1` and asks Wikidata for each work's listed characters via UNION of `?work wdt:P674 ?character` (work-listed character) and `?work p:P161/pq:P453 ?character` (cast statement with character-role qualifier), batched 25 work ids per SPARQL call. Catches characters that have neither P577 nor P4584 (so scope `108` is structurally blind to them) but are referenced by a known film or series. P31 is OPTIONAL on the discovered character — if an entity is the object of P674 or P453 it is treated as a character regardless of its declared instance-of.
  - `116` — Person → characters: iterates every row of `T_WC_WIKIDATA_PERSON_V1` and asks Wikidata for every (work, character) pair via `?work p:P161/ps:P161 ?person; pq:P453 ?character`, batched 15 person ids per SPARQL call. Catches secondary / one-off roles that no work explicitly lists in P674. Most batches return zero rows because `T_WC_WIKIDATA_PERSON_V1` contains far more than actors; the cost is still bounded by the per-batch SPARQL call. This is the slowest scope — disable it from `arrwikidatascope` if you don't need it.
  - `109` — Items → seasons: same pattern for season instance-of classes, batched 500 ids per SPARQL call (wired right after `106` so seasons are resolved against series that the same run just promoted)
  - `110` — Items → episodes: same pattern for episode instance-of classes, batched 500 ids per SPARQL call (wired right after `109` so episodes can resolve their parent season)
  - `111` — Seasons, year by year using season start P580 (current year + 4 → 1925), wired right after `105`
  - `112` — Episodes, year by year using publication date P577 (current year + 4 → 1925), wired right after `111`
  - `113` — Serie → seasons: iterates every row of `T_WC_WIKIDATA_SERIE_V1` and asks Wikidata for its P179 backlinks (seasons), batched 100 series ids per SPARQL call. Catches seasons missing P580 that scope `111` skips entirely.
  - `114` — Serie → episodes: same source as `113`, but the query is a UNION of `episode→P4908→season→P179→series` (modern multi-season shows) and `episode→P179→series` directly (single-season or older data), batched 25 series ids per SPARQL call (lower batch size because of higher per-series fan-out and Wikidata's 30 s query timeout). Catches episodes missing P577 that scope `112` skips, and single-season shows whose episodes never had a P4908 season link. Wired right after `113` so the season rows it might create are already in place before episodes are resolved against them.
- **Configurable instance-of classes** for persons (default `Q5`), movies (default `Q11424 Q202866 Q226730 Q24862 Q20650540 Q506240 Q17517379`), series (default `Q5398426 Q1259759 Q117467246 Q63952888 Q15416`), seasons (default `Q3464665`), episodes (default `Q21191270`), and characters (default `Q15632617 Q15773347 Q15773317 Q15711870 Q80447738 Q118247723 Q123126876`). Defaults are seeded into the database `T_WC_SERVER_VARIABLE` table on first run and can be edited there to expand or restrict the scope without code changes.
- **Resume-friendly**: progress markers (`strsparqlaltcrawler*currentvalue`, `strsparqlaltcrawler*currentprocess`, runtime, start/end timestamps) are written to the `T_WC_SERVER_VARIABLE` table after every entity so a restart picks up where the previous run stopped.
- **Idempotent upserts** through `cp.f_sqlupdatearray(...)` in [citizenphil.py](citizenphil.py), which inserts or updates by primary key (`ID_WIKIDATA`).
- **Rich Wikidata properties** captured per entity:
  - Movies / series: IMDb (P345), TMDb (P4947 / P4983), release/start date (P577 / P580), genres (P136), Plex media key (P11460), Criterion film ID (P9584) and spine (P12279), color (P462), instance-of type.
  - Seasons: IMDb (P345), season start P580 / end P582, parent series via `p:P179` with the season number taken from the `pq:P1545` series-ordinal qualifier, instance-of type. Parent series TMDb ID is back-filled from `T_WC_WIKIDATA_SERIE_V1`.
  - Episodes: IMDb (P345), publication date P577, parent series via P179, parent season via `p:P4908` with the episode number taken from the `pq:P1545` series-ordinal qualifier, instance-of type. Parent series and season TMDb IDs and the season ordinal are back-filled from `T_WC_WIKIDATA_SERIE_V1` and `T_WC_WIKIDATA_SEASON_V1`.
  - Persons: IMDb (P345), TMDb (P4985), birth date (P569), death date (P570), instance-of class.
  - Characters: itemLabel as NAME (the only required field), IMDb (P345), birth date (P569), death date (P570), instance-of class, plus English aliases via `skos:altLabel` accumulated into the `ALIASES` column.
- **Graceful SPARQL error handling**: `EndPointInternalError`, `QueryBadFormed`, `EndPointNotFound`, and rate-limit-style failures fall through to a 60-second retry loop.
- **Polite request pacing**: 5 s (persons by year), 90 s (movies / series / seasons / episodes / characters by year), and 2 s (item-driven sub-queries) between requests, plus a `WIKIMEDIA_USER_AGENT` header on every call. The item-driven scopes `103` / `104` / `106` / `107` / `109` / `110` issue one batched SPARQL call per 500 staged ids rather than one per id, so the 2 s sleep is per batch. The serie-driven scopes `113` and `114` use smaller batches and longer sleeps — 100 series ids with a 5 s sleep for seasons, and 25 series ids with a 10 s sleep for episodes — because each series can fan out to many seasons / episodes and Wikidata's 30 s query timeout is the real ceiling. The work- and person-driven character scopes `115` and `116` use 25 and 15 ids per batch respectively with a 10 s sleep, for the same fan-out / timeout reason (cast lists on long-running shows and prolific actors are large).
- **POST for every SPARQL request**: all six crawlers (`f_sparqlpersonscrawl`, `f_sparqlmoviescrawl`, `f_sparqlseriescrawl`, `f_sparqlseasonscrawl`, `f_sparqlepisodescrawl`, `f_sparqlcharactercrawl`) call `sparql.setMethod(POST)`. A 500-id `VALUES { wd:Q1 wd:Q2 … }` clause overflows the WDQS nginx request-URI limit when sent as GET (HTTP 414); POST puts the query in the request body and removes that ceiling.

---

## Repository layout

| Path | Role |
| --- | --- |
| [sparql-movies-persons.py](sparql-movies-persons.py) | Main entry point. Defines `f_sparqlpersonscrawl`, `f_sparqlmoviescrawl`, `f_sparqlseriescrawl`, `f_sparqlseasonscrawl`, `f_sparqlepisodescrawl`, `f_sparqlcharactercrawl`, and drives the sixteen-scope loop. `f_sparqlseasonscrawl` and `f_sparqlepisodescrawl` accept an optional `strseriewikidataidquery` argument that switches them to series-rooted discovery via P179 backlinks (scopes `113` / `114`). `f_sparqlcharactercrawl` accepts optional `strworkwikidataidquery` and `strpersonwikidataidquery` arguments that switch it to work-rooted (P674 / P161+P453) or person-rooted (P161+P453 reverse) discovery (scopes `115` / `116`). |
| [citizenphil.py](citizenphil.py) | Shared helper library: MariaDB connection management, `f_sqlupdatearray` upsert helper, server-variable getter/setter, time / duration utilities, TMDb-API plumbing. |
| [requirements.txt](requirements.txt) | Python dependencies (SPARQLWrapper, pymysql, pandas, requests, python-dotenv, etc.). |
| [Dockerfile](Dockerfile) | Slim Python 3.10 image that installs the requirements and runs the crawler. |
| [.dockerignore](.dockerignore) | Excludes `.env`, `citizenphilsecrets.py`, `.git/`, `__pycache__/`, and editor state from the build context so secrets and noise cannot end up in image layers. |
| [sparql-movies-persons.sh](sparql-movies-persons.sh) | Host-side launcher: builds the image and starts the container detached on the host network. |
| [on.sh](on.sh) / [off.sh](off.sh) | Toggle scripts that rename the launcher between an "on" and "off" filename — used by an external cron / supervisor to enable or disable the scheduled run. |
| [AGENTS.md](AGENTS.md) | Coding conventions and agent guide (Hungarian notation, SQL naming, function prefixes). |
| [doc/sql/](doc/sql/) | Reference DDL — `Wikidata-tables.sql` for the Wikidata tables, `T_WC_SERVER_VARIABLE.sql` for the server-variable table. |

---

## Requirements

- Python 3.10 (the Docker image pins `python:3.10.5-slim-buster`)
- A MariaDB or MySQL server reachable from the host running the crawler
- Network access to `https://query.wikidata.org/sparql`
- A descriptive `WIKIMEDIA_USER_AGENT` (Wikimedia requires a contact-bearing UA on SPARQL traffic)
- Optional: Docker, if you intend to run the container via `sparql-movies-persons.sh`

Python packages (installed from [requirements.txt](requirements.txt)):

```
numpy, pandas, requests, pymysql, beautifulsoup4, lxml, html5lib,
schedule, pytz, thefuzz, SPARQLWrapper, python-dotenv>=1.0.0
```

---

## Configuration

The crawler reads its configuration from a `.env` file in the project root (loaded via `python-dotenv`). Copy [.env.example](.env.example) to `.env` and fill in real values:

```dotenv
# Wikimedia requires a descriptive User-Agent including a contact URL or e-mail
WIKIMEDIA_USER_AGENT=Your user agent here

# Target database
DB_HOST=your_database_host
DB_PORT=3306
DB_NAME=your_database_name
DB_USER=your_mysql_username
DB_PASSWORD=your_mysql_password
DB_NAMESPACE=T_WC_

USER_TIMEZONE=Europe/Paris
```

Beyond the file-based config, the following knobs live in the `T_WC_SERVER_VARIABLE` table and can be tuned at runtime without redeploying:

| Server variable | Purpose |
| --- | --- |
| `strsparqlaltcrawlerpersoninstanceof` | Space-separated list of Wikidata QIDs treated as "person" instance-of classes. |
| `strsparqlaltcrawlermovieinstanceof` | Same, for movie classes. |
| `strsparqlaltcrawlerserieinstanceof` | Same, for series classes. |
| `strsparqlaltcrawlerseasoninstanceof` | Same, for TV season classes. |
| `strsparqlaltcrawlerepisodeinstanceof` | Same, for TV episode classes. |
| `strsparqlaltcrawlercharacterinstanceof` | Same, for fictional-character classes. |
| `strsparqlaltcrawler*currentprocess` / `*currentvalue` | Progress markers (one per scope), used for resuming and monitoring. |
| `strsparqlaltcrawlerstartdatetime` / `enddatetime` / `totalruntime` | Run-level timing data persisted at the end of each cycle. |

Defaults for the six `*instanceof` variables are seeded automatically on first run, so the table only needs hand-editing to *change* the scope.

---

## Database schema

Reference DDL lives under [doc/sql/](doc/sql/) and is the source of truth for table shapes; the crawler does not create tables itself. The main destinations are:

- `T_WC_WIKIDATA_PERSON_V1` — persons, keyed by `ID_WIKIDATA` (e.g. `Q6691`). Scope `116` re-reads this table on every run to drive person-rooted character discovery; rows are *not* deleted after processing.
- `T_WC_WIKIDATA_MOVIE_V1` — movies, keyed by `ID_WIKIDATA`. Scope `115` re-reads this table on every run (alongside `T_WC_WIKIDATA_SERIE_V1`) to drive work-rooted character discovery; rows are *not* deleted after processing.
- `T_WC_WIKIDATA_SERIE_V1` — TV series, keyed by `ID_WIKIDATA`. Scopes `113` / `114` / `115` also re-read this table on every run to drive series-rooted season / episode / character discovery; rows are *not* deleted after processing.
- `T_WC_WIKIDATA_SEASON_V1` — TV seasons, keyed by `ID_WIKIDATA`, with `ID_WIKIDATA_SERIE` / `ID_SERIE` and `SEASON_NUMBER` pointing to the parent series
- `T_WC_WIKIDATA_EPISODE_V1` — TV episodes, keyed by `ID_WIKIDATA`, with `ID_WIKIDATA_SERIE` / `ID_SERIE`, `ID_WIKIDATA_SEASON` / `ID_SEASON`, `SEASON_NUMBER`, and `EPISODE_NUMBER` linking back to the parent series and season
- `T_WC_WIKIDATA_CHARACTER_V1` — fictional characters, keyed by `ID_WIKIDATA`
- `T_WC_WIKIDATA_ITEM_V1` — staging of items discovered elsewhere; scopes `103` / `104` / `106` / `107` / `109` / `110` consume rows here and delete them once promoted to the typed `*_V1` table.
- `T_WC_WIKIDATA_ITEM_PROPERTY` — multi-valued properties (e.g. genres P136, colors P462) attached to a `ID_WIKIDATA`; scopes that own the row clean up stale rows so the values in the table always match the latest SPARQL result.
- `T_WC_SERVER_VARIABLE` — name/value/description rows used for both crawler configuration and progress reporting.

Naming follows the project-wide conventions documented in [AGENTS.md](AGENTS.md): uppercase snake-case, `T_WC_` prefix for persistent tables, `ID_*` primary keys, `DAT_*` / `TIM_*` for dates and timestamps, `IS_*` for boolean flags.

---

## Installation and run

### Local Python

```bash
python -m venv .venv
source .venv/bin/activate          # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env               # then edit .env
python sparql-movies-persons.py
```

The script logs each SPARQL query, every upserted row, and the per-scope progress markers to standard output. A full sweep takes hours to days depending on the configured year range and how saturated the destination database already is — the crawler is designed to be left running and respawned on completion.

### Docker

**Secrets handling.** The `.env` file is excluded from the build context by [.dockerignore](.dockerignore) and is **never** baked into the image — no `COPY` of `.env`, no `ENV` lines containing secrets in the [Dockerfile](Dockerfile). Instead, credentials are injected at run time with `--env-file` pointing at a host-managed file that lives **outside** the application source tree. This keeps secrets out of image layers, the build cache, and any registry the image might later be pushed to.

Build and run a one-shot container:

```bash
docker build -t sparql-movies-persons-python-app .

docker run --rm --network=host \
    --env-file /home/debian/docker/sparql-movies-persons/.env \
    --name sparql-movies-persons \
    sparql-movies-persons-python-app
```

Adjust the path after `--env-file` to wherever you keep the env file on your host. The repository's own [.env](./.env) (used for `python sparql-movies-persons.py` runs from a clone) is git- and docker-ignored; the host copy is kept at a separate path so a fresh `git clone` cannot accidentally pull it in.

Or use [sparql-movies-persons.sh](sparql-movies-persons.sh) on the host (this is what the production server runs from cron). The script:

1. Checks whether a container named `sparql-movies-persons` is already running and exits if so;
2. Otherwise rebuilds the image from `/home/debian/docker/sparql-movies-persons` and starts it detached with `--network=host`, `--rm`, and `--env-file /home/debian/docker/sparql-movies-persons/.env`. The env file is read by Docker at container start and the values become environment variables inside the container only — they are not persisted in the image.

`on.sh` / `off.sh` are a deliberately minimal toggle: cron is configured to look for `sparql-movies-persons.sh`, and renaming the file in or out of that name pauses the cycle without touching the crontab itself.

---

## Operational notes

- **Run cadence**: each scope sleeps between iterations (5 s, 90 s, or 2 s). Do not shorten these without a documented reason — Wikidata's SPARQL endpoint enforces fair-use limits and an aggressive crawler will be throttled, which then cascades into the retry loop and slows the whole run further.
- **Resuming after a crash**: the script always re-runs from the start of `arrwikidatascope`, but because every upsert is idempotent and progress markers are persisted, restarts are safe. To skip a long-running scope on the next run, comment it out of the dict in [sparql-movies-persons.py:1507](sparql-movies-persons.py#L1507).
- **Adding a new instance-of class**: edit the relevant `strsparqlaltcrawler*instanceof` row in `T_WC_SERVER_VARIABLE`. The next run will pick up the new QIDs automatically.
- **Date handling**: birth, death, and release dates are parsed as `%Y-%m-%dT%H:%M:%SZ`; values that fail to parse are silently dropped. Wikidata's BCE / partial dates therefore appear as `NULL` rather than raising.
- **Label sanitisation**: any `itemLabel` that matches the regex `^[QPL]\d+$` is rejected (these are Wikidata fallback IDs leaking into the label slot), preventing rows like `NAME = 'Q12345'`.

---

## Code conventions

The project uses legacy Hungarian-prefixed variables (`str`, `lng`, `dbl`, `arr`, `int`), `f_` prefixes on public functions, and Google-style docstrings on shared helpers — see [AGENTS.md](AGENTS.md) for the full list before editing. Keep markdown, prompts, and SQL UTF-8; the dataset is heavily multilingual and any encoding rewrite will introduce mojibake.

---

## License

Internal Citizen Phil project — see repository owner for licensing terms.
