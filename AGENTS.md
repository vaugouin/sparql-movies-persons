# AGENTS.md - Agent Guide for Sparql Alt Crawler

Rules and traps for agents. Project overview, scopes, configuration, server variables, database destinations, Docker build and run, operational notes: @README.md (canonical). Reference DDL: `doc/sql/*.sql` (`Wikidata-tables.sql`, `T_WC_SERVER_VARIABLE.sql`), read-only unless the user asks for schema-doc edits; do not duplicate table definitions elsewhere.

Update `README.md` for user-facing behavior, configuration or deployment changes; update this file only when agent workflow or safety context changes.

---

## Related repositories (project ecosystem)

One stage of **Agent BBB** (GitHub `vaugouin`; siblings at `%USERPROFILE%/Code/<repo>` and `github.com/vaugouin/<repo>`, converging on a shared MariaDB with `T_WC_*` tables). Canonical roster and stage map: `%USERPROFILE%/Nestor/projets/t2s-backlog/topics/related-repositories.txt`.

**This repository's role:** acquisition, Wikidata link discovery (V1). Queries the Wikidata Query Service to discover Wikidata QIDs and link them to TMDb movies, series and persons via IMDb/TMDb cross-reference properties, into the `T_WC_WIKIDATA_*_V1` tables. The links are enriched by `sparql-crawler` / `wikidata-crawler` and consumed by `tmdb-movie-preprocess`.

---

## Code conventions

- Legacy Hungarian notation: `str` strings, `lng` integers, `dbl` floats, `arr` lists/dicts, `int` boolean-like flags.
- Public functions use the `f_` prefix, private helpers `_`; Google-style docstrings on shared helpers.
- Write through `cp.f_sqlupdatearray` (idempotent upsert by `ID_WIKIDATA`) and record progress with `cp.f_setservervariable`, so a restart stays safe.
- Do not shorten the sleeps between SPARQL requests without a documented reason: WDQS throttles aggressive clients, which cascades into the 60 s retry loop. Keep every SPARQL call on POST (a 500-id `VALUES` clause overflows the GET URI limit, HTTP 414).
- Keep Markdown, JSON and SQL UTF-8: the data is multilingual and any encoding rewrite introduces mojibake.

---

## SQL Object Naming Conventions

- Uppercase snake case (except legacy TMDb genre columns `id`, `name`). Prefixes: `T_WC_*` persistent, `T_WC_T2S_*` Text2SQL read model, `T_WC_TMDB_*` TMDb, `T_WC_WIKIDATA_*` Wikidata (`STG_T_WC_WIKIDATA_*` staging), `T_WC_WIKIPEDIA_*` Wikipedia. Join tables `T_WC_T2S_{PARENT}_{CHILD}`.
- Keys: `ID_{ENTITY}`, `ID_ROW` for generic/join rows, or a table-specific surrogate; foreign keys reuse the referenced key name.
- Suffixes/prefixes: `DAT_*` dates, `TIM_*` timestamps, `IS_*` or legacy integer flags (`DELETED`), `DISPLAY_ORDER`, `*_COUNT`, `*_PATH`, `_FR` / `LANG` for languages, `*_NORM` / `*_KEY` for RapidFuzz columns.
- Index names are mixed legacy style; preserve it: `KEY COLUMN_NAME`, `IDX_*`, `UK_*`, `FK_*`, `ft_*`.

---

**Last Updated**: 2026-10-31
**Current Version**: 1.0.5

## Backlog (Nestor second-brain)

Backlog for this repo: `C:\Users\vaugo\Nestor\projets\t2s-backlog\repos\sparql-movies-persons.md` (dashboard: `...\t2s-backlog\index.md`). Tasks are `SPARQL-MOVIES-PERSONS-NNN`; consult it before implementing. Local paths on Philippe's PC only, they do not resolve on the VPS or cloud agents.
