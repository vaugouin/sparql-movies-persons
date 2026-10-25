#!/bin/bash
# Switch sparql-movies-persons.sh back on: removes the flag file CRON_OFF that off.sh wrote.
cd "$(dirname "$(readlink -f "$0")")" || exit 1
# A launcher renamed by the old off.sh (<name>-off.sh, before 2026-10-25) is put back,
# or dropped when git has already restored the tracked one.
[ -f sparql-movies-persons-off.sh ] && { [ -f sparql-movies-persons.sh ] && rm -f sparql-movies-persons-off.sh || mv sparql-movies-persons-off.sh sparql-movies-persons.sh; }
rm -f CRON_OFF && echo "sparql-movies-persons: on (CRON_OFF removed)"
