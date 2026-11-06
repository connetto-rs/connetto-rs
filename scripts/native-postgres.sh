#!/usr/bin/env bash
# Makes the stopped template cluster each test fixture copies and starts in
# place of a Postgres container, and names it in the two variables selecting
# it, which go to GITHUB_ENV when it is set and to standard output otherwise.
#
#   eval "$(scripts/native-postgres.sh DIR [BIN])"
#   cargo nextest run --profile native --cargo-profile testfast --all-features
#
# BIN holds PostgreSQL 16's initdb and pg_ctl, matching the postgres:16 image.
set -euo pipefail

dir=$(realpath -m "$1")
bin=${2:-/usr/lib/postgresql/16/bin}

if ! "$bin/postgres" --version | grep -q ' 16\.'; then
  echo "$bin does not hold PostgreSQL 16" >&2
  exit 1
fi
if [ ! -f "$dir/template/PG_VERSION" ]; then
  mkdir -p "$dir"
  "$bin/initdb" -D "$dir/template" -U postgres --auth=trust --no-sync > "$dir/initdb.log"
fi
variables=(
  "CONNETTO_POSTGRES_BIN=$bin"
  "CONNETTO_POSTGRES_TEMPLATE=$dir/template"
)
if [ -n "${GITHUB_ENV:-}" ]; then
  printf '%s\n' "${variables[@]}" >> "$GITHUB_ENV"
else
  printf 'export %s\n' "${variables[@]}"
fi
