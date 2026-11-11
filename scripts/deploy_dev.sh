#!/usr/bin/env bash
# Deploy and run the lakehouse on the `dev` target (Databricks Free Edition, serverless).
#
# Usage (Git Bash or any POSIX shell, from the repository root):
#   scripts/deploy_dev.sh <profile>                # deploy, upload raw files and wheel, run the pipeline
#   scripts/deploy_dev.sh <profile> --full-refresh # same, with a full refresh (needed after a schema or
#                                                  # table-property change, e.g. a new contract column)
#   scripts/deploy_dev.sh <profile> --no-run       # deploy and upload only
#
# <profile> is a Databricks CLI profile in ~/.databrickscfg. It is passed to every command: without it the
# CLI would fall back to the default profile, which may point to another workspace.
# The CLI is taken from $DATABRICKS_CLI if set, otherwise `databricks` on PATH.
# Raw files must exist in data/raw (run `uv run scripts/fetch_sources.py` first).
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash: keep dbfs:/ and /Volumes paths as they are

PROFILE="${1:?usage: scripts/deploy_dev.sh <profile> [--full-refresh|--no-run]}"
MODE="${2:-}"
TARGET=dev
cd "$(dirname "$0")/.."

# Find the CLI: $DATABRICKS_CLI, then `databricks` on PATH, then the copy bundled with the VS Code
# Databricks extension under the Windows user folder (Git Bash, MSYS2/UCRT64 and WSL-style paths).
find_cli() {
  if [ -n "${DATABRICKS_CLI:-}" ] && [ -x "$DATABRICKS_CLI" ]; then echo "$DATABRICKS_CLI"; return; fi
  if command -v databricks >/dev/null 2>&1; then command -v databricks; return; fi
  local homes=() h c last=""
  [ -n "${USERPROFILE:-}" ] && homes+=("$USERPROFILE")
  if [ -n "${USERPROFILE:-}" ] && command -v cygpath >/dev/null 2>&1; then homes+=("$(cygpath -u "$USERPROFILE")"); fi
  [ -n "${USERNAME:-}" ] && homes+=("/c/Users/$USERNAME" "/mnt/c/Users/$USERNAME")
  homes+=("$HOME")
  for h in "${homes[@]}"; do
    for c in "$h"/.vscode/extensions/databricks.databricks-*/bin/databricks.exe; do
      [ -x "$c" ] && last="$c"
    done
    if [ -n "${last:-}" ]; then echo "$last"; return; fi
  done
  echo "Databricks CLI not found. Tried: \$DATABRICKS_CLI='${DATABRICKS_CLI:-}', PATH, and" \
       "<home>/.vscode/extensions/databricks.databricks-*/bin/databricks.exe for: ${homes[*]}" >&2
  return 1
}
DBX="$(find_cli)" || exit 1
echo "Databricks CLI: $DBX ($("$DBX" --version 2>/dev/null | head -1))"
command -v uv >/dev/null 2>&1 || { echo "uv not found on PATH (needed to build the wheel): add its folder, e.g. export PATH=\"\$PATH:/c/Users/\$USERNAME/.local/bin\"" >&2; exit 1; }
ls data/raw/beds_*.csv data/raw/population_istat.csv >/dev/null 2>&1 || { echo "data/raw is empty: run 'uv run scripts/fetch_sources.py'" >&2; exit 1; }

echo "== 1/5 validate"
"$DBX" bundle validate -t "$TARGET" -p "$PROFILE"

echo "== 2/5 deploy (schema, volume, pipeline)"
"$DBX" bundle deploy -t "$TARGET" -p "$PROFILE"

# The dev target runs in development mode, which prefixes the schema (dev_<user>_healthcare_lakehouse):
# read the resolved names from the bundle instead of assuming them.
read -r CATALOG SCHEMA VOLUME < <("$DBX" bundle summary -t "$TARGET" -p "$PROFILE" -o json | uv run --no-project python -c "
import json, sys
r = json.load(sys.stdin)['resources']
s = r['schemas']['lakehouse_schema']; v = r['volumes']['landing']
print(s['catalog_name'], s['name'], v['name'])" | tr -d '\r')
# Windows Python ends lines with CRLF: without the tr above, VOLUME would be "landing<CR>" and every
# upload would fail with an empty "databricks-api error". Check the names before using them.
for part in "$CATALOG" "$SCHEMA" "$VOLUME"; do
  [[ "$part" =~ ^[A-Za-z0-9_]+$ ]] || { echo "Unexpected name from the bundle: '$part'" >&2; exit 1; }
done
LANDING="dbfs:/Volumes/$CATALOG/$SCHEMA/$VOLUME"
echo "   landing volume: $LANDING"

echo "== 3/5 build the wheel"
uv build --wheel
WHEEL="$(ls -t dist/healthcare_lakehouse-*.whl | head -1)"

echo "== 4/5 upload raw files and wheel"
# Uploads to a Volume can fail transiently with an empty "databricks-api error": retry them.
retry() {
  local n
  for n in 1 2 3; do
    "$@" && return 0
    echo "   attempt $n failed, retrying in $((n * 5)) s..." >&2
    sleep $((n * 5))
  done
  echo "   giving up after 3 attempts: $*" >&2
  return 1
}
retry "$DBX" fs cp -r --overwrite data/raw "$LANDING" -p "$PROFILE"
retry "$DBX" fs mkdir "$LANDING/_libs" -p "$PROFILE"
# Copy to the full file name: a trailing-slash destination that does not exist becomes a file.
retry "$DBX" fs cp --overwrite "$WHEEL" "$LANDING/_libs/$(basename "$WHEEL")" -p "$PROFILE"

if [ "$MODE" = "--no-run" ]; then
  echo "== 5/5 skipped (--no-run)"
  exit 0
fi

echo "== 5/5 run the pipeline"
if [ "$MODE" = "--full-refresh" ]; then
  "$DBX" bundle run lakehouse -t "$TARGET" -p "$PROFILE" --full-refresh-all
else
  "$DBX" bundle run lakehouse -t "$TARGET" -p "$PROFILE"
fi
echo "Done. Gold tables: $CATALOG.$SCHEMA (dim_*, fact_*)."
