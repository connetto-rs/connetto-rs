#!/usr/bin/env bash
# R71 proof 4 without a screen: gnome-keyring's own dialogs on a virtual display.
# Usage: run.sh PROBE OUT_DIR
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
if ! log="$(docker build --progress=plain -t connetto-r71-gnome "$here" 2>&1)"; then
  printf '%s\n' "$log" >&2
  exit 1
fi
mkdir -p "$2"; chmod 777 "$2"
docker run --rm -v "$1:/probe:ro" -v "$here/scenario.sh:/scenario.sh:ro" -v "$2:/out" connetto-r71-gnome \
  bash -c 'Xvfb :99 -screen 0 1024x768x24 >/dev/null 2>&1 & export DISPLAY=:99 XDG_DATA_HOME=/tmp/data; sleep 1; dbus-run-session -- bash /scenario.sh'
