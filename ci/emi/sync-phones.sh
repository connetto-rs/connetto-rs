#!/usr/bin/env bash
# Makes the usb devices of Incus profile ci-android match ci/emi/phones, enrolling each phone it adds.
#
# Run as: bash ci/emi/sync-phones.sh   (as lucac on emi, with adb on PATH, each phone to add plugged into emi)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
PROFILE=ci:ci-android
KEY_HOME=/srv/ci-cache/android-adb
ADB_PORT=5139
AUTH_BOUND=120

fail() { echo "$*" >&2; exit 1; }
inc() { incus "$@" </dev/null; }
now() { local up; read -r up _ </proc/uptime; echo "${up%.*}"; }

command -v adb >/dev/null || fail "adb is not on PATH, source ~/.android-env.sh first"
inc remote list --format csv | grep -q '^ci,' || fail "$(id -un) has no incus remote ci, run host-setup.sh emi first"
[ -w "$KEY_HOME" ] || fail "$KEY_HOME is not writable by $(id -un), run host-setup.sh emi first"

declare -A wanted=()
while read -r serial vendor _; do
  [[ -z $serial || $serial == \#* ]] && continue
  [[ $serial =~ ^[A-Za-z0-9]+$ && $vendor =~ ^[0-9a-f]{4}$ ]] || fail "ci/emi/phones has a bad line: $serial $vendor"
  wanted[$serial]=$vendor
done <"$here/phones"

declare -A enrolled=()
while read -r name; do
  [[ $name == phone-* ]] && enrolled[${name#phone-}]=1
done < <(inc profile device list "$PROFILE")

for serial in "${!enrolled[@]}"; do
  [[ -v wanted[$serial] ]] && continue
  inc profile device remove "$PROFILE" "phone-$serial" >/dev/null
  unset "enrolled[$serial]"
  echo "$serial: removed, unplug and replug it to use it on emi's host"
done

# Checks the phone on its own adb server, which reads the CI key as its own key, so the phone is asked to trust that key.
enrol() {
  local serial=$1 adb=(timeout 20 adb -P "$ADB_PORT" -s "$1") deadline asked="" problem problems=()
  adb -P "$ADB_PORT" kill-server >/dev/null 2>&1 || true
  HOME=$KEY_HOME adb -P "$ADB_PORT" --one-device "$serial" start-server >/dev/null \
    || { echo "$serial: its adb server did not start" >&2; return 1; }
  deadline=$(($(now) + AUTH_BOUND))
  until [ "$("${adb[@]}" get-state 2>/dev/null)" = device ]; do
    if [ "$(now)" -ge "$deadline" ]; then
      adb -P "$ADB_PORT" kill-server >/dev/null 2>&1 || true
      echo "$serial: not plugged into emi, or the CI key was not allowed within ${AUTH_BOUND} s" >&2
      return 1
    fi
    if [ -z "$asked" ] && "${adb[@]}" get-state 2>&1 | grep -q unauthorized; then
      echo "$serial: tap Allow on the phone, with 'Always allow from this computer'"
      asked=1
    fi
    sleep 2
  done
  # Verifying with no old credential succeeds only on a phone without a PIN, pattern or password, Swipe included.
  "${adb[@]}" shell locksettings verify >/dev/null 2>&1 \
    || problems+=("has a PIN, pattern or password, which collides with the proof's throwaway PIN")
  # Chrome's state for user 0: 0 default, 1 enabled, 4 disabled until used (Samsung ships it so, and first use enables it).
  chrome=$("${adb[@]}" shell dumpsys package com.android.chrome | tr -d '\r' | grep -m1 -oE 'User 0:.* enabled=[0-9]+' | grep -oE '[0-9]+$' || true)
  case "$chrome" in
    0 | 1 | 4) ;;
    '') problems+=("has no Chrome, which the proof drives, so install it on the phone") ;;
    *) problems+=("has Chrome disabled (state $chrome), which the proof drives, so enable it in Settings, Apps, Chrome") ;;
  esac
  # Android otherwise forgets an allowed key after 7 days without a connection.
  "${adb[@]}" shell settings put global adb_allowed_connection_time 0 \
    || problems+=("refused adb_allowed_connection_time 0")
  adb -P "$ADB_PORT" kill-server >/dev/null 2>&1 || true
  if [ ${#problems[@]} -gt 0 ]; then
    for problem in "${problems[@]}"; do echo "$serial: $problem" >&2; done
    return 1
  fi
}

failed=0
for serial in "${!wanted[@]}"; do
  [[ -v enrolled[$serial] ]] && continue
  if ! grep -qxF "$serial" /sys/bus/usb/devices/*/serial 2>/dev/null; then
    echo "$serial: not plugged into emi, enrolled at the next run once it is" >&2
    failed=1
    continue
  fi
  # A phone the default adb server holds cannot be claimed by another server.
  adb kill-server >/dev/null 2>&1 || true
  if enrol "$serial"; then
    inc profile device add "$PROFILE" "phone-$serial" usb vendorid="${wanted[$serial]}" serial="$serial" >/dev/null
    enrolled[$serial]=1
    echo "$serial: enrolled, it now belongs to the phone VM"
  else
    failed=1
  fi
done

# host-setup.sh's udev rule reads this list, so an enrolled phone is root-only on emi from its next plug-in.
list=$(mktemp "$KEY_HOME/enrolled.XXXXXX")
for serial in "${!enrolled[@]}"; do echo "$serial"; done | sort >"$list"
mv "$list" "$KEY_HOME/enrolled"
exit "$failed"
