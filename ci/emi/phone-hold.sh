#!/usr/bin/env bash
# Moves an enrolled phone from the phone VM into the hands-on VM ci:phone-hands-on (on), or back (off).
#
# Run as: bash ci/emi/phone-hold.sh SERIAL on|off   (as lucac on emi)
# Then:   incus exec ci:phone-hands-on -- sudo -u runner -H bash   (adb there uses the CI key)
set -euo pipefail

PROFILE=ci:ci-android
VM=ci:phone-hands-on
IMAGE=ci:ci-ubuntu-2404

fail() { echo "$*" >&2; exit 1; }
inc() { incus "$@" </dev/null; }

{ [ $# -eq 2 ] && { [ "$2" = on ] || [ "$2" = off ]; }; } || fail "usage: bash ci/emi/phone-hold.sh SERIAL on|off"
serial=$1
inc profile device list "$PROFILE" | grep -qx "phone-$serial" || fail "$serial is not enrolled, see ci/emi/phones"
vendor=$(inc profile device get "$PROFILE" "phone-$serial" vendorid)

if [ "$2" = on ]; then
  inc profile device set "$PROFILE" "phone-$serial" attached=false
  if ! inc info "$VM" >/dev/null 2>&1; then
    inc launch "$IMAGE" "$VM" --vm --profile default --profile ci-hands-on >/dev/null
    for _ in $(seq 1 90); do inc exec "$VM" -- true 2>/dev/null && break; sleep 2; done
    inc exec "$VM" -- sudo -u runner -H sh -c 'mkdir -p ~/.android && cp /srv/cache/android-adb/.android/adbkey* ~/.android/'
  fi
  inc config device show "$VM" | grep -q "^phone-$serial:" \
    || inc config device add "$VM" "phone-$serial" usb vendorid="$vendor" serial="$serial" >/dev/null
  echo "$serial: in $VM, CI treats it as absent until phone-hold.sh $serial off"
else
  if inc info "$VM" >/dev/null 2>&1; then
    inc config device show "$VM" | grep -q "^phone-$serial:" && inc config device remove "$VM" "phone-$serial" >/dev/null
    inc config device show "$VM" | grep -q '^phone-' || inc delete --force "$VM"
  fi
  inc profile device set "$PROFILE" "phone-$serial" attached=true
  echo "$serial: back with the phone VM"
fi
