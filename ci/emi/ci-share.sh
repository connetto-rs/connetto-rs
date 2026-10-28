#!/usr/bin/env bash
# Sets how much of one cluster member CI may use, and sizes GARM's scale sets to all members' shares together.
#
# Run as: sudo bash ci-share.sh MEMBER MEMORY_GIB VMS   (on emi_machine)
#         sudo bash ci-share.sh commons 0 0             (commons gives CI nothing)
# Lowering a share stops new runner VMs there, and the VMs already there finish their jobs.
set -euo pipefail

PROJECT=ci
RUNNER_USER=ci-runner
GARM_CLI=/srv/ci-runner/garm/bin/garm-cli

usage() { echo "usage: sudo bash ci-share.sh MEMBER MEMORY_GIB VMS" >&2; exit 1; }
fail() { echo "$*" >&2; exit 1; }
number() { [[ $1 =~ ^(0|[1-9][0-9]*)$ ]]; }
garm() { sudo -u "$RUNNER_USER" -H "$GARM_CLI" "$@"; }
# incus cluster get fails on a key that was never set, and a member without a share is normal.
member_key() { incus query "/1.0/cluster/members/$1" | jq -r --arg key "user.ci.$2" '.config[$key] // ""'; }
flavor_gib() {
  local value
  value=$(incus profile get "$1" limits.memory --project "$PROJECT")
  [[ $value =~ ^[1-9][0-9]*GiB$ ]] || fail "profile $1 has limits.memory $value, expected a whole number of GiB"
  echo "${value%GiB}"
}

[ $# -eq 3 ] || usage
member=$1 memory=$2 vms=$3
{ number "$memory" && number "$vms"; } || usage
[ "$(id -u)" -eq 0 ] || fail "run this with sudo"
{ [ "$memory" -eq 0 ] && [ "$vms" -eq 0 ]; } || { [ "$memory" -gt 0 ] && [ "$vms" -gt 0 ]; } \
  || fail "MEMORY_GIB and VMS are both 0 or both above 0"

members=$(incus cluster list --format csv --columns n)
grep -qx "$member" <<<"$members" || fail "$member is not a cluster member: $(echo "$members" | paste -sd' ')"
ceiling_memory=$(member_key "$member" ceiling_memory_gib)
ceiling_vms=$(member_key "$member" ceiling_vms)
{ number "$ceiling_memory" && number "$ceiling_vms"; } || fail "$member has no CI ceiling yet: run host-setup.sh emi"
{ [ "$memory" -le "$ceiling_memory" ] && [ "$vms" -le "$ceiling_vms" ]; } \
  || fail "$member may give CI at most ${ceiling_memory} GiB and $ceiling_vms VMs"

light_gib=$(flavor_gib ci-light)
heavy_gib=$(flavor_gib ci-heavy)
scale_sets=$(garm scaleset list --format json)
scale_set_of() {
  local ids
  ids=$(jq -r --arg flavor "$1" '.[] | select(.enabled == true and .flavor == $flavor) | .id' <<<"$scale_sets")
  [ "$(grep -c . <<<"$ids")" -eq 1 ] || fail "expected one enabled GARM scale set with flavor $1, found: ${ids//$'\n'/ }"
  echo "$ids"
}
light_set=$(scale_set_of ci-light)
heavy_set=$(scale_set_of ci-heavy)

incus cluster set "$member" user.ci.memory_gib="$memory"
incus cluster set "$member" user.ci.vms="$vms"

total_memory=0 total_vms=0
while read -r name; do
  share_memory=$(member_key "$name" memory_gib)
  share_vms=$(member_key "$name" vms)
  { number "$share_memory" && number "$share_vms"; } || continue
  echo "$name: ${share_memory} GiB, $share_vms VMs"
  total_memory=$((total_memory + share_memory))
  total_vms=$((total_vms + share_vms))
done <<<"$members"

# Half the memory goes to heavy VMs and the rest to light ones.
heavy=$((total_memory / 2 / heavy_gib))
heavy=$((heavy < total_vms ? heavy : total_vms))
light=$(((total_memory - heavy * heavy_gib) / light_gib))
light=$((light < total_vms - heavy ? light : total_vms - heavy))

garm scaleset update "$light_set" --max-runners "$light" >/dev/null
garm scaleset update "$heavy_set" --max-runners "$heavy" >/dev/null
echo "GARM: scale set $light_set ci-light at most $light runners, scale set $heavy_set ci-heavy at most $heavy"
