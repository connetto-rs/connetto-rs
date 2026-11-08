#!/usr/bin/env bash
# Sets how much of one cluster member CI may use, gives each member as many heavy and light runner slots
# as its own share holds after the phone and hands-on VMs' reserve, and caps GARM's scale sets at their sums.
#
# Run as: sudo bash ci-share.sh MEMBER MEMORY_GIB VMS CPUS   (on emi_machine)
#         sudo bash ci-share.sh commons 0 0 0                (commons gives CI nothing)
# Lowering a share stops new runner VMs there, and the VMs already there finish their jobs.
set -euo pipefail

PROJECT=ci
RUNNER_USER=ci-runner
GARM_CLI=/srv/ci-runner/garm/bin/garm-cli

usage() { echo "usage: sudo bash ci-share.sh MEMBER MEMORY_GIB VMS CPUS" >&2; exit 1; }
fail() { echo "$*" >&2; exit 1; }
number() { [[ $1 =~ ^(0|[1-9][0-9]*)$ ]]; }
min() { echo $(($1 < $2 ? $1 : $2)); }
garm() { sudo -u "$RUNNER_USER" -H "$GARM_CLI" "$@"; }
# incus cluster get fails on a key that was never set, and a member without a share is normal.
member_key() { incus query "/1.0/cluster/members/$1" | jq -r --arg key "user.ci.$2" '.config[$key] // ""'; }
flavor() {
  local memory cpus
  memory=$(incus profile get "$1" limits.memory --project "$PROJECT")
  cpus=$(incus profile get "$1" limits.cpu --project "$PROJECT")
  [[ $memory =~ ^[1-9][0-9]*GiB$ ]] || fail "profile $1 has limits.memory $memory, expected a whole number of GiB"
  [[ $cpus =~ ^[1-9][0-9]*$ ]] || fail "profile $1 has limits.cpu $cpus, expected a CPU count"
  echo "${memory%GiB} $cpus"
}

[ $# -eq 4 ] || usage
member=$1 memory=$2 vms=$3 cpus=$4
{ number "$memory" && number "$vms" && number "$cpus"; } || usage
[ "$(id -u)" -eq 0 ] || fail "run this with sudo"
{ [ "$memory" -eq 0 ] && [ "$vms" -eq 0 ] && [ "$cpus" -eq 0 ]; } \
  || { [ "$memory" -gt 0 ] && [ "$vms" -gt 0 ] && [ "$cpus" -gt 0 ]; } \
  || fail "MEMORY_GIB, VMS and CPUS are all 0 or all above 0"

members=$(incus cluster list --format csv --columns n)
grep -qx "$member" <<<"$members" || fail "$member is not a cluster member: $(echo "$members" | paste -sd' ')"
ceiling_memory=$(member_key "$member" ceiling_memory_gib)
ceiling_vms=$(member_key "$member" ceiling_vms)
ceiling_cpus=$(member_key "$member" ceiling_cpus)
{ number "$ceiling_memory" && number "$ceiling_vms" && number "$ceiling_cpus"; } \
  || fail "$member has no CI ceiling yet: run host-setup.sh emi"
{ [ "$memory" -le "$ceiling_memory" ] && [ "$vms" -le "$ceiling_vms" ] && [ "$cpus" -le "$ceiling_cpus" ]; } \
  || fail "$member may give CI at most ${ceiling_memory} GiB, $ceiling_vms VMs and $ceiling_cpus CPUs"

read -r light_gib light_cpus <<<"$(flavor ci-light)"
read -r heavy_gib heavy_cpus <<<"$(flavor ci-heavy)"
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
incus cluster set "$member" user.ci.cpus="$cpus"

# Half a member's memory and CPUs go to heavy VMs, the rest to light ones, each member on its own,
# since the placement scriptlet holds a runner to its flavor's slots on one member.
slots_of() {
  local memory=$1 vms=$2 cpus=$3 heavy light
  heavy=$(min $((memory / 2 / heavy_gib)) $((cpus / 2 / heavy_cpus)))
  heavy=$(min "$heavy" "$vms")
  light=$(min $(((memory - heavy * heavy_gib) / light_gib)) $(((cpus - heavy * heavy_cpus) / light_cpus)))
  light=$(min "$light" $((vms - heavy)))
  echo "$heavy $light"
}

heavy=0 light=0
while read -r name; do
  share_memory=$(member_key "$name" memory_gib)
  share_vms=$(member_key "$name" vms)
  share_cpus=$(member_key "$name" cpus)
  { number "$share_memory" && number "$share_vms" && number "$share_cpus"; } || continue
  reserved_memory=0 reserved_vms=0 reserved_cpus=0
  if [ "$share_memory" -gt 0 ]; then
    reserved_memory=$(member_key "$name" reserved_memory_gib)
    reserved_vms=$(member_key "$name" reserved_vms)
    reserved_cpus=$(member_key "$name" reserved_cpus)
    reserved_memory=${reserved_memory:-0} reserved_vms=${reserved_vms:-0} reserved_cpus=${reserved_cpus:-0}
    { number "$reserved_memory" && number "$reserved_vms" && number "$reserved_cpus"; } \
      || fail "$name has a malformed user.ci.reserved_* key: run host-setup.sh emi"
    { [ "$reserved_memory" -le "$share_memory" ] && [ "$reserved_vms" -lt "$share_vms" ] && [ "$reserved_cpus" -le "$share_cpus" ]; } \
      || fail "$name reserves ${reserved_memory} GiB, $reserved_vms VMs and $reserved_cpus CPUs for its phone VMs, which leaves no room for a runner in its share"
  fi
  read -r member_heavy member_light <<<"$(slots_of $((share_memory - reserved_memory)) $((share_vms - reserved_vms)) $((share_cpus - reserved_cpus)))"
  incus cluster set "$name" user.ci.heavy_slots="$member_heavy"
  incus cluster set "$name" user.ci.light_slots="$member_light"
  kept=""
  [ "$reserved_vms" -gt 0 ] && kept=", keeping ${reserved_memory} GiB, $reserved_vms VMs and $reserved_cpus CPUs for the phone and hands-on VMs"
  echo "$name: ${share_memory} GiB, $share_vms VMs, $share_cpus CPUs$kept, room for $member_heavy heavy and $member_light light runners"
  heavy=$((heavy + member_heavy))
  light=$((light + member_light))
done <<<"$members"

garm scaleset update "$light_set" --max-runners "$light" >/dev/null
garm scaleset update "$heavy_set" --max-runners "$heavy" >/dev/null
echo "GARM: scale set $light_set ci-light at most $light runners, scale set $heavy_set ci-heavy at most $heavy"
