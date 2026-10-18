#!/usr/bin/env bash
#
# verify_sparkrca_access.sh — run FROM THE COLLECTOR HOST after provisioning.
#
#   ./verify_sparkrca_access.sh application_<cluster ts>_<seq> <host>...
#   ./verify_sparkrca_access.sh application_<cluster ts>_<seq> --hosts hosts.txt
#
# Key, known_hosts file and service account come from the environment or
# sparkrca.conf (see sparkrca.conf.example), same as the collector.
#
# Runs positive and negative tests. The negative tests matter more: they prove
# the account cannot do anything except the one thing it exists for. Do not
# roll out to the fleet until a single node passes all of them.
#
# Each host's key must already be in $SPARKRCA_KNOWN_HOSTS (added with
# ssh-keyscan after checking the fingerprint against the node). Host keys are
# never auto-accepted (and OpenSSH < 7.6 has no accept-new anyway).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=sparkrca_lib.sh
source "$SCRIPT_DIR/sparkrca_lib.sh"
sparkrca_load_conf "$SCRIPT_DIR"
sparkrca_defaults
KEY="$SPARKRCA_KEY"
KNOWN_HOSTS="$SPARKRCA_KNOWN_HOSTS"
USER_NAME="$SPARKRCA_USER"
SSH="ssh -i $KEY -o BatchMode=yes -o LogLevel=ERROR -o ConnectTimeout=10 \
     -o UserKnownHostsFile=$KNOWN_HOSTS -o StrictHostKeyChecking=yes -o CheckHostIP=no"

APP_ID=""; HOSTS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --hosts) mapfile -t HOSTS < <(grep -vE '^\s*(#|$)' "$2"); shift 2 ;;
    application_*) APP_ID="$1"; shift ;;
    *) HOSTS+=("$1"); shift ;;
  esac
done
[[ -n "$APP_ID" && ${#HOSTS[@]} -gt 0 ]] || {
  echo "usage: $0 application_<id> <host>... | --hosts file" >&2; exit 2; }
[[ -r "$KEY" ]] || { echo "private key $KEY not readable (set SPARKRCA_KEY)" >&2; exit 2; }

pass=0; fail=0
ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; ((pass++)); }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; ((fail++)); }

# Every test captures output first and greps it second. Never grep an ssh
# pipeline directly: under pipefail a refused call (rc 2/255, the *expected*
# outcome) fails the whole pipeline even when grep matched.
for h in "${HOSTS[@]}"; do
  echo; echo "=== $h ==="
  sparkrca_valid_host "$h" || { bad "invalid hostname: $h"; continue; }

  if ! ssh-keygen -F "$h" -f "$KNOWN_HOSTS" >/dev/null 2>&1; then
    bad "host key for $h not in $KNOWN_HOSTS — ssh-keyscan it and check the fingerprint first"
    continue
  fi

  # -- positive: the intended call works and returns real content -----------
  out="$($SSH "$USER_NAME@$h" "$APP_ID" </dev/null 2>&1)"; rc=$?
  if [[ $rc -eq 0 ]] && grep -q '### host:' <<<"$out"; then
    ok "probe returns evidence ($(wc -l <<<"$out") lines)"
    grep -q 'oom-killer victims'   <<<"$out" && ok "oom victim section present"   || bad "oom victim section missing"
    grep -q 'co-tenant services'   <<<"$out" && ok "co-tenancy section present"   || bad "co-tenancy section missing"
    grep -qiE 'permission denied|sudo:|unbound variable' <<<"$out" \
      && bad "errors inside output — check sudoers / probe" || ok "no permission errors"
  else
    bad "probe call failed (rc=$rc): $(grep -v 'not authorized' <<<"$out" | head -2)"
    continue
  fi

  # -- negative: arbitrary commands must be refused -------------------------
  for evil in 'id' 'cat /etc/shadow' 'application_1_1; id' '../../bin/sh' ''; do
    out="$($SSH "$USER_NAME@$h" "$evil" </dev/null 2>&1)"; rc=$?
    if [[ $rc -ne 0 ]] && grep -qi 'refused' <<<"$out"; then
      ok "refused: '${evil:-<empty>}'"
    else
      bad "NOT REFUSED: '${evil:-<empty>}' (rc=$rc) -> $(head -1 <<<"$out")"
    fi
  done

  # -- negative: no interactive shell, no forwarding ------------------------
  out="$(timeout 15 $SSH -T "$USER_NAME@$h" </dev/null 2>&1)"; rc=$?
  [[ $rc -ne 0 ]] && grep -qi 'refused' <<<"$out" \
    && ok "no shell without a command" || bad "shell may be reachable (rc=$rc) -> $(head -1 <<<"$out")"

  out="$(timeout 15 $SSH -W localhost:22 "$USER_NAME@$h" </dev/null 2>&1)"; rc=$?
  [[ $rc -ne 0 ]] && grep -qiE 'stdio forwarding failed|administratively prohibited' <<<"$out" \
    && ok "port forwarding blocked" || bad "port forwarding may be allowed (rc=$rc) -> $(head -1 <<<"$out")"

  # -- refusals must be auditable ------------------------------------------
  echo "  note: confirm refusals were logged with the collector IP — on $h run:"
  echo "        sudo journalctl -t sparkrca_probe --since '10 min ago' | tail"
done

echo
echo "passed: $pass   failed: $fail"
[[ $fail -eq 0 ]] || exit 1
