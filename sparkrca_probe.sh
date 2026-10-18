#!/usr/bin/env bash
#
# sparkrca_probe.sh — node-side evidence probe for Spark-on-YARN RCA.
#
# DEPLOY: /usr/local/bin/sparkrca_probe.sh on every NodeManager host,
#         owned by root, mode 0755. NOT writable by svc-sparkrca.
#
# INVOCATION: never called directly. SSH ForceCommand runs it and passes the
# caller's requested command in $SSH_ORIGINAL_COMMAND, which must be a bare
# YARN application ID and nothing else.
#
# This is the entire remote attack surface. The caller chooses ONE thing:
# which application ID to grep for. It cannot choose commands, paths, or flags.
#
# Read-only. Writes nothing. Exits non-zero on a malformed request.

set -uo pipefail
umask 077

# must be running as root, which only happens via the sudo call in
# authorized_keys command=. Direct invocation as svc-sparkrca stops here.
[[ "$(id -u)" == "0" ]] || { echo "must run via sudo ForceCommand" >&2; exit 3; }

# ---- site paths ----------------------------------------------------------
# Defaults cover common package layouts. Override per site in PROBE_CONF,
# written by setup_sparkrca_node.sh. sudo resets the environment, so the
# config file is the only way in. It is parsed, never sourced, and ignored
# unless root-owned and not group/world-writable: this script runs as root.
PROBE_CONF="/etc/sparkrca/probe.conf"
NM_LOG_GLOB="/var/log/hadoop-yarn/*nodemanager*.log* /var/log/hadoop/yarn/*nodemanager*.log* /opt/hadoop/logs/*nodemanager*.log*"
SYSLOG_GLOB="/var/log/messages* /var/log/syslog* /var/log/kern.log*"
YARN_SITE="/etc/hadoop/conf/yarn-site.xml"

if [[ -f "$PROBE_CONF" ]]; then
  read -r conf_owner conf_mode < <(stat -c '%U %a' "$PROBE_CONF")
  if [[ "$conf_owner" == "root" ]] && ! (( 8#$conf_mode & 8#022 )); then
    while IFS='=' read -r k v; do
      v="${v%\"}"; v="${v#\"}"
      case "$k" in
        NM_LOG_GLOB) NM_LOG_GLOB="$v" ;;
        SYSLOG_GLOB) SYSLOG_GLOB="$v" ;;
        YARN_SITE)   YARN_SITE="$v" ;;
      esac
    done < "$PROBE_CONF"
  else
    echo "sparkrca_probe: ignoring $PROBE_CONF (owner $conf_owner, mode $conf_mode)" >&2
  fi
fi
# --------------------------------------------------------------------------

# sudo resets the environment; SSH_CLIENT only survives if sudoers env_keeps
# it. Never let a missing audit field abort the probe under set -u.
CALLER="${SSH_CLIENT:-unknown}"
CALLER="${CALLER%% *}"

# ---- validate the one caller-supplied value ------------------------------
REQ="${SSH_ORIGINAL_COMMAND:-}"
if [[ ! "$REQ" =~ ^application_[0-9]{1,20}_[0-9]{1,10}$ ]]; then
  echo "sparkrca_probe: refused. expected a bare application ID, got: ${REQ:0:80}" >&2
  logger -t sparkrca_probe "REFUSED request from $CALLER: ${REQ:0:200}"
  exit 2
fi
APP_ID="$REQ"
logger -t sparkrca_probe "probe for $APP_ID from $CALLER"

section() { printf '\n--- %s ---\n' "$1"; }

echo "### host: $(hostname -f)"
echo "### app: $APP_ID"
echo "### collected: $(date -Is)"

section "probe paths"
# An empty match makes every section below silently blank; say so up front.
for name in NM_LOG_GLOB SYSLOG_GLOB YARN_SITE; do
  n=0; for g in ${!name}; do n=$(( n + $(compgen -G "$g" | wc -l) )); done
  printf '%-12s %s  [%s]\n' "$name" "$([[ $n -gt 0 ]] && echo "$n files" || echo MISSING)" "${!name}"
done

section "memory"
free -g
echo
ps -eo rss,comm --no-headers --sort=-rss | head -25

section "oom-killer invocations by day"
grep -h 'invoked oom-killer' $SYSLOG_GLOB 2>/dev/null \
  | awk '{print $1, $2}' | sort | uniq -c

section "oom-killer victims"
# The invoker is NOT the victim. Only these lines name what actually died.
grep -hE 'Out of memory: Kill process|Killed process' $SYSLOG_GLOB 2>/dev/null | tail -40

section "nodemanager entries for this app"
grep -h -- "$APP_ID" $NM_LOG_GLOB 2>/dev/null \
  | grep -iE 'kill|memory|preempt|unhealthy|local-dirs|DiskChecker|failed|EXITED' | tail -40

section "nodemanager health and disk"
grep -hiE 'local-dirs are bad|DiskChecker|Disk(s)? failed|unhealthy|Node is (un)?healthy' \
  $NM_LOG_GLOB 2>/dev/null | tail -20
echo
df -h | grep -vE 'tmpfs|udev'

section "yarn advertised capacity"
grep -A2 -E 'resource\.memory-mb|resource\.cpu-vcores' "$YARN_SITE" 2>/dev/null

section "co-tenant services"
# Non-Hadoop resident processes. A NodeManager sharing a box with postgres,
# redis or httpd means yarn.nodemanager.resource.memory-mb is advertising
# memory those services already hold — YARN cannot see non-container usage.
ps -eo comm --no-headers | sort -u \
  | grep -viE 'kworker|ksoftirq|migration|rcu_|kthread|scsi_|jbd2|irq/|systemd-'

section "uptime"
uptime

exit 0
