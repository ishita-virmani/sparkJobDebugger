#!/usr/bin/env bash
#
# collect_spark_evidence.sh — deterministic evidence collector for Spark-on-YARN RCA.
#
#   ./collect_spark_evidence.sh application_<cluster ts>_<seq> \
#       [--owner USER] [--driver-log /path/to/driver.log] [--no-ssh]
#
#   --driver-log  client-mode driver output, i.e. the log of whatever ran
#                 spark-submit (Airflow task log, Oozie launcher, cron output...).
#                 --airflow-log is accepted as an alias.
#
# Site settings (RM / History Server URLs, ssh key, service account, output
# dir, co-tenant service list) come from the environment or sparkrca.conf — see sparkrca.conf.example.
# RM and History Server URLs are auto-discovered from yarn-site.xml and
# spark-defaults.conf when not set.
#
# Produces $SPARKRCA_OUT_DIR/<app_id>/ (default /tmp/spark_rca) containing:
#   facts.json          machine-readable summary  <- LLM reads this first
#   app.json            RM app record (ages out of RM memory within hours)
#   shs_app.json        History Server app record (fallback for app.json)
#   stages.json         History Server stage list (stage retries live here)
#   allexecutors.json   History Server executor list, including dead executors
#   cluster_nodes.json  RM node list
#   am.log              ApplicationMaster container log
#   driver.log          driver log: am.log in cluster mode, --driver-log in client mode
#   failure_hosts.txt   hosts probed, derived from am.log + allexecutors.json
#   nodes/<host>.txt    per-host probe output
#   MANIFEST.txt        what was collected, what failed, what's missing
# A previous bundle for the same app is moved aside to <app_id>.prev.<timestamp>.
#
# SAFETY CONTRACT
#   - Node access is a single ssh call per host as $SPARKRCA_USER. The node's
#     authorized_keys forces it to run sparkrca_probe.sh; the only thing sent
#     is the application ID, which the probe validates. No remote shell.
#   - Hostnames are validated against a strict pattern before use.
#   - Read-only. No writes to HDFS, S3, or any remote filesystem.
#
# OPERATIONAL NOTES
#   - On busy clusters the RM forgets completed apps within hours (app volume
#     vs yarn.resourcemanager.max-completed-applications) — app.json is then
#     MISS and the History Server record is used. Run this soon after the job ends.
#   - `yarn logs -am` needs RM RPC / Timeline Server, which edge nodes often
#     can't reach. The AM container is fetched by ID instead.
#   - Host keys are never auto-accepted (older OpenSSH has no accept-new);
#     node host keys are pre-added to $SPARKRCA_KNOWN_HOSTS.

set -uo pipefail

# ---- cluster endpoints and access ----------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=sparkrca_lib.sh
source "$SCRIPT_DIR/sparkrca_lib.sh"
sparkrca_load_conf "$SCRIPT_DIR"
sparkrca_defaults
sparkrca_discover_endpoints
RM_URL="$SPARKRCA_RM_URL"
SHS_URL="$SPARKRCA_SHS_URL"
SSH_OPTS=(-i "$SPARKRCA_KEY" -o BatchMode=yes -o LogLevel=ERROR -o ConnectTimeout=10
          -o UserKnownHostsFile="$SPARKRCA_KNOWN_HOSTS" -o StrictHostKeyChecking=yes
          -o CheckHostIP=no)
# --------------------------------------------------------------------------

APP_ID=""; APP_OWNER=""; DRIVER_LOG=""; USE_SSH=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --owner)       APP_OWNER="$2"; shift 2 ;;
    --driver-log|--airflow-log) DRIVER_LOG="$2"; shift 2 ;;
    --no-ssh)      USE_SSH=0; shift ;;
    application_*) APP_ID="$1"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
[[ "$APP_ID" =~ ^application_[0-9]{1,20}_[0-9]{1,10}$ ]] \
  || { echo "usage: $0 application_<id> [--owner U] [--driver-log P] [--no-ssh]" >&2; exit 2; }

[[ -n "$RM_URL" || -n "$SHS_URL" ]] || {
  echo "neither ResourceManager nor History Server URL is known." >&2
  echo "set SPARKRCA_RM_URL / SPARKRCA_SHS_URL (env or sparkrca.conf), or HADOOP_CONF_DIR / SPARK_CONF_DIR for discovery." >&2
  exit 2; }

OUT="$SPARKRCA_OUT_DIR/$APP_ID"
# Never mix files from an earlier run into this bundle: curl -f leaves an old
# file in place when a fetch fails, which would silently pass off stale data.
[[ -d "$OUT" ]] && mv "$OUT" "$OUT.prev.$(date +%Y%m%d%H%M%S)"
mkdir -p "$OUT/nodes"
MAN="$OUT/MANIFEST.txt"
: > "$MAN"
note() { printf '%s\n' "$*" | tee -a "$MAN"; }

# json_get FILE PYEXPR — evaluate a python expression against parsed JSON `j`,
# print the result or nothing.
json_get() {
  python3 -c 'import json,re,sys
try:
    j = json.load(open(sys.argv[1])); v = eval(sys.argv[2])
    print(v if v is not None else "")
except Exception:
    pass' "$1" "$2" 2>/dev/null
}

note "collector run: $(date -Is)"
note "app: $APP_ID  owner: ${APP_OWNER:-(auto)}"
note "config: $SPARKRCA_CONF_USED"
note "rm:  ${RM_URL:-(unset)} [$SPARKRCA_RM_SOURCE]"
note "shs: ${SHS_URL:-(unset)} [$SPARKRCA_SHS_SOURCE]"

# ===========================================================================
# 1. ResourceManager: authoritative app record
# ===========================================================================
if curl -sfL --max-time 20 "$RM_URL/ws/v1/cluster/apps/$APP_ID" -o "$OUT/app.json"; then
  note "ok   app.json"
else
  rm -f "$OUT/app.json"
  note "MISS app.json — RM unreachable or app aged out of RM memory (no vcore/memory seconds)."
fi

curl -sfL --max-time 20 "$RM_URL/ws/v1/cluster/nodes" -o "$OUT/cluster_nodes.json" \
  && note "ok   cluster_nodes.json" || { rm -f "$OUT/cluster_nodes.json"; note "MISS cluster_nodes.json"; }

# ===========================================================================
# 2. History Server: app record, stage attempts, executors
#    This is where stage recomputation is measurable. Highest-value artifact.
#    Cluster-mode apps are keyed by attempt ("/1/") in the SHS API.
# ===========================================================================
shs_get() {   # shs_get <endpoint suffix or ""> <outfile>
  local ep="$1" out="$2" sfx
  for sfx in "" "/1"; do
    if curl -sfL --max-time 60 "$SHS_URL/api/v1/applications/$APP_ID$sfx${ep:+/$ep}" -o "$out"; then
      return 0
    fi
  done
  rm -f "$out"; return 1
}

curl -sfL --max-time 20 "$SHS_URL/api/v1/applications/$APP_ID" -o "$OUT/shs_app.json" \
  && note "ok   shs_app.json" || { rm -f "$OUT/shs_app.json"; note "MISS shs_app.json"; }

for ep in stages allexecutors; do
  if shs_get "$ep" "$OUT/$ep.json"; then
    note "ok   $ep.json"
  else
    note "MISS $ep.json — event log absent or still being written."
  fi
done

# ===========================================================================
# 3. ApplicationMaster container log
#    AM container ID comes from app.json, else is derived from the app ID
#    (attempt 1). Not `yarn logs -am`: that needs RM RPC / Timeline Server.
#    -containerId reads the aggregated log straight from HDFS.
# ===========================================================================
AM_CONTAINER="$(json_get "$OUT/app.json" 're.search(r"container_[0-9_e]+", j["app"].get("amContainerLogs") or "").group(0)')"
AM_SOURCE="app.json"
if [[ -z "$AM_CONTAINER" ]]; then
  AM_CONTAINER="container_${APP_ID#application_}_01_000001"
  AM_SOURCE="derived from app ID, assumes attempt 1"
fi

if [[ -z "$APP_OWNER" ]]; then
  APP_OWNER="$(json_get "$OUT/app.json" 'j["app"]["user"]')"
  [[ -z "$APP_OWNER" ]] && APP_OWNER="$(json_get "$OUT/shs_app.json" 'j["attempts"][-1]["sparkUser"]')"
  [[ -z "$APP_OWNER" ]] && APP_OWNER="$SPARKRCA_DEFAULT_OWNER"
fi
note "am container: $AM_CONTAINER ($AM_SOURCE)  owner: $APP_OWNER"

: > "$OUT/am.fetch.err"
yarn logs -applicationId "$APP_ID" -appOwner "$APP_OWNER" -containerId "$AM_CONTAINER" \
     > "$OUT/am.log" 2>>"$OUT/am.fetch.err"
# yarn may print errors on stdout; a real log always has a Container: header
grep -q '^Container: ' "$OUT/am.log" || { cat "$OUT/am.log" >> "$OUT/am.fetch.err"; : > "$OUT/am.log"; }
if [[ ! -s "$OUT/am.log" ]]; then
  # fallback: whole app from HDFS, keep only the AM container's sections
  yarn logs -applicationId "$APP_ID" -appOwner "$APP_OWNER" 2>>"$OUT/am.fetch.err" \
    | awk -v c="$AM_CONTAINER" '/^Container: /{keep=index($0,c)>0} keep' > "$OUT/am.log"
fi
if [[ -s "$OUT/am.log" ]]; then
  note "ok   am.log ($(wc -l < "$OUT/am.log") lines)"
else
  note "MISS am.log — aggregation may have expired. See am.fetch.err"
fi

# ===========================================================================
# 4. Driver log
#    Client mode (AM runs ExecutorLauncher) => DAGScheduler/TaskSetManager
#    logging is NOT in YARN. It lives wherever spark-submit ran (the
#    orchestrator's task log), so it has to be passed in with --driver-log.
#    No am.log => mode is unknown; don't guess.
# ===========================================================================
if [[ ! -s "$OUT/am.log" ]]; then DEPLOY_MODE="unknown"
elif grep -q 'ExecutorLauncher' "$OUT/am.log"; then DEPLOY_MODE="client"
else DEPLOY_MODE="cluster"; fi
note "deploy mode: $DEPLOY_MODE"

case "$DEPLOY_MODE" in
  client)
    if [[ -n "$DRIVER_LOG" && -r "$DRIVER_LOG" ]]; then
      cp "$DRIVER_LOG" "$OUT/driver.log"; note "ok   driver.log (from --driver-log)"
    else
      note "MISS driver.log — client mode and no --driver-log supplied."
      note "     Without it, stage retries can only come from stages.json."
    fi ;;
  cluster)
    cp "$OUT/am.log" "$OUT/driver.log"; note "ok   driver.log (cluster mode: same as am.log)" ;;
  *)
    if [[ -n "$DRIVER_LOG" && -r "$DRIVER_LOG" ]]; then
      cp "$DRIVER_LOG" "$OUT/driver.log"; note "ok   driver.log (from --driver-log)"
    else
      note "MISS driver.log — deploy mode unknown (no am.log)"
    fi ;;
esac

# ===========================================================================
# 5. Derive the host list. THIS IS THE ONLY RUNTIME-DERIVED INPUT, and it is
#    a set of hostnames, never commands. Validated before use.
#    Sources: AM log failure lines, plus History Server executors removed for
#    any reason other than the driver releasing them (dynamic allocation).
#    The latter still works after log aggregation has expired.
# ===========================================================================
{
  grep -E 'Container marked as failed|Container from a bad node|Completed container' "$OUT/am.log" 2>/dev/null \
    | grep -oE 'on host: [A-Za-z0-9.-]+' | cut -d' ' -f3
  python3 - "$OUT/allexecutors.json" <<'PY' 2>/dev/null
import json, sys
for e in json.load(open(sys.argv[1])):
    r = e.get("removeReason") or ""
    if e.get("id") != "driver" and r and "by driver" not in r:
        print(e.get("hostPort", "").rsplit(":", 1)[0])
PY
} | sed 's/\.*$//' | sort -u > "$OUT/failure_hosts.txt"   # log sentences end in '.'
note "failure hosts: $(wc -l < "$OUT/failure_hosts.txt")"

# ===========================================================================
# 6. Per-host probes — one call per host; the node decides what runs.
# ===========================================================================
node_probe() {
  local h="$1" f="$OUT/nodes/$h.txt"
  sparkrca_valid_host "$h" || { note "SKIP invalid hostname: $h"; return; }
  # -n: ssh must not read stdin, or it swallows the rest of the host list.
  if ssh -n "${SSH_OPTS[@]}" "$SPARKRCA_USER@$h" "$APP_ID" > "$f" 2>&1 \
     && grep -q '### host:' "$f"; then
    note "ok   nodes/$h.txt"
  else
    note "MISS nodes/$h.txt — probe failed (node provisioned? host key in $SPARKRCA_KNOWN_HOSTS?):"
    note "     $(grep -v 'not authorized' "$f" | head -1)"
  fi
}

if [[ "$USE_SSH" == "1" ]]; then
  while read -r h; do [[ -n "$h" ]] && node_probe "$h"; done < "$OUT/failure_hosts.txt"
else
  note "SKIP node probes (--no-ssh)"
fi

# ===========================================================================
# 7. facts.json — the compressed view the analyst reads first
# ===========================================================================
python3 - "$OUT" "$APP_ID" "$DEPLOY_MODE" "$AM_CONTAINER" "$SPARKRCA_COTENANT_SERVICES" <<'PY' > "$OUT/facts.json" 2>"$OUT/facts.err"
import json, os, re, sys
out, app_id, mode, am_container, cotenant = sys.argv[1:6]
# process names that mark a NodeManager host as running non-Hadoop workloads
names = [c.strip() for c in cotenant.split(",") if c.strip()]
COTENANT = re.compile(r"^(%s)$" % "|".join(map(re.escape, names)) if names else r"(?!)", re.M)

def load(name):
    try:
        with open(os.path.join(out, name)) as f: return json.load(f)
    except Exception: return None

def read(name):
    try:
        with open(os.path.join(out, name), errors="replace") as f: return f.read()
    except Exception: return ""

facts = {"app_id": app_id, "deploy_mode": mode, "am_container": am_container}

app = (load("app.json") or {}).get("app", {})
if app:
    dur = (app.get("finishedTime", 0) - app.get("startedTime", 0)) / 1000.0
    facts["app"] = {
        "source": "resourcemanager",
        "name": app.get("name"), "queue": app.get("queue"), "user": app.get("user"),
        "final_status": app.get("finalStatus"), "state": app.get("state"),
        "duration_s": round(dur, 1), "duration_h": round(dur / 3600, 2),
        "diagnostics": (app.get("diagnostics") or "")[:2000],
        "memory_seconds": app.get("memorySeconds"),
        "vcore_seconds": app.get("vcoreSeconds"),
        "preempted_vcore_seconds": app.get("preemptedVcoreSeconds"),
        "am_host": app.get("amHostHttpAddress"),
    }
    # allocation health: avg vcores actually held vs requested peak
    if dur > 0 and app.get("vcoreSeconds"):
        facts["app"]["avg_vcores_held"] = round(app["vcoreSeconds"] / dur, 1)

# RM forgot the app: fall back to the History Server record. No vcore or
# memory seconds there, so alloc_health_pct cannot be computed.
shs = load("shs_app.json") or {}
if not facts.get("app") and shs.get("attempts"):
    at = shs["attempts"][-1]
    facts["app"] = {
        "source": "history_server (RM record aged out: no vcore/memory seconds)",
        "name": shs.get("name"), "user": at.get("sparkUser"),
        "completed": at.get("completed"),
        "start": at.get("startTime"), "end": at.get("endTime"),
        "duration_s": round(at.get("duration", 0) / 1000.0, 1),
        "duration_h": round(at.get("duration", 0) / 3600000.0, 2),
    }

am = read("am.log")

# executor container requests
m = re.search(r"Will request (\d+) executor container\(s\).*?(\d+) core\(s\) and (\d+) MB", am)
if m:
    facts["requested"] = {"executors": int(m.group(1)),
                          "cores_each": int(m.group(2)),
                          "container_mb_each": int(m.group(3))}
    if facts.get("app", {}).get("avg_vcores_held"):
        full = int(m.group(1)) * int(m.group(2)) + 1
        facts["app"]["alloc_health_pct"] = round(
            100 * facts["app"]["avg_vcores_held"] / full, 1)

mo = re.search(r"memoryOverhead, amount: (\d+)", am)
mh = re.search(r"-Xmx(\d+)m", am)
if mo and mh:
    facts["memory"] = {"heap_mb": int(mh.group(1)), "overhead_mb": int(mo.group(1)),
                       "overhead_pct_of_heap": round(100*int(mo.group(1))/int(mh.group(1)), 1)}

facts["shuffle_service_enabled"] = ("spark.shuffle.service.enabled=true" in am)
facts["oom_kill_flag"] = ("OnOutOfMemoryError" in am)

# container failures, classified
EXIT = {
  "-100": "container released on a LOST node (NM stopped heartbeating)",
  "-104": "killed by YARN: exceeded physical memory limit",
  "-103": "killed by YARN: exceeded virtual memory limit",
  "137":  "SIGKILL from outside the JVM (kernel OOM killer, or NM shutdown)",
  "143":  "SIGTERM (JVM self-kill via OnOutOfMemoryError, or graceful stop)",
  "1":    "container-launch failure (JVM never started)",
}
fails = []
for line in am.splitlines():
    if "Container marked as failed" in line or "Container from a bad node" in line:
        ts = line.split(" INFO")[0].split(" WARN")[0].strip()
        host = re.search(r"on host: ([A-Za-z0-9.-]+)", line)
        code = re.search(r"Exit status: (-?\d+)", line)
        c = code.group(1) if code else "?"
        fails.append({"time": ts, "host": host.group(1).rstrip(".") if host else None,
                      "exit": c, "meaning": EXIT.get(c, "unclassified")})
facts["container_failures"] = fails
facts["container_failure_count"] = len(fails)
by_host = {}
for f in fails: by_host[f["host"]] = by_host.get(f["host"], 0) + 1
facts["failures_by_host"] = dict(sorted(by_host.items(), key=lambda kv: -kv[1]))

# executors lost per the History Server (survives log-aggregation expiry)
ex = load("allexecutors.json")
if isinstance(ex, list):
    facts["executors_removed"] = [
        {"id": e.get("id"), "host": e.get("hostPort", "").rsplit(":", 1)[0],
         "reason": (e.get("removeReason") or "")[:300]}
        for e in ex if e.get("id") != "driver" and e.get("removeReason")
        and "by driver" not in e.get("removeReason")]

# stage retries from the History Server
stages = load("stages.json")
if isinstance(stages, list):
    retried = [{"stage_id": s.get("stageId"), "attempt": s.get("attemptId"),
                "status": s.get("status"), "name": (s.get("name") or "")[:90],
                "num_tasks": s.get("numTasks"),
                "duration_ms": s.get("executorRunTime"),
                "shuffle_read": s.get("shuffleReadBytes"),
                "shuffle_write": s.get("shuffleWriteBytes"),
                "failure": (s.get("failureReason") or "")[:400]}
               for s in stages if (s.get("attemptId") or 0) > 0 or s.get("status") == "FAILED"]
    facts["retried_or_failed_stages"] = retried
    facts["retried_stage_count"] = len(retried)
    wasted = sum(s.get("executorRunTime", 0) for s in stages
                 if (s.get("attemptId") or 0) > 0)
    facts["recompute_executor_ms"] = wasted
else:
    facts["retried_or_failed_stages"] = None
    facts["_note_stages"] = "stages.json unavailable — recomputation cost NOT measured"

# node probe rollup
nd = {}
for fn in sorted(os.listdir(os.path.join(out, "nodes")) if os.path.isdir(os.path.join(out,"nodes")) else []):
    t = read(os.path.join("nodes", fn))
    host = fn[:-4]
    if "### host:" not in t:
        nd[host] = {"probe_ok": False}
        continue
    nd[host] = {
        "probe_ok": True,
        # probe prints "<count> <Mon> <day>" lines under the invocations header
        "oom_invocations_seen": sum(int(n) for n in re.findall(r"^\s*(\d+) \w{3}\s+\d+\s*$",
            (re.search(r"--- oom-killer invocations by day ---\n(.*?)\n---", t, re.S) or [None, ""])[1], re.M)),
        "oom_victims_seen": len(re.findall(r"Killed process|Out of memory: Kill process", t)),
        "nm_unhealthy": bool(re.search(r"local-dirs are bad|unhealthy", t)),
        # one process name per line in the probe's co-tenant section
        "cotenant_services": sorted(set(COTENANT.findall(
            (re.search(r"--- co-tenant services ---\n(.*?)(?:\n---|\Z)", t, re.S) or [None, ""])[1]))),
    }
facts["node_probes"] = nd

json.dump(facts, sys.stdout, indent=2)
PY

[[ -s "$OUT/facts.json" ]] && note "ok   facts.json" || note "MISS facts.json — see facts.err"

note ""
note "bundle: $OUT"
