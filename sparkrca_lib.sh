# shellcheck shell=bash
#
# sparkrca_lib.sh — shared settings for the collector-side scripts. Sourced, not run.
#
# Every site-specific value is a SPARKRCA_* setting, resolved in this order:
#   1. environment variable
#   2. config file: $SPARKRCA_CONF, else sparkrca.conf next to the scripts,
#      else ~/.config/sparkrca/sparkrca.conf  (see sparkrca.conf.example)
#   3. auto-discovery (RM / History Server URLs only) from the local Hadoop
#      and Spark client config
#   4. built-in default

# sparkrca_load_conf <script dir>
# Reads KEY=VALUE lines. Only SPARKRCA_* keys are accepted; values are taken
# literally (never evaluated), apart from a leading ~ expanding to $HOME.
sparkrca_load_conf() {
  local f line k v
  for f in "${SPARKRCA_CONF:-}" "$1/sparkrca.conf" "$HOME/.config/sparkrca/sparkrca.conf"; do
    [[ -n "$f" && -r "$f" ]] && break
    f=""
  done
  SPARKRCA_CONF_USED="${f:-none}"
  [[ -n "$f" ]] || return 0

  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*(#|$) ]] && continue
    k="${line%%=*}"; v="${line#*=}"
    k="${k//[[:space:]]/}"
    [[ "$k" =~ ^SPARKRCA_[A-Z0-9_]+$ ]] || continue
    [[ -n "${!k+x}" ]] && continue            # environment wins
    v="${v#"${v%%[![:space:]]*}"}"; v="${v%"${v##*[![:space:]]}"}"
    [[ "$v" =~ ^\"(.*)\"$ || "$v" =~ ^\'(.*)\'$ ]] && v="${BASH_REMATCH[1]}"
    v="${v/#\~/$HOME}"
    printf -v "$k" '%s' "$v"
  done < "$f"
}

# Candidate RM web URLs from yarn-site.xml, HA-aware, one per line.
_sparkrca_rm_candidates() {
  local conf="${HADOOP_CONF_DIR:-${HADOOP_HOME:+$HADOOP_HOME/etc/hadoop}}"
  python3 - "${conf:-/etc/hadoop/conf}/yarn-site.xml" <<'PY' 2>/dev/null
import re, sys, xml.etree.ElementTree as ET
try:
    root = ET.parse(sys.argv[1]).getroot()
except Exception:
    sys.exit(0)
p = {e.findtext("name"): (e.findtext("value") or "").strip() for e in root.iter("property")}
sub = lambda s: re.sub(r"\$\{([^}]+)\}", lambda m: p.get(m.group(1), ""), s or "")
https = p.get("yarn.http.policy") == "HTTPS_ONLY"
key = "yarn.resourcemanager.webapp.https.address" if https else "yarn.resourcemanager.webapp.address"
port = "8090" if https else "8088"
ids = [""]
if p.get("yarn.resourcemanager.ha.enabled") == "true":
    ids = ["." + i.strip() for i in p.get("yarn.resourcemanager.ha.rm-ids", "").split(",") if i.strip()]
for i in ids:
    addr = sub(p.get(key + i))
    if not addr or addr.startswith(":"):
        host = sub(p.get("yarn.resourcemanager.hostname" + i))
        addr = host + ":" + port if host else ""
    if addr and not addr.startswith("0.0.0.0"):
        print(("https://" if https else "http://") + addr)
PY
}

# History Server URL from spark-defaults.conf.
_sparkrca_shs_candidate() {
  local conf="${SPARK_CONF_DIR:-${SPARK_HOME:+$SPARK_HOME/conf}}" a
  a="$(awk '$1 == "spark.yarn.historyServer.address" {print $2; exit}' \
        "${conf:-/etc/spark/conf}/spark-defaults.conf" 2>/dev/null)"
  [[ -z "$a" ]] && return
  [[ "$a" == *://* ]] || a="http://$a"
  printf '%s\n' "$a"
}

# Fill SPARKRCA_RM_URL / SPARKRCA_SHS_URL if neither env nor config set them.
# With RM HA, prefer the RM that reports itself ACTIVE.
sparkrca_discover_endpoints() {
  local c first=""
  if [[ -z "${SPARKRCA_RM_URL:-}" ]]; then
    while read -r c; do
      [[ -z "$c" ]] && continue
      if curl -sf --max-time 5 "$c/ws/v1/cluster/info" | grep -q '"haState" *: *"ACTIVE"'; then
        SPARKRCA_RM_URL="$c"; break
      fi
      [[ -z "$first" ]] && first="$c"
    done < <(_sparkrca_rm_candidates)
    SPARKRCA_RM_URL="${SPARKRCA_RM_URL:-$first}"
    [[ -n "$SPARKRCA_RM_URL" ]] && SPARKRCA_RM_SOURCE="discovered" || SPARKRCA_RM_SOURCE="unset"
  else
    SPARKRCA_RM_SOURCE="configured"
  fi
  if [[ -z "${SPARKRCA_SHS_URL:-}" ]]; then
    SPARKRCA_SHS_URL="$(_sparkrca_shs_candidate)"
    [[ -n "$SPARKRCA_SHS_URL" ]] && SPARKRCA_SHS_SOURCE="discovered" || SPARKRCA_SHS_SOURCE="unset"
  else
    SPARKRCA_SHS_SOURCE="configured"
  fi
  SPARKRCA_RM_URL="${SPARKRCA_RM_URL%/}"; SPARKRCA_SHS_URL="${SPARKRCA_SHS_URL%/}"
}

# Hostnames come from logs and REST responses and end up as ssh arguments:
# RFC 1123 labels only, and never a leading '-' (ssh option injection).
sparkrca_valid_host() {
  [[ ${#1} -le 253 && "$1" =~ ^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?([.][A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$ ]]
}

sparkrca_defaults() {
  SPARKRCA_USER="${SPARKRCA_USER:-svc-sparkrca}"
  SPARKRCA_KEY="${SPARKRCA_KEY:-$HOME/.ssh/sparkrca}"
  SPARKRCA_KNOWN_HOSTS="${SPARKRCA_KNOWN_HOSTS:-$HOME/.ssh/known_hosts_sparkrca}"
  SPARKRCA_OUT_DIR="${SPARKRCA_OUT_DIR:-/tmp/spark_rca}"
  SPARKRCA_DEFAULT_OWNER="${SPARKRCA_DEFAULT_OWNER:-$(id -un)}"
  SPARKRCA_COTENANT_SERVICES="${SPARKRCA_COTENANT_SERVICES:-postgres,redis-server,httpd,nginx,dockerd,containerd,mysqld,mariadbd,mongod}"
}
