#!/usr/bin/env bash
#
# setup_sparkrca_node.sh — provision the RCA probe account on one NodeManager.
#
# RUN AS ROOT on each NM host:
#   ./setup_sparkrca_node.sh --pubkey-file ./sparkrca.pub \
#                            --from <collector IPv4>[,<IPv4 or CIDR>...] \
#                            --probe ./sparkrca_probe.sh \
#                            [--probe-dest /usr/local/bin/sparkrca_probe.sh] \
#                            [--user svc-sparkrca] [--home /var/lib/sparkrca] \
#                            [--nm-log-glob '<hadoop log dir>/*nodemanager*.log*'] \
#                            [--syslog-glob '/var/log/messages*'] \
#                            [--yarn-site <hadoop conf dir>/yarn-site.xml]
#
# The three path flags are written to /etc/sparkrca/probe.conf for the probe.
# Omit them to use the probe's built-in defaults (common package layouts);
# the self-test at the end reports any path that matches nothing.
#
# Idempotent. Safe to re-run — intended to be driven by config management so
# newly-added nodes don't silently drop out of RCA coverage.
#
# Creates:
#   user   svc-sparkrca (system account, no password)
#   file   /usr/local/bin/sparkrca_probe.sh       root:root 0755
#   file   /etc/sudoers.d/sparkrca                root:root 0440
#   file   /etc/sparkrca/probe.conf               root:root 0644 (only if path flags given)
#   file   <home>/.ssh/authorized_keys            svc-sparkrca 0600
#
# The probe runs as root, so it must NOT live under the service account's home
# or any other directory a non-root user can write to: the owner of a directory
# can replace files in it, root-owned or not. The script refuses such a path.
#
# --from is written verbatim into authorized_keys from="...": one or more
# comma-separated IPv4 addresses or CIDR blocks (e.g. two collectors, or an
# edge-node subnet). Keep it as narrow as your collector topology allows.
#
# Access model: the SSH key is pinned to the collector source address(es) and forced to run the
# probe via sudo. The account cannot open a shell, forward ports, or choose
# what command runs. Its single degree of freedom is the application ID it
# asks the probe to grep for, which the probe validates.

set -euo pipefail

USER_NAME="svc-sparkrca"
USER_HOME="/var/lib/sparkrca"
PROBE_DEST="/usr/local/bin/sparkrca_probe.sh"
SUDOERS="/etc/sudoers.d/sparkrca"
PROBE_CONF="/etc/sparkrca/probe.conf"

PUBKEY_FILE=""; FROM_IP=""; PROBE_SRC=""
NM_LOG_GLOB=""; SYSLOG_GLOB=""; YARN_SITE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --pubkey-file) PUBKEY_FILE="$2"; shift 2 ;;
    --from)        FROM_IP="$2";     shift 2 ;;
    --probe)       PROBE_SRC="$2";   shift 2 ;;
    --probe-dest)  PROBE_DEST="$2";  shift 2 ;;
    --user)        USER_NAME="$2";   shift 2 ;;
    --home)        USER_HOME="$2";   shift 2 ;;
    --nm-log-glob) NM_LOG_GLOB="$2"; shift 2 ;;
    --syslog-glob) SYSLOG_GLOB="$2"; shift 2 ;;
    --yarn-site)   YARN_SITE="$2";   shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ $EUID -eq 0 ]] || { echo "must run as root" >&2; exit 1; }
[[ -r "$PUBKEY_FILE" ]] || { echo "--pubkey-file required and must be readable" >&2; exit 2; }
[[ -r "$PROBE_SRC"   ]] || { echo "--probe required and must be readable" >&2; exit 2; }
IPV4='[0-9]{1,3}(\.[0-9]{1,3}){3}(/([0-9]|[12][0-9]|3[0-2]))?'
[[ "$FROM_IP" =~ ^$IPV4(,$IPV4)*$ ]] \
  || { echo "--from must be IPv4 addresses or CIDR blocks, comma-separated" >&2; exit 2; }
[[ "$PROBE_DEST" == /* ]] || { echo "--probe-dest must be an absolute path" >&2; exit 2; }
[[ "$USER_NAME" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || { echo "--user is not a valid account name" >&2; exit 2; }
[[ "$USER_HOME" == /* ]] || { echo "--home must be an absolute path" >&2; exit 2; }
for v in "$NM_LOG_GLOB" "$SYSLOG_GLOB" "$YARN_SITE"; do
  [[ -z "$v" || "$v" =~ ^/[A-Za-z0-9_./*?\ -]+$ ]] \
    || { echo "path flags must be absolute paths/globs (no quotes or newlines): $v" >&2; exit 2; }
done

# Every directory from the probe up to / must be root-owned and not group- or
# world-writable, otherwise a non-root user can swap the file sudo runs as root.
d="$(dirname "$PROBE_DEST")"
while :; do
  [[ -d "$d" ]] || { echo "probe directory $d does not exist" >&2; exit 2; }
  read -r owner mode < <(stat -c '%U %a' "$d")
  if [[ "$owner" != "root" ]] || (( 8#$mode & 8#022 )); then
    echo "refusing: $d is owned by $owner, mode $mode." >&2
    echo "          Every directory above the probe must be root-owned and not group/world-writable." >&2
    exit 2
  fi
  [[ "$d" == "/" ]] && break
  d="$(dirname "$d")"
done

PUBKEY="$(tr -d '\r\n' < "$PUBKEY_FILE")"
[[ "$PUBKEY" =~ ^(ssh-ed25519|ssh-rsa|ecdsa-sha2-)[A-Za-z0-9+/=[:space:]-]+$ ]] \
  || { echo "pubkey does not look like an OpenSSH public key" >&2; exit 2; }

echo "==> host: $(hostname -f)"

# --------------------------------------------------------------------------
# 1. service account
#    Shell must be a real shell: sshd runs the forced command via it.
# --------------------------------------------------------------------------
if id "$USER_NAME" &>/dev/null; then
  echo "    user $USER_NAME exists"
else
  useradd --system --create-home --home-dir "$USER_HOME" \
          --shell /bin/bash --comment "Spark RCA evidence probe" "$USER_NAME"
  echo "    created $USER_NAME"
fi
# No password, and no way to set one. Key-only.
passwd -l "$USER_NAME" >/dev/null
# An existing account keeps its home; --home only applies on creation.
USER_HOME="$(getent passwd "$USER_NAME" | cut -d: -f6)"
LEGACY_PROBE="$USER_HOME/sparkrca_probe.sh"
chmod 0750 "$USER_HOME"
chown "$USER_NAME:$USER_NAME" "$USER_HOME"

# --------------------------------------------------------------------------
# 2. probe script — root-owned, in a root-only directory
# --------------------------------------------------------------------------
install -o root -g root -m 0755 "$PROBE_SRC" "$PROBE_DEST"
echo "    installed $PROBE_DEST"

# Earlier versions put the probe in the service account's home — remove it.
if [[ "$LEGACY_PROBE" != "$PROBE_DEST" && -e "$LEGACY_PROBE" ]]; then
  rm -f "$LEGACY_PROBE"
  echo "    removed legacy $LEGACY_PROBE"
fi

if grep -qE '^\s*sudo ' "$PROBE_DEST"; then
  echo "    WARNING: probe still contains internal 'sudo' calls."
  echo "             It now runs as root via the sudoers entry below."
  echo "             Fix with: sed -i 's/^sudo -n //' $PROBE_DEST"
fi

# Site paths for the probe. Root-owned and not writable by others, or the
# probe ignores it.
if [[ -n "$NM_LOG_GLOB$SYSLOG_GLOB$YARN_SITE" ]]; then
  install -d -o root -g root -m 0755 "$(dirname "$PROBE_CONF")"
  TMP_CONF="$(mktemp)"
  {
    echo "# Managed by setup_sparkrca_node.sh — do not edit by hand."
    [[ -n "$NM_LOG_GLOB" ]] && echo "NM_LOG_GLOB=$NM_LOG_GLOB"
    [[ -n "$SYSLOG_GLOB" ]] && echo "SYSLOG_GLOB=$SYSLOG_GLOB"
    [[ -n "$YARN_SITE"   ]] && echo "YARN_SITE=$YARN_SITE"
  } > "$TMP_CONF"
  install -o root -g root -m 0644 "$TMP_CONF" "$PROBE_CONF"
  rm -f "$TMP_CONF"
  echo "    installed $PROBE_CONF"
fi

# --------------------------------------------------------------------------
# 3. sudoers — exactly one command, no arguments, no wildcards.
#    env_keep is required: sudo resets the environment. The probe reads the
#    requested application ID from SSH_ORIGINAL_COMMAND and the caller IP for
#    its audit log from SSH_CLIENT.
# --------------------------------------------------------------------------
TMP_SUDO="$(mktemp)"
cat > "$TMP_SUDO" <<EOF
# Managed by setup_sparkrca_node.sh — do not edit by hand.
# Spark-on-YARN RCA evidence probe. Read-only; writes nothing.
Cmnd_Alias SPARKRCA_PROBE = $PROBE_DEST
Defaults!SPARKRCA_PROBE env_keep += "SSH_ORIGINAL_COMMAND SSH_CLIENT"
Defaults!SPARKRCA_PROBE !requiretty
$USER_NAME ALL=(root) NOPASSWD: SPARKRCA_PROBE
EOF

if visudo -cqf "$TMP_SUDO"; then
  install -o root -g root -m 0440 "$TMP_SUDO" "$SUDOERS"
  rm -f "$TMP_SUDO"
  echo "    installed $SUDOERS"
else
  rm -f "$TMP_SUDO"
  echo "!! sudoers syntax check FAILED — nothing installed" >&2
  exit 1
fi

# --------------------------------------------------------------------------
# 4. authorized_keys — ForceCommand + source pin + every forward disabled
# --------------------------------------------------------------------------
SSH_DIR="$USER_HOME/.ssh"
AK="$SSH_DIR/authorized_keys"
install -d -o "$USER_NAME" -g "$USER_NAME" -m 0700 "$SSH_DIR"

# 'restrict' (OpenSSH 7.2+) disables port/agent/X11 forwarding, PTY and user-rc
# and is future-proofed against new forwarding types. Fall back if older.
SSH_VER="$(ssh -V 2>&1 | sed -nE 's/^OpenSSH_([0-9]+)\.([0-9]+).*/\1 \2/p')"
read -r SSH_MAJ SSH_MIN <<<"${SSH_VER:-0 0}"
if (( SSH_MAJ > 7 || (SSH_MAJ == 7 && SSH_MIN >= 2) )); then
  OPTS="command=\"sudo -n $PROBE_DEST\",from=\"$FROM_IP\",restrict"
else
  OPTS="command=\"sudo -n $PROBE_DEST\",from=\"$FROM_IP\",no-pty,no-agent-forwarding,no-port-forwarding,no-X11-forwarding,no-user-rc"
fi

LINE="$OPTS $PUBKEY sparkrca-collector"
TMP_AK="$(mktemp)"
# Drop any previous sparkrca line, keep anything else, append current.
[[ -f "$AK" ]] && grep -v 'sparkrca-collector' "$AK" > "$TMP_AK" || true
printf '%s\n' "$LINE" >> "$TMP_AK"
install -o "$USER_NAME" -g "$USER_NAME" -m 0600 "$TMP_AK" "$AK"
rm -f "$TMP_AK"
echo "    installed $AK (pinned to $FROM_IP)"

# --------------------------------------------------------------------------
# 5. sanity
# --------------------------------------------------------------------------
echo "==> checks"
sudo -n -u "$USER_NAME" sudo -n -l "$PROBE_DEST" >/dev/null 2>&1 \
  && echo "    sudo rule resolves" \
  || echo "    WARNING: sudo rule did not resolve — inspect $SUDOERS"

# End-to-end as the service account, through the same sudo path sshd uses.
# Catches env_keep / set -u problems here instead of from the collector.
SELFTEST="$(sudo -n -u "$USER_NAME" env SSH_ORIGINAL_COMMAND=application_1_1 \
            SSH_CLIENT="127.0.0.1 0 22" sudo -n "$PROBE_DEST" 2>&1 || true)"
if grep -q '### host:' <<<"$SELFTEST" && ! grep -qiE 'unbound variable|sudo:' <<<"$SELFTEST"; then
  echo "    probe self-test ok ($(wc -l <<<"$SELFTEST") lines)"
else
  echo "    WARNING: probe self-test failed:"
  head -5 <<<"$SELFTEST" | sed 's/^/      /'
fi

# The probe reports which of its configured paths matched anything.
sed -n '/^--- probe paths ---$/,/^$/{/^---/d;/^$/d;s/^/    /;p}' <<<"$SELFTEST"
grep -q 'MISSING' <<<"$SELFTEST" \
  && echo "    WARNING: probe paths missing — re-run with --nm-log-glob / --syslog-glob / --yarn-site"

echo "==> done on $(hostname -f)"
echo "    next, on the collector: add this host's key to ~/.ssh/known_hosts_sparkrca"
echo "    (ssh-keyscan + fingerprint check), then run verify_sparkrca_access.sh"
