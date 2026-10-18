# sparkrca — root-cause analysis for Spark-on-YARN

Collects a deterministic evidence bundle for one YARN application (RM and
History Server REST records, AM log, per-node OOM / co-tenancy probes) and
compresses it into a `facts.json` that a human or an LLM can reason over.
`SKILL.md` is the analysis guide.

**Collection is deterministic code; analysis is the LLM. No LLM-authored shell
ever touches a cluster host.** The only remote access is a forced-command SSH
key that can ask one root-owned probe script for one validated application ID.

See [docs/CASE_STUDY.md](docs/CASE_STUDY.md) for the incident this came from.

## Files

| File | Runs on | Role |
|---|---|---|
| `collect_spark_evidence.sh` | collector (edge node) | builds `$SPARKRCA_OUT_DIR/<app_id>/` |
| `verify_sparkrca_access.sh` | collector | positive and negative access tests per node |
| `sparkrca_lib.sh` | collector | shared config loading and endpoint discovery |
| `sparkrca.conf.example` | collector | every site setting, documented |
| `setup_sparkrca_node.sh` | each NodeManager, as root | provisions account, probe, sudoers, `authorized_keys` |
| `sparkrca_probe.sh` | each NodeManager | the only thing the collector can run on a node |
| `SKILL.md` | analysis | how to read a bundle |

## Configuration

Nothing about your cluster is hard-coded. Every collector setting is a
`SPARKRCA_*` key, resolved in this order:

1. environment variable
2. config file: `$SPARKRCA_CONF`, else `sparkrca.conf` next to the scripts,
   else `~/.config/sparkrca/sparkrca.conf`
3. auto-discovery — RM URL from `$HADOOP_CONF_DIR/yarn-site.xml` (HA-aware,
   picks the ACTIVE RM), History Server URL from
   `$SPARK_CONF_DIR/spark-defaults.conf`
4. built-in default

| Key | Default |
|---|---|
| `SPARKRCA_RM_URL` | discovered |
| `SPARKRCA_SHS_URL` | discovered |
| `SPARKRCA_USER` | `svc-sparkrca` |
| `SPARKRCA_KEY` | `~/.ssh/sparkrca` |
| `SPARKRCA_KNOWN_HOSTS` | `~/.ssh/known_hosts_sparkrca` |
| `SPARKRCA_OUT_DIR` | `/tmp/spark_rca` |
| `SPARKRCA_DEFAULT_OWNER` | user running the collector |
| `SPARKRCA_COTENANT_SERVICES` | `postgres,redis-server,httpd,nginx,dockerd,containerd,mysqld,mariadbd,mongod` |

Node-side paths (NodeManager logs, syslog, `yarn-site.xml`) default to common
package layouts and are overridden per node with `setup_sparkrca_node.sh
--nm-log-glob / --syslog-glob / --yarn-site`, which writes
`/etc/sparkrca/probe.conf`.

`sparkrca.conf` is git-ignored; keep your site's copy out of the repo.

## Setup

**1. Collector key** (on the edge node that will run collections):

```bash
ssh-keygen -t ed25519 -N '' -f ~/.ssh/sparkrca -C sparkrca-collector
cp sparkrca.conf.example sparkrca.conf   # edit only what discovery can't find
```

**2. Each NodeManager**, as root:

```bash
./setup_sparkrca_node.sh --pubkey-file sparkrca.pub \
    --from <collector IP>[,<IP or CIDR>...] \
    --probe ./sparkrca_probe.sh
```

The self-test at the end reports any probe path that matched nothing — re-run
with the path flags if so. Drive this from config management so new nodes
don't silently drop out of coverage.

**3. Enrol host keys** on the collector. Host keys are never auto-accepted:
compare `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub` on the node with
`ssh-keyscan -t ed25519 <node> | ssh-keygen -lf -`, then append the keyscan
output to `~/.ssh/known_hosts_sparkrca`.

**4. Verify** — all positive and negative tests must pass before rollout:

```bash
./verify_sparkrca_access.sh application_<ts>_<seq> <node>...
```

## Use

```bash
./collect_spark_evidence.sh application_<ts>_<seq> \
    [--owner USER] [--driver-log /path/to/driver.log] [--no-ssh]
```

- Run it **soon after the job ends**: busy RMs forget completed apps within
  hours, and with them `vcoreSeconds` / allocation health. Ideally trigger it
  from your orchestrator's failure or SLA-miss callback.
- In **client mode** the driver log is not in YARN. Pass the log of whatever
  ran `spark-submit` (Airflow task log, Oozie launcher, cron output) with
  `--driver-log`.
- `--no-ssh` skips node probes — useful before any node is provisioned.

Then hand the bundle to an analyst or LLM along with `SKILL.md`.

## Requirements

Collector: bash 4.2+, `curl`, `python3`, OpenSSH client, and the `yarn` CLI
with HDFS read access to aggregated logs. Nodes: bash, `sudo`, OpenSSH server
(7.2+ for `restrict`; older falls back to explicit `no-*` options).

## Security notes

- The probe runs as root via sudo. Setup refuses any probe path whose
  directory chain is not root-owned and non-group/world-writable.
- The probe reads system logs (`/var/log/messages` etc.), which on
  co-tenanted hosts contain lines from non-Hadoop services. Review that with
  your security team before rollout.
- Refused requests are logged with `logger -t sparkrca_probe` and the caller IP.

## Known limitations

- `facts.json` regexes are tuned to Spark 3.3 AM log strings
  (`Will request N executor container(s)`, `memoryOverhead, amount:`).
- No Kerberos/SPNEGO or TLS client-cert support for the REST calls yet.
- AM container derivation assumes attempt 1 when the RM record is gone.
