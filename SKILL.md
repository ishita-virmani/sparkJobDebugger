---
name: spark-yarn-rca
description: Root-cause a slow, hung, or timed-out Spark-on-YARN application from a collected evidence bundle. Use when given a YARN application ID and asked why a job ran long, was killed, or failed — or when handed a spark_rca/<app_id>/ evidence bundle. Covers stage recomputation, executor loss classification, node-level OOM and co-tenancy, and container allocation starvation. Do NOT use for Spark query-plan or SQL performance tuning on a healthy run.
---

# Spark-on-YARN RCA

## Inputs

An evidence bundle at `$SPARKRCA_OUT_DIR/<app_id>/` (default
`/tmp/spark_rca/<app_id>/`) produced by `collect_spark_evidence.sh`.
Read `facts.json` first — it is the compressed view. Only open the raw logs
when `facts.json` leaves a question open.

If no bundle exists, run the collector. Never hand-write SSH commands against
cluster hosts; the collector's allowlist is the only sanctioned remote access.

## Read order

1. `facts.json` — everything below is keyed off this
2. `MANIFEST.txt` — what is MISSING changes what you may conclude
3. `stages.json` — only if quantifying recomputation
4. `nodes/*.txt` — only for hosts that `facts.json` flags

## The seven checks, in order

### 1. Did the job actually finish?

`app.final_status` is unreliable in **client mode**. The AM is just
`ExecutorLauncher`; when the driver socket closes it unregisters with
SUCCEEDED regardless of what happened. If `deploy_mode == "client"` and
`final_status == "SUCCEEDED"` but the user reports a timeout, the app was
killed externally.

Check `duration_s` against round numbers. A duration within seconds of
3600/7200/10800 is an orchestrator timeout, not a Spark failure.

### 2. Classify every container failure

`container_failures[].meaning` is pre-computed. What each implies:

| Exit | Cause | Whose problem |
|---|---|---|
| `-100` | NM stopped heartbeating; RM declared node lost | infra |
| `-104` | YARN memory monitor: container exceeded physical limit | job config |
| `137` | SIGKILL from outside the JVM — kernel OOM killer, or NM shutdown | node |
| `143` | SIGTERM — often the JVM self-killing via `OnOutOfMemoryError` | job heap |
| `1` | JVM never started (check for `os::commit_memory ... errno=12`) | node memory |

**137 vs 143 is the load-bearing distinction.** `-XX:OnOutOfMemoryError='kill %p'`
sends SIGTERM → 143. A 137 means something external killed it, which moves the
investigation from the job to the node.

`-104` would name a memory limit in the diagnostic text. Its absence alongside
a 137 rules out the YARN memory monitor.

### 3. Is it one bad node or the cluster?

Read `failures_by_host`. Repeat offenders get the node-probe treatment. Then
in `node_probes[host]`:

- `oom_invocations_seen` high and spread across the day → the node is
  chronically over-committed, not perturbed by this job
- `cotenant_services` non-empty → the box runs non-Hadoop workloads and
  `yarn.nodemanager.resource.memory-mb` is advertising memory that those
  services already hold. The list matched is `SPARKRCA_COTENANT_SERVICES`;
  an empty result only means none of *those* names were running. YARN cannot see non-container memory.
- `nm_unhealthy` → disk or local-dirs failure

**The process that invokes the OOM killer is not the victim.** Never name a
killed process from an "invoked oom-killer" line; use the `Killed process`
lines. If `oom_victims_seen == 0`, say the victim is unconfirmed.

Nodes that went LOST (-100) and nodes that OOM-kill are often the same
failure: a memory-starved NM stalls, misses heartbeats, gets declared lost.
Check whether the -100 hosts also show OOM activity before treating them as
separate incidents.

### 4. Quantify the capacity loss

`app.alloc_health_pct` = average vcores held ÷ peak requested. This is the
*allocation* shortfall only.

If alloc_health is ~90% but the job took 2x as long, allocation loss does
**not** explain it. Say so explicitly, and go to check 5. Resist attributing
a 2x overrun to a 10% capacity loss.

Also read the gaps between a `Will request N executor container(s)` line and
the matching `Launching container` lines in `am.log`. Long gaps mean the queue
was full; corroborate with `RESERVED`/`unreserved` transitions in the RM log
and `available=<...vCores:0>` on the reserving node.

### 5. Quantify recomputation — usually the real answer

`retried_stage_count` and `recompute_executor_ms` from `stages.json`.

The mechanism: with `shuffle_service_enabled == false`, a dead executor takes
its completed shuffle output with it. The DAGScheduler must re-run the entire
parent map stage. Losing 2 of 9 executors wipes ~22% of map output for every
shuffle those executors served. This is what turns a 10% capacity loss into a
100% runtime increase.

If `stages.json` is missing, say the recomputation cost is **unmeasured** and
present the mechanism as the leading hypothesis, not as a finding.

### 6. Config smells worth flagging (secondary, never the headline)

- `memory.overhead_pct_of_heap` < 10 — Spark's own default is
  `max(384, 0.1 × heap)`. Sort-heavy shuffle jobs with Netty direct buffers
  want more.
- `deploy_mode == "client"` — driver unsupervised, logs not aggregated,
  edge-node hiccup kills the app
- `shuffle_service_enabled == false` — see check 5
- no exclusion configured while one host fails repeatedly
  (`spark.excludeOnFailure.enabled`)

### 7. State what is missing

From `MANIFEST.txt`. Common gaps and what they cost:

- `driver.log` MISS in client mode → no `FetchFailed` evidence; lean on `stages.json`,
  and ask for the orchestrator's task log to re-run with `--driver-log`
- `stages.json` MISS → recomputation unquantified
- `am.log` MISS → log aggregation expired; nothing else can substitute
- node probes skipped → node-level cause unconfirmed

## Output shape

Lead with what happened and whether the job finished. Then the causal chain,
shortest path first. Then a timeline table only if there are ≥3 events. Then
recommendations split into **infra** (what the platform team changes) and
**job** (what the owner changes), ordered by impact. Close with what evidence
is still missing and the exact command to get it.

Keep findings and hypotheses visibly separate. "Three nodes went LOST" is a
finding. "Which likely forced stage retries" is a hypothesis unless
`stages.json` backs it. Label the second kind.
