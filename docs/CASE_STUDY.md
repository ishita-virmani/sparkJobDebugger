# Case study: the incident that motivated this tool

Anonymized. Hostnames, addresses, application IDs and job names are
placeholders; the numbers and the reasoning are real.

## The job

A scheduled Spark 3.3 batch job on Hadoop 3.3 (YARN), client mode, 9 executors
× 4 cores, 8 GB heap. Normally ~1.5 h. This run hit 3 h.

## What actually happened

The job **never finished**. The RM reported `finalStatus=SUCCEEDED`, but:

- It ran in **client mode**: the AM container was `ExecutorLauncher` and the
  driver lived on the submitting host, outside YARN. In client mode the AM
  unregisters with SUCCEEDED whenever the driver socket closes, regardless of
  outcome.
- `finishTime - startTime` = **2 h 59 m 51 s**. The orchestrator's timeout
  killed the driver at almost exactly 3 hours.

## Causal chain

1. **Node-level memory exhaustion and node loss.** Nine executor containers
   lost across four hosts:

   | Time | Host | Exit | Meaning |
   |---|---|---|---|
   | +0:00 | node-a | 1 | JVM failed at launch: `os::commit_memory(1.9GB) failed, errno=12` |
   | +0:24 | node-a | 137 | SIGKILL from outside the JVM |
   | +1:20 | node-b | -100 | node LOST — took 2 executors |
   | +1:50 | node-c | -100 | node LOST — 1 executor |
   | +2:09 | node-a | 137 | SIGKILL again — third failure on this host |
   | +2:29 | node-d | -100 | node LOST — took 2 executors |

2. **node-a is co-tenanted and over-advertised.** Its kernel log showed ~19
   OOM-killer invocations across the day, roughly every 30 min, before and
   after this job. Invokers included `httpd`, `postgres`, `redis-server`,
   `dockerd` and several `python` processes. It was not a dedicated YARN
   worker: `yarn.nodemanager.resource.memory-mb` advertised memory those
   services already held, and YARN cannot see non-container usage.

   Exit 137 = SIGKILL = external. The job set `-XX:OnOutOfMemoryError='kill %p'`,
   which sends SIGTERM → 143. And there was no `-104` / "beyond physical
   memory limits" text, so YARN's memory monitor didn't do it either. The
   kernel OOM killer is the remaining explanation.

3. **No external shuffle service.** `ExecutorDeadException ... executor(Id: 3)
   ... is dead` confirms shuffle blocks were served by executor JVMs. Every
   executor death destroyed its completed map output → `FetchFailed` → the
   DAGScheduler re-ran the whole parent map stage (13,000+ map tasks).

4. **Container starvation.** 17 min to go from 3 to 9 executors at startup
   (a container stayed RESERVED with `available=<memory:5719, vCores:0>`).
   Replacements took 11 and 22 min; the final 2 requested were never granted.

## The quantitative point

`vcoreSeconds=360866` over 10,791 s = **33.4 average vcores** against 37
requested (9 × 4 + AM) = **~90 % allocation health**. A 10 % capacity loss
does not double a runtime. The remainder is stage recomputation — which could
only be inferred, not measured, because in client mode the DAGScheduler logs
were in the orchestrator's task log, which was never obtained.

**That gap is the main thing this tool fixes**, via the History Server
`/stages` API.

## Recommendations that came out of it

Infra: enable the external shuffle service; drain or fix the co-tenanted node;
recompute `resource.memory-mb` on co-tenanted nodes; consider cgroup memory
enforcement (`LinuxContainerExecutor` + `CGroupsMemoryResourceHandler`) so
kills come from YARN with a readable diagnostic instead of the kernel picking
a victim.

Job: raise `spark.executor.memoryOverhead` (600 MB on an 8 GB heap = 7 %;
Spark's own default would be 819 MB); move to cluster mode; enable
`spark.excludeOnFailure.enabled`; alert on driver exit code, not YARN app
state.

## Operational lessons baked into the collector

- **The RM forgets completed apps within hours** on a busy cluster (app volume
  vs `yarn.resourcemanager.max-completed-applications`). `vcoreSeconds` and
  `alloc_health_pct` are only available if collection runs soon after the job
  ends — trigger it from the orchestrator's failure/SLA callback.
- **`yarn logs -am` often fails from edge nodes** (needs RM RPC and a Timeline
  Server). `yarn logs -applicationId X -containerId <AM>` reads HDFS directly.
- **The process that invokes the OOM killer is not the victim.** Only
  `Killed process` / `Out of memory: Kill process` lines name what died.
- **`finalStatus=SUCCEEDED` means nothing in client mode.**
- **Keep findings and hypotheses separate.** "Three nodes went LOST" is a
  finding. "Which forced stage retries" is a hypothesis unless `stages.json`
  backs it.
