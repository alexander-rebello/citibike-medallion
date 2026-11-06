# 10-minute presentation and live demo plan

Principle: one idea per slide. Story: raw files -> trustworthy data -> an answer. Silver gets the time.

| # | Min | Slide | Message / evidence |
|---|---:|---|---|
| 1 | 0:45 | The question | Pandemic and e-bikes: did usage change? Teaser chart: e-bike share 14 % -> 66 % -> 73 %. |
| 2 | 1:15 | The data is not one dataset | 334 M rows, 2 schemas, 4 headers, 8 timestamp formats; timeline figure. Our wrong assumption and what it taught us. |
| 3 | 1:00 | Architecture | Medallion diagram, dev/prod, 2-node cluster, git -> CI lane. |
| 4 | 0:45 | Bronze | Store unchanged, reconcile every month, an unknown header stops the run. |
| 5 | 3:00 | **Silver** | (a) rules come from measurements, (b) row accounting: 99.91 % accepted, every reject has a reason, (c) the daylight-saving repair story. |
| 6 | 1:00 | Gold and the answer | Two charts from Gold, the publish gate. |
| 7 | 1:00 | Evidence of efficiency | 8:39 -> 4:57, the NFS hypothesis disproved, what did not help. |
| 8 | 0:30 | Operations and lessons | Scheduled run, failure handling, three lessons. |

Total 9:15 plus 0:45 buffer. Rehearse twice with a timer; cut slide 7 detail first if over.

## Live demo: end-to-end run, deployment, schedule, Gold output
1. Merged PR with green CI (deployment path), 30 s.
2. Trigger the prod workflow (manual approval) on a small scope, e.g. one new month; check beforehand that it runs in < 3 min.
3. Show the Bronze reconcile line, the Silver quality row and the Gold `_READY` with the new run_id.
4. Show the scheduled run history (Actions tab) and the run log.
5. Failure demo: an unknown header makes Bronze refuse; Gold `_READY` stays unchanged.
6. Open the dashboard that reads Gold only.

Fallbacks: cluster down -> unset `CITIBIKE_SPARK_MASTER` (local mode); runner down -> run the script by hand and show the log;
network down -> pre-recorded terminal capture and screenshots of every step kept in docs/.
Day before: run the whole demo once and keep the logs; check disk space (Bronze is ~10 GB, plus Silver and Gold).
