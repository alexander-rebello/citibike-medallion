# Spark standalone cluster (spark-vm + calc)

| Node | Role | Config | Worker size |
|---|---|---|---|
| spark-vm `192.168.26.92` | master + worker (also hosts driver, NFS server, CI runner) | `spark-env.vm.sh` | 8 cores, 8 GB |
| calc `192.168.26.96` | worker only | `spark-env.calc.sh` | 8 cores, 12 GB |

Both nodes mount `~/citibike` (NFS from spark-vm) at the same path, so executors see the same files.
Addresses are pinned to the private IPs because hostname resolution broke after Tailscale.

Run on spark-vm:

```
infra/spark/cluster.sh deploy    # once, or after changing these files: installs config + systemd units, enables boot start
infra/spark/cluster.sh start     # stop | restart | status also available
```

Master UI: `ssh -L 8080:192.168.26.92:8080 spark-vm`, then http://localhost:8080.
Use the cluster for a run with `CITIBIKE_SPARK_MASTER=spark://192.168.26.92:7077` (needs `driver_host` in the env config);
without it, runs stay in `local[8]`. Services restart on failure and retry until the master is reachable.
