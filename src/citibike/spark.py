"""SparkSession built from the environment config.

Local mode (`local[N]`) pins the driver to 127.0.0.1 so it never depends on hostname
resolution. Cluster mode (`spark://...`) needs an address the workers can reach, taken from
`[spark] driver_host`. CITIBIKE_SPARK_MASTER overrides the configured master, so the same
environment can run on one node or on the cluster (scaling experiment).
Every session writes an event log for the history server (performance evidence).
Small intermediate tables that several actions reuse are computed once with compute_once(): a reliable checkpoint in
the lake (shared over NFS, so it survives a lost executor) instead of a localCheckpoint on the executors.
"""
import os

from pyspark.sql import DataFrame, SparkSession

DEFAULT_EVENT_LOG_DIR = "~/citibike/spark-events"


def spark_conf(cfg: dict) -> tuple[str, dict[str, str]]:
    """Return (master, Spark settings) for this environment, without starting Spark."""
    sc = cfg.get("spark", {})
    master = os.environ.get("CITIBIKE_SPARK_MASTER") or sc.get("master", "local[*]")
    event_dir = os.path.expanduser(sc.get("event_log_dir", DEFAULT_EVENT_LOG_DIR))

    conf = {
        "spark.driver.memory": sc.get("driver_memory", "4g"),
        "spark.sql.shuffle.partitions": str(sc.get("shuffle_partitions", 16)),
        # rewrite only the year/month partitions a run touches
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.session.timeZone": sc.get("timezone", "America/New_York"),
        "spark.sql.parquet.compression.codec": "zstd",
        # a timestamp that does not match its pattern becomes null (and a counted reject), never an exception
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
        "spark.ui.showConsoleProgress": "false",  # progress bars flood logs and CI output
        "spark.cleaner.referenceTracking.cleanCheckpoints": "true",  # delete checkpoint files once they are unused
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": f"file://{event_dir}",
    }
    # extra Spark settings from [spark.conf], e.g. the result of a tuning experiment (docs/performance.md)
    conf.update({k: str(v) for k, v in sc.get("conf", {}).items()})

    if master.startswith("local"):
        conf["spark.driver.bindAddress"] = "127.0.0.1"
        conf["spark.driver.host"] = "127.0.0.1"
    elif master.startswith("spark://"):
        # the driver shares spark-vm with a worker, so it gets less memory than in local mode
        conf["spark.driver.memory"] = sc.get("cluster_driver_memory", conf["spark.driver.memory"])
        host = sc.get("driver_host")
        if not host:
            raise ValueError(f"master {master} needs [spark] driver_host (an address the workers can reach)")
        conf["spark.driver.bindAddress"] = host
        conf["spark.driver.host"] = host
        for key, name in [("executor_memory", "spark.executor.memory"),
                          ("executor_cores", "spark.executor.cores"),
                          ("cores_max", "spark.cores.max")]:
            if key in sc:
                conf[name] = str(sc[key])
    else:
        raise ValueError(f"unsupported Spark master {master!r}")

    return master, conf


def get_spark(cfg: dict, app: str) -> SparkSession:
    master, conf = spark_conf(cfg)
    # Spark refuses to start if the event log directory is missing
    os.makedirs(conf["spark.eventLog.dir"].removeprefix("file://"), exist_ok=True)
    builder = SparkSession.builder.appName(f"citibike-{cfg['env']}-{app}").master(master)
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    lake = cfg.get("paths", {}).get("lake_root")
    if lake:   # on the shared lake, so both workers write to and read from the same place
        spark.sparkContext.setCheckpointDir(os.path.join(os.path.expanduser(lake), "_meta", "checkpoints"))
    return spark


def compute_once(df: DataFrame) -> DataFrame:
    """Compute a small, reused table once and cut its lineage.

    With a checkpoint directory (always set by get_spark) this is a reliable checkpoint: the result is written to the
    shared lake and survives the loss of an executor. localCheckpoint() would keep the blocks on the executors, so
    losing the second node mid-job would fail the job. Without a directory (ad-hoc sessions) it falls back to that.
    """
    if df.sparkSession.sparkContext.getCheckpointDir():
        return df.checkpoint()
    return df.localCheckpoint()
