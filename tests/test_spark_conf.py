import pytest

from citibike.spark import spark_conf


def cfg(**spark):
    return {"env": "test", "spark": spark}


def test_local_mode_pins_driver_to_loopback_and_logs_events(monkeypatch, tmp_path):
    monkeypatch.delenv("CITIBIKE_SPARK_MASTER", raising=False)
    master, conf = spark_conf(cfg(master="local[8]", event_log_dir=str(tmp_path)))
    assert master == "local[8]"
    assert conf["spark.driver.host"] == conf["spark.driver.bindAddress"] == "127.0.0.1"
    assert conf["spark.eventLog.enabled"] == "true"
    assert conf["spark.eventLog.dir"] == f"file://{tmp_path}"
    assert "spark.executor.memory" not in conf


def test_cluster_mode_uses_driver_host_and_executor_settings(monkeypatch):
    monkeypatch.delenv("CITIBIKE_SPARK_MASTER", raising=False)
    master, conf = spark_conf(cfg(master="spark://10.0.0.1:7077", driver_host="10.0.0.1",
                                  executor_memory="6g", executor_cores=4, cores_max=8))
    assert master == "spark://10.0.0.1:7077"
    assert conf["spark.driver.host"] == conf["spark.driver.bindAddress"] == "10.0.0.1"
    assert conf["spark.executor.memory"] == "6g"
    assert conf["spark.executor.cores"] == "4"
    assert conf["spark.cores.max"] == "8"


def test_cluster_mode_without_driver_host_is_refused(monkeypatch):
    monkeypatch.delenv("CITIBIKE_SPARK_MASTER", raising=False)
    with pytest.raises(ValueError, match="driver_host"):
        spark_conf(cfg(master="spark://10.0.0.1:7077"))


def test_env_override_switches_cluster_config_to_local(monkeypatch):
    monkeypatch.setenv("CITIBIKE_SPARK_MASTER", "local[8]")
    master, conf = spark_conf(cfg(master="spark://10.0.0.1:7077", driver_host="10.0.0.1",
                                  executor_memory="6g"))
    assert master == "local[8]"
    assert conf["spark.driver.host"] == "127.0.0.1"
    assert "spark.executor.memory" not in conf


def test_unknown_master_is_refused(monkeypatch):
    monkeypatch.delenv("CITIBIKE_SPARK_MASTER", raising=False)
    with pytest.raises(ValueError, match="unsupported"):
        spark_conf(cfg(master="yarn"))


def test_cluster_mode_uses_smaller_driver_than_local_mode(monkeypatch):
    monkeypatch.delenv("CITIBIKE_SPARK_MASTER", raising=False)
    settings = dict(driver_memory="10g", cluster_driver_memory="3g", driver_host="10.0.0.1")
    _, local = spark_conf(cfg(master="local[8]", **settings))
    _, cluster = spark_conf(cfg(master="spark://10.0.0.1:7077", **settings))
    assert local["spark.driver.memory"] == "10g"
    assert cluster["spark.driver.memory"] == "3g"
