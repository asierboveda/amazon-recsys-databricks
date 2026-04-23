from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.config import load_paths_config, load_spark_config


def _normalize_spark_conf(raw_conf: dict[str, Any]) -> dict[str, str]:
    normalized = {key: str(value) for key, value in raw_conf.items()}

    # Kryo improves serialization throughput and reduces payload size
    # for large-scale shuffles common in implicit-feedback recommenders.
    normalized.setdefault("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    normalized.setdefault("spark.kryoserializer.buffer.max", "256m")
    return normalized


def create_spark_session(
    app_name: str | None = None,
    spark_config_path: str | Path | None = None,
    paths_config_path: str | Path | None = None,
) -> SparkSession:
    spark_config = load_spark_config(spark_config_path)
    spark_conf = _normalize_spark_conf(spark_config.get("spark_conf", {}))

    effective_app_name = app_name or spark_config.get("app_name", "amazon-recommendation-engine")
    builder = SparkSession.builder.appName(effective_app_name)

    master = spark_config.get("master")
    if master:
        builder = builder.master(master)

    for key, value in spark_conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(spark_config.get("log_level", "WARN"))

    if spark_config.get("set_checkpoint_dir", True):
        paths = load_paths_config(paths_config_path)
        checkpoint_dir = paths.get("checkpoints_dir")
        if checkpoint_dir:
            spark.sparkContext.setCheckpointDir(checkpoint_dir)

    return spark
