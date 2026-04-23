from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONF_DIR = PROJECT_ROOT / "conf"
DEFAULT_SPARK_CONFIG_PATH = CONF_DIR / "spark_config.json"
DEFAULT_PATHS_CONFIG_PATH = CONF_DIR / "paths.json"


def _load_json(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path

    with config_path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _is_remote_uri(path_value: str) -> bool:
    return path_value.startswith(("s3://", "dbfs:/", "abfss://", "gs://", "hdfs://"))


def _resolve_project_path(path_value: str) -> str:
    if _is_remote_uri(path_value):
        return path_value

    resolved = Path(path_value)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return str(resolved)


def load_spark_config(path: str | Path | None = None) -> dict[str, Any]:
    spark_config_path = path or DEFAULT_SPARK_CONFIG_PATH
    return _load_json(spark_config_path)


def load_paths_config(path: str | Path | None = None, resolve_paths: bool = True) -> dict[str, str]:
    paths_config_path = path or DEFAULT_PATHS_CONFIG_PATH
    paths_config = _load_json(paths_config_path)
    if not resolve_paths:
        return paths_config

    return {key: _resolve_project_path(value) for key, value in paths_config.items()}
