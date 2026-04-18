"""Shared helpers for saving and loading benchmark result snapshots."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
PARENT_DIR = ROOT_DIR.parent
RESULTS_DIR = PARENT_DIR / "benchmark_results"


def ensure_results_dir() -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    return RESULTS_DIR


def latest_result_path(name: str) -> Path:
    return ensure_results_dir() / f"{name}_latest.json"


def save_latest_results(name: str, payload: dict[str, Any]) -> Path:
    data = {
        "name": name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    path = latest_result_path(name)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    return path


def load_latest_results(name: str) -> dict[str, Any] | None:
    path = latest_result_path(name)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
