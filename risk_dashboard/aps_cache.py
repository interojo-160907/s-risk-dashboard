from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class FileSignature:
    path: str
    mtime: float
    size: int


def signature(path: str | Path) -> FileSignature:
    p = Path(path)
    st = p.stat()
    return FileSignature(path=str(p.resolve()), mtime=float(st.st_mtime), size=int(st.st_size))


@dataclass(frozen=True)
class ApsCachePaths:
    data_pkl: Path
    meta_json: Path


def default_cache_paths(base_dir: str | Path = "data") -> ApsCachePaths:
    base = Path(base_dir)
    return ApsCachePaths(
        data_pkl=base / "aps_risk_cache.pkl",
        meta_json=base / "aps_risk_cache_meta.json",
    )


def load_cached_tables(paths: ApsCachePaths, *, sig: FileSignature, scope_key: str) -> dict[str, pd.DataFrame] | None:
    if not paths.data_pkl.exists() or not paths.meta_json.exists():
        return None
    try:
        meta = json.loads(paths.meta_json.read_text(encoding="utf-8"))
    except Exception:
        return None

    if (
        meta.get("input_path") != sig.path
        or float(meta.get("input_mtime", -1)) != sig.mtime
        or int(meta.get("input_size", -1)) != sig.size
        or meta.get("scope_key") != scope_key
    ):
        return None

    try:
        obj: Any = pd.read_pickle(paths.data_pkl)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    # best-effort: ensure values are DataFrames
    out: dict[str, pd.DataFrame] = {}
    for k, v in obj.items():
        if isinstance(v, pd.DataFrame):
            out[str(k)] = v
    return out or None


def load_any_tables(paths: ApsCachePaths) -> dict[str, pd.DataFrame] | None:
    if not paths.data_pkl.exists():
        return None
    try:
        obj: Any = pd.read_pickle(paths.data_pkl)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    out: dict[str, pd.DataFrame] = {}
    for k, v in obj.items():
        if isinstance(v, pd.DataFrame):
            out[str(k)] = v
    return out or None


def save_cached_tables(
    paths: ApsCachePaths,
    *,
    sig: FileSignature,
    scope_key: str,
    tables: dict[str, pd.DataFrame],
) -> None:
    paths.data_pkl.parent.mkdir(parents=True, exist_ok=True)
    pd.to_pickle(tables, paths.data_pkl)
    meta = {
        "input_path": sig.path,
        "input_mtime": sig.mtime,
        "input_size": sig.size,
        "scope_key": scope_key,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    paths.meta_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
