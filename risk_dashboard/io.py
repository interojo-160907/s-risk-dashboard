from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .schema import APS_REQUIRED_COLS, PROD_REQUIRED_COLS


@dataclass(frozen=True)
class DataPaths:
    aps_snapshot_csv: Path
    production_actuals_csv: Path


def default_data_paths(base_dir: str | os.PathLike = "data") -> DataPaths:
    base = Path(base_dir)
    return DataPaths(
        aps_snapshot_csv=base / "aps_snapshot.csv",
        production_actuals_csv=base / "production_actuals.csv",
    )


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _normalize_dates(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.date
    return out


def load_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def append_csv(path: Path, df_new: pd.DataFrame) -> None:
    _ensure_parent_dir(path)
    if path.exists():
        df_new.to_csv(path, mode="a", header=False, index=False, encoding="utf-8-sig")
    else:
        df_new.to_csv(path, index=False, encoding="utf-8-sig")


def validate_required_columns(df: pd.DataFrame, required: list[str], *, name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} 필수 컬럼 누락: {missing}")


def load_aps_snapshot(path: Path) -> pd.DataFrame:
    df = load_csv_if_exists(path)
    if df.empty:
        return df
    validate_required_columns(df, APS_REQUIRED_COLS, name="APS 스냅샷")
    date_cols = [c for c in APS_REQUIRED_COLS if c.endswith("일")]
    return _normalize_dates(df, date_cols)


def load_production_actuals(path: Path) -> pd.DataFrame:
    df = load_csv_if_exists(path)
    if df.empty:
        return df
    validate_required_columns(df, PROD_REQUIRED_COLS, name="생산실적")
    return _normalize_dates(df, ["생산일자"])


def read_input_table(path: str | os.PathLike) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    if p.suffix.lower() in [".xlsx", ".xlsm", ".xls"]:
        return pd.read_excel(p)  # type: ignore[call-arg]
    return pd.read_csv(p, encoding="utf-8-sig")

