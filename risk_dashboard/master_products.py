from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class MasterCols:
    제품명코드: str = "제품명코드"
    제품명: str = "제품명"
    공장구분: str = "공장구분"


MASTER_COLS = MasterCols()


def _norm_text(x: object) -> str:
    if pd.isna(x):
        return ""
    return " ".join(str(x).strip().split())


def load_master_table(path: Path, sheet_name: str | int = 0) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name)  # type: ignore[call-arg]
    return df


def filter_sgwan(df: pd.DataFrame) -> pd.DataFrame:
    if MASTER_COLS.공장구분 in df.columns:
        mask = df[MASTER_COLS.공장구분].astype(str).str.contains("S관", na=False)
        return df[mask].copy()
    return df.copy()


def build_name_to_code(df: pd.DataFrame) -> dict[str, str]:
    if MASTER_COLS.제품명 not in df.columns or MASTER_COLS.제품명코드 not in df.columns:
        raise ValueError("마스터 테이블에 '제품명코드', '제품명' 컬럼이 필요합니다.")

    out: dict[str, str] = {}
    for _, row in df.iterrows():
        name = _norm_text(row.get(MASTER_COLS.제품명))
        code = _norm_text(row.get(MASTER_COLS.제품명코드))
        if name and code and name not in out:
            out[name] = code
    return out


def s_product_name_set(df: pd.DataFrame) -> set[str]:
    if MASTER_COLS.제품명 not in df.columns:
        raise ValueError("마스터 테이블에 '제품명' 컬럼이 필요합니다.")
    return {_norm_text(x) for x in df[MASTER_COLS.제품명].tolist() if _norm_text(x)}

