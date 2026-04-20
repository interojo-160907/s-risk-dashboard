from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from .schema import APS_COLS, PROCESS_COLS_IN_ORDER


@dataclass(frozen=True)
class SnapshotPair:
    asof_date: date
    prev_date: date | None
    today: pd.DataFrame
    prev: pd.DataFrame


def available_snapshot_dates(aps_df: pd.DataFrame) -> list[date]:
    if aps_df.empty:
        return []
    d = pd.to_datetime(aps_df[APS_COLS.기준일], errors="coerce").dt.date.dropna().unique()
    return sorted(d.tolist())


def pick_snapshot_pair(aps_df: pd.DataFrame, asof: date | None = None) -> SnapshotPair:
    dates = available_snapshot_dates(aps_df)
    if not dates:
        return SnapshotPair(asof_date=date.today(), prev_date=None, today=pd.DataFrame(), prev=pd.DataFrame())

    asof_date = asof or dates[-1]
    if asof_date not in dates:
        asof_date = dates[-1]

    prev_dates = [d for d in dates if d < asof_date]
    prev_date = prev_dates[-1] if prev_dates else None

    today = aps_df[pd.to_datetime(aps_df[APS_COLS.기준일], errors="coerce").dt.date == asof_date].copy()
    prev = (
        aps_df[pd.to_datetime(aps_df[APS_COLS.기준일], errors="coerce").dt.date == prev_date].copy()
        if prev_date
        else pd.DataFrame(columns=today.columns)
    )
    return SnapshotPair(asof_date=asof_date, prev_date=prev_date, today=today, prev=prev)


def _days_diff(a: pd.Series, b: pd.Series) -> pd.Series:
    a_dt = pd.to_datetime(a, errors="coerce")
    b_dt = pd.to_datetime(b, errors="coerce")
    return (a_dt - b_dt).dt.days


def compute_due_status(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    pack = pd.to_datetime(out[APS_COLS.포장종료일], errors="coerce")
    due = pd.to_datetime(out[APS_COLS.납기일], errors="coerce")
    delta = (pack - due).dt.days

    out["납기차이일"] = delta
    out["상태"] = np.select(
        [
            pack.isna() | due.isna(),
            delta <= 0,
            (delta > 0) & (delta <= 2),
            delta > 2,
        ],
        ["미정", "정상", "긴급", "지연"],
        default="미정",
    )
    return out


def _key_cols() -> list[str]:
    return [APS_COLS.수주번호, APS_COLS.품목코드]


def compute_daily_changes(today: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    if today.empty:
        return today

    key = _key_cols()
    today2 = today.copy()
    prev2 = prev.copy() if not prev.empty else pd.DataFrame(columns=today2.columns)

    # 중복 키가 있을 수 있어 마지막 행(가장 최근 입력)을 우선 사용
    today2 = today2.drop_duplicates(subset=key, keep="last")
    prev2 = prev2.drop_duplicates(subset=key, keep="last") if not prev2.empty else prev2

    merged = today2.merge(
        prev2[key + [c for _, c in PROCESS_COLS_IN_ORDER] + ([APS_COLS.필요수량] if APS_COLS.필요수량 in prev2.columns else [])],
        on=key,
        how="left",
        suffixes=("", "_전일"),
    )

    merged["포장변경일수"] = _days_diff(merged[APS_COLS.포장종료일], merged[f"{APS_COLS.포장종료일}_전일"])
    merged["변경상태"] = np.select(
        [
            merged[f"{APS_COLS.포장종료일}_전일"].isna(),
            merged["포장변경일수"] > 0,
            merged["포장변경일수"] < 0,
            merged["포장변경일수"] == 0,
        ],
        ["신규", "지연", "개선", "유지"],
        default="유지",
    )

    proc_deltas: dict[str, pd.Series] = {}
    for proc_name, col in PROCESS_COLS_IN_ORDER:
        prev_col = f"{col}_전일"
        if prev_col not in merged.columns:
            merged[prev_col] = pd.NaT
        delta_col = f"{proc_name}변경일수"
        merged[delta_col] = _days_diff(merged[col], merged[prev_col])
        proc_deltas[proc_name] = merged[delta_col]

    delta_df = pd.DataFrame(proc_deltas)
    max_delta = delta_df.max(axis=1, skipna=True)
    max_proc = delta_df.idxmax(axis=1, skipna=True)

    merged["병목변경일수"] = max_delta
    merged["병목공정"] = np.where((max_delta.fillna(0) > 0), max_proc, "없음")
    return merged


def compute_cause(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    need_today = out[APS_COLS.필요수량] if APS_COLS.필요수량 in out.columns else pd.Series([np.nan] * len(out))
    need_prev = out[f"{APS_COLS.필요수량}_전일"] if f"{APS_COLS.필요수량}_전일" in out.columns else pd.Series([np.nan] * len(out))
    need_increase = (pd.to_numeric(need_today, errors="coerce") - pd.to_numeric(need_prev, errors="coerce")) > 0

    is_delay = out.get("변경상태", pd.Series([""] * len(out))) == "지연"
    is_improve = out.get("변경상태", pd.Series([""] * len(out))) == "개선"
    bottleneck = out.get("병목공정", pd.Series(["없음"] * len(out))) != "없음"

    out["원인"] = np.select(
        [
            is_improve,
            is_delay & need_increase & bottleneck,
            is_delay & need_increase,
            is_delay & (~need_increase),
        ],
        ["생산 개선", "복합", "수주 증가", "생산 지연"],
        default="유지",
    )
    return out


def build_dashboard_table(aps_df: pd.DataFrame, asof: date | None = None) -> tuple[SnapshotPair, pd.DataFrame]:
    pair = pick_snapshot_pair(aps_df, asof=asof)
    if pair.today.empty:
        return pair, pd.DataFrame()

    base = compute_due_status(pair.today)
    changed = compute_daily_changes(base, pair.prev)
    final = compute_cause(changed)

    # 보기 좋은 컬럼 순서
    front = [
        APS_COLS.기준일,
        APS_COLS.수주번호,
        APS_COLS.품목코드,
        APS_COLS.품명,
        APS_COLS.납기일,
        APS_COLS.포장종료일,
        "상태",
        "납기차이일",
        "변경상태",
        "포장변경일수",
        "병목공정",
        "병목변경일수",
        "원인",
    ]
    cols = [c for c in front if c in final.columns] + [c for c in final.columns if c not in front]
    return pair, final[cols].copy()

