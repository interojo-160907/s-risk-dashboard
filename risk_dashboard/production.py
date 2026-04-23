from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from .schema import PROD_COLS


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return date(y, m, 1)


def prev_month_range(asof: date) -> tuple[date, date]:
    this_start = month_start(asof)
    prev_start = add_months(this_start, -1)
    prev_end = this_start - timedelta(days=1)
    return prev_start, prev_end


@dataclass(frozen=True)
class ProductionMonthView:
    label: str
    start: date
    end: date
    df: pd.DataFrame


@dataclass(frozen=True)
class ProductionCutoffViews:
    asof: date
    cutoff: date
    prev_month: ProductionMonthView
    curr_month: ProductionMonthView


def _to_date_series(s: pd.Series) -> pd.Series:
    # Already normalized in IO, but be defensive.
    return pd.to_datetime(s, errors="coerce").dt.date


def build_cutoff_views(prod_df: pd.DataFrame, *, asof: date) -> ProductionCutoffViews:
    cutoff = asof - timedelta(days=1)

    pm_start, pm_end = prev_month_range(asof)
    cm_start = month_start(asof)
    cm_end = cutoff

    if prod_df.empty:
        empty = prod_df.copy()
        return ProductionCutoffViews(
            asof=asof,
            cutoff=cutoff,
            prev_month=ProductionMonthView(label="전월", start=pm_start, end=pm_end, df=empty),
            curr_month=ProductionMonthView(label="당월", start=cm_start, end=cm_end, df=empty),
        )

    df = prod_df.copy()
    if PROD_COLS.생산일자 in df.columns:
        df[PROD_COLS.생산일자] = _to_date_series(df[PROD_COLS.생산일자])

    # Always exclude 'asof' day and beyond (production may be in-progress).
    df = df[df[PROD_COLS.생산일자].notna()].copy()
    df = df[df[PROD_COLS.생산일자] <= cutoff].copy()

    prev_df = df[(df[PROD_COLS.생산일자] >= pm_start) & (df[PROD_COLS.생산일자] <= pm_end)].copy()
    if cm_end < cm_start:
        curr_df = df.iloc[0:0].copy()
    else:
        curr_df = df[(df[PROD_COLS.생산일자] >= cm_start) & (df[PROD_COLS.생산일자] <= cm_end)].copy()

    return ProductionCutoffViews(
        asof=asof,
        cutoff=cutoff,
        prev_month=ProductionMonthView(label="전월", start=pm_start, end=pm_end, df=prev_df),
        curr_month=ProductionMonthView(label="당월", start=cm_start, end=cm_end, df=curr_df),
    )


def summarize_by_process(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[PROD_COLS.공정, PROD_COLS.생산수량])
    out = (
        df.groupby(PROD_COLS.공정, dropna=False)[PROD_COLS.생산수량]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    out[PROD_COLS.생산수량] = out[PROD_COLS.생산수량].astype(int)
    return out


def summarize_daily_total(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[PROD_COLS.생산일자, PROD_COLS.생산수량])
    out = (
        df.groupby(PROD_COLS.생산일자, dropna=False)[PROD_COLS.생산수량]
        .sum()
        .sort_index()
        .reset_index()
    )
    out[PROD_COLS.생산수량] = out[PROD_COLS.생산수량].astype(int)
    return out

