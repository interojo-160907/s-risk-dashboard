from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from .schema import APS_COLS


@dataclass(frozen=True)
class OrderProgressConfig:
    # "공정 코드:" 블록
    제품그룹코드: tuple[str, str] = ("공정 코드:", "제품 그룹 코드")
    이니셜: tuple[str, str] = ("공정 코드:", "이니셜")
    제품이름: tuple[str, str] = ("공정 코드:", "제품 이름")
    납기일: tuple[str, str] = ("공정 코드:", "납기일")

    # 공정별 종료일 컬럼(1단 헤더에 공정명이 있고 2단은 '종료일')
    사출: tuple[str, str] = ("[10]사출조립", "종료일")
    분리: tuple[str, str] = ("[20]분리", "종료일")
    하이드: tuple[str, str] = ("[45]하이드레이션/전면검사", "종료일")
    접착: tuple[str, str] = ("[55]접착/멸균", "종료일")
    누수: tuple[str, str] = ("[80]누수/규격검사", "종료일")
    포장: tuple[str, str] = ("[85]포장", "종료일")


CFG = OrderProgressConfig()


def _norm_text(x: object) -> str:
    if pd.isna(x):
        return ""
    return " ".join(str(x).strip().split())


def parse_sheet_date(sheet_name: str, *, default_century: int = 2000) -> date | None:
    s = _norm_text(sheet_name)
    s_digits = "".join(ch for ch in s if ch.isdigit())

    try:
        if len(s_digits) == 6:  # yyMMdd
            yy = int(s_digits[0:2])
            mm = int(s_digits[2:4])
            dd = int(s_digits[4:6])
            return date(default_century + yy, mm, dd)
        if len(s_digits) == 8:  # yyyyMMdd
            yyyy = int(s_digits[0:4])
            mm = int(s_digits[4:6])
            dd = int(s_digits[6:8])
            return date(yyyy, mm, dd)
    except Exception:
        return None
    return None


def load_order_progress_sheet(path: Path, sheet_name: str | int) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name, header=[0, 1])  # type: ignore[call-arg]
    return df


def to_aps_snapshot(
    df_multi: pd.DataFrame,
    *,
    기준일: date,
    s_product_names: set[str],
    name_to_code: dict[str, str],
) -> tuple[pd.DataFrame, dict[str, int]]:
    required_cols = [
        CFG.이니셜,
        CFG.제품이름,
        CFG.납기일,
        CFG.사출,
        CFG.분리,
        CFG.하이드,
        CFG.접착,
        CFG.누수,
        CFG.포장,
    ]
    missing = [c for c in required_cols if c not in df_multi.columns]
    if missing:
        raise ValueError(f"주문별 공정진도 시트 필수 컬럼 누락: {missing}")

    tmp = pd.DataFrame(
        {
            "이니셜": df_multi[CFG.이니셜],
            "제품 이름": df_multi[CFG.제품이름],
            "납기일": df_multi[CFG.납기일],
            "사출종료일": df_multi[CFG.사출],
            "분리종료일": df_multi[CFG.분리],
            "하이드종료일": df_multi[CFG.하이드],
            "접착종료일": df_multi[CFG.접착],
            "누수종료일": df_multi[CFG.누수],
            "포장종료일": df_multi[CFG.포장],
        }
    )

    tmp["제품 이름_norm"] = tmp["제품 이름"].map(_norm_text)
    before = len(tmp)
    if s_product_names:
        tmp = tmp[tmp["제품 이름_norm"].isin(s_product_names)].copy()
    after_filter = len(tmp)

    tmp["품목코드"] = tmp["제품 이름_norm"].map(lambda n: name_to_code.get(n, ""))
    missing_code = int((tmp["품목코드"] == "").sum())

    out = pd.DataFrame(
        {
            APS_COLS.기준일: 기준일,
            APS_COLS.수주번호: tmp["이니셜"].astype(str).map(_norm_text),
            APS_COLS.품목코드: tmp["품목코드"],
            APS_COLS.품명: tmp["제품 이름_norm"],
            APS_COLS.납기일: pd.to_datetime(tmp["납기일"], errors="coerce").dt.date,
            APS_COLS.사출종료일: pd.to_datetime(tmp["사출종료일"], errors="coerce").dt.date,
            APS_COLS.분리종료일: pd.to_datetime(tmp["분리종료일"], errors="coerce").dt.date,
            APS_COLS.하이드종료일: pd.to_datetime(tmp["하이드종료일"], errors="coerce").dt.date,
            APS_COLS.접착종료일: pd.to_datetime(tmp["접착종료일"], errors="coerce").dt.date,
            APS_COLS.누수종료일: pd.to_datetime(tmp["누수종료일"], errors="coerce").dt.date,
            APS_COLS.포장종료일: pd.to_datetime(tmp["포장종료일"], errors="coerce").dt.date,
        }
    )

    stats = {
        "rows_in_sheet": before,
        "rows_after_s_filter": after_filter,
        "missing_item_code": missing_code,
    }
    return out, stats

