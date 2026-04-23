from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from risk_dashboard.io import default_data_paths, load_production_actuals
from risk_dashboard.logging_utils import get_logger
from risk_dashboard.production import month_start, prev_month_range
from risk_dashboard.production import build_views_with_ranges, summarize_by_process, summarize_daily_total
from risk_dashboard.schema import PROD_COLS, PROD_REQUIRED_COLS


st.set_page_config(page_title="S관 생산실적 대시보드", layout="wide")
st.title("S관 생산실적 대시보드")

paths = default_data_paths("data")
logger = get_logger("streamlit.app", log_file="logs/streamlit_app.log")


def _norm_text(x: object) -> str:
    if pd.isna(x):
        return ""
    return " ".join(str(x).strip().split())


def _map_process(code: object) -> str:
    s = _norm_text(code)
    if s.startswith("[10]"):
        return "사출"
    if s.startswith("[20]"):
        return "분리"
    if s.startswith("[45]"):
        return "하이드"
    if s.startswith("[55]"):
        return "접착"
    if s.startswith("[80]"):
        return "누수"
    if s.startswith("[85]"):
        return "포장"
    return s


@st.cache_data(show_spinner=False)
def load_prod_from_excel(prod_xlsx: str, *, status: str = "확인", qty_col: str = "양품수량") -> pd.DataFrame:
    p = Path(prod_xlsx)
    if not p.exists():
        raise FileNotFoundError(str(p))
    xl = pd.ExcelFile(p)
    frames = []
    for sh in ["전월", "당월"]:
        if sh in xl.sheet_names:
            frames.append(pd.read_excel(p, sheet_name=sh))  # type: ignore[call-arg]
    if not frames:
        return pd.DataFrame(columns=PROD_REQUIRED_COLS)

    raw = pd.concat(frames, ignore_index=True)
    required = ["생산일자", "공정코드", "품목코드", qty_col, "상태"]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise ValueError(f"필수 컬럼 누락: {missing}")

    raw = raw.copy()
    raw["상태_norm"] = raw["상태"].map(_norm_text)
    raw = raw[raw["상태_norm"] == _norm_text(status)].copy()

    out = pd.DataFrame(
        {
            PROD_COLS.생산일자: pd.to_datetime(raw["생산일자"], errors="coerce").dt.date,
            PROD_COLS.공정: raw["공정코드"].map(_map_process),
            PROD_COLS.품목코드: raw["품목코드"].map(_norm_text),
            PROD_COLS.생산수량: pd.to_numeric(raw[qty_col], errors="coerce"),
        }
    )
    out = out.dropna(subset=[PROD_COLS.생산일자, PROD_COLS.생산수량]).copy()
    out = out[out[PROD_COLS.품목코드].astype(str).str.len() > 0].copy()
    out[PROD_COLS.생산수량] = out[PROD_COLS.생산수량].astype(int)
    return out[PROD_REQUIRED_COLS].copy()


with st.sidebar:
    st.header("데이터")
    default_prod = Path("data/production_actuals_recent.csv")
    source = st.radio("소스", ["CSV", "엑셀"], index=0, horizontal=True)

    if source == "CSV":
        prod_path = st.text_input(
            "생산실적 CSV",
            value=str(default_prod if default_prod.exists() else paths.production_actuals_csv),
        )
        st.caption("CSV 인코딩은 `utf-8-sig` 권장(엑셀 호환).")
    else:
        prod_xlsx = st.text_input("생산실적(간편)", value="생산실적현황(간편)_S관.xlsx")
        st.caption("엑셀 파일이 리포지토리(같은 폴더)에 있으면 바로 동작합니다.")

if source == "CSV":
    logger.info("source=CSV | prod=%s", prod_path)
    try:
        prod_df = load_production_actuals(Path(prod_path))
    except Exception as e:
        logger.exception("production load failed")
        st.error(f"생산실적 로드 실패: {e}")
        prod_df = pd.DataFrame()
else:
    logger.info("source=EXCEL | prod=%s", prod_xlsx)
    with st.spinner("엑셀에서 생산실적 생성 중..."):
        try:
            prod_df = load_prod_from_excel(prod_xlsx, status="확인", qty_col="양품수량")
        except Exception as e:
            logger.exception("excel->production failed")
            st.error(f"엑셀 → 생산실적 변환 실패: {e}")
            prod_df = pd.DataFrame(columns=PROD_REQUIRED_COLS)

with st.sidebar:
    asof = st.date_input("기준일", value=date.today())

tabs = st.tabs(["S관 실적"])

with tabs[0]:
    st.subheader("S관 생산실적 현황(간편)")
    st.caption("컨셉: 전월/당월 기간조회(직접 선택) + 당월 생산실적은 끝공정(누수) 기준.")

    with st.sidebar:
        st.header("기간조회")
        cols = st.columns(3)
        reset_clicked = cols[0].button("해제", use_container_width=True)
        manual_clicked = cols[1].button("직접", use_container_width=True)
        auto_clicked = cols[2].button("전월/당월", use_container_width=True)

        if reset_clicked:
            st.session_state.pop("range_mode", None)
            for k in ["prev_start", "prev_end", "curr_start", "curr_end"]:
                st.session_state.pop(k, None)
        if manual_clicked:
            st.session_state["range_mode"] = "manual"
        if auto_clicked:
            st.session_state["range_mode"] = "auto"

        range_mode = st.session_state.get("range_mode", "auto")
        prev_start = prev_end = curr_start = curr_end = None
        if range_mode == "manual":
            if "prev_start" not in st.session_state:
                p0, p1 = prev_month_range(asof)
                st.session_state["prev_start"] = p0
                st.session_state["prev_end"] = p1
            if "curr_start" not in st.session_state:
                c0 = month_start(asof)
                st.session_state["curr_start"] = c0
                st.session_state["curr_end"] = c0

            st.caption("전월/당월 비교 기간을 각각 선택하세요.")
            prev_start = st.date_input("전월 시작", key="prev_start")
            prev_end = st.date_input("전월 종료", key="prev_end")
            curr_start = st.date_input("당월 시작", key="curr_start")
            curr_end = st.date_input("당월 종료", key="curr_end")

    if prod_df.empty:
        st.info("생산실적 데이터가 없습니다.")
    else:
        views = build_views_with_ranges(
            prod_df,
            asof=asof,
            prev_start=prev_start,
            prev_end=prev_end,
            curr_start=curr_start,
            curr_end=curr_end,
        )
        st.caption(f"기준일: {views.asof.isoformat()} / 집계 cutoff(당일 제외): {views.cutoff.isoformat()}")

        def _sum_qty(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(pd.to_numeric(df[PROD_COLS.생산수량], errors="coerce").fillna(0).sum())

        def _sum_final(df: pd.DataFrame, *, final_proc: str) -> int:
            if df.empty:
                return 0
            mask = df[PROD_COLS.공정].astype(str).str.contains(final_proc, na=False)
            return _sum_qty(df[mask].copy())

        def _n_days(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(df[PROD_COLS.생산일자].nunique())

        def _n_items(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(df[PROD_COLS.품목코드].nunique())

        final_proc = "누수"
        prev_total = _sum_final(views.prev_month.df, final_proc=final_proc)
        curr_total = _sum_final(views.curr_month.df, final_proc=final_proc)
        prev_all = _sum_qty(views.prev_month.df)
        curr_all = _sum_qty(views.curr_month.df)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("전월 생산수량(누수)", f"{prev_total:,}")
        m2.metric("당월 생산수량(누수/MTD)", f"{curr_total:,}")
        m3.metric("당월 생산일수", f"{_n_days(views.curr_month.df):,}")
        m4.metric("당월 품목수", f"{_n_items(views.curr_month.df):,}")
        st.caption(f"참고(전체 공정 합계) 전월: {prev_all:,} / 당월: {curr_all:,}")

        left, right = st.columns(2)

        process_order = ["사출", "분리", "하이드", "접착", "누수"]

        def _process_view(df: pd.DataFrame) -> pd.DataFrame:
            s = summarize_by_process(df)
            if s.empty:
                return s
            s2 = s.copy()
            known = s2[PROD_COLS.공정].astype(str).isin(process_order)
            other_qty = int(s2.loc[~known, PROD_COLS.생산수량].sum()) if (~known).any() else 0
            s2 = s2[known].copy()
            s2[PROD_COLS.공정] = pd.Categorical(s2[PROD_COLS.공정].astype(str), categories=process_order, ordered=True)
            s2 = s2.sort_values(PROD_COLS.공정).copy()
            s2[PROD_COLS.생산수량] = s2[PROD_COLS.생산수량].astype(int)
            if other_qty > 0:
                s2 = pd.concat(
                    [
                        s2,
                        pd.DataFrame({PROD_COLS.공정: ["기타"], PROD_COLS.생산수량: [other_qty]}),
                    ],
                    ignore_index=True,
                )
            return s2.reset_index(drop=True)

        with left:
            st.markdown(f"**전월 ({views.prev_month.start.isoformat()} ~ {views.prev_month.end.isoformat()})**")
            prev_proc = _process_view(views.prev_month.df)
            if prev_proc.empty:
                st.info("전월 실적이 없습니다(집계 기준/데이터를 확인하세요).")
            else:
                st.bar_chart(prev_proc.set_index(PROD_COLS.공정)[PROD_COLS.생산수량])
                st.dataframe(prev_proc, use_container_width=True, hide_index=True)

        with right:
            st.markdown(f"**당월 ({views.curr_month.start.isoformat()} ~ {views.curr_month.end.isoformat()})**")
            if views.curr_month.end < views.curr_month.start:
                st.info("당월은 아직 집계 대상이 없습니다(기준일이 월초이거나, cutoff가 월 시작 이전).")
            else:
                curr_proc = _process_view(views.curr_month.df)
                if curr_proc.empty:
                    st.info("당월 실적이 없습니다(당일 제외/데이터를 확인하세요).")
                else:
                    st.bar_chart(curr_proc.set_index(PROD_COLS.공정)[PROD_COLS.생산수량])
                    st.dataframe(curr_proc, use_container_width=True, hide_index=True)

                daily = summarize_daily_total(
                    views.curr_month.df[views.curr_month.df[PROD_COLS.공정].astype(str).str.contains(final_proc, na=False)].copy()
                )
                if not daily.empty:
                    st.markdown("**당월 일자별 합계(누수)**")
                    st.line_chart(daily.set_index(PROD_COLS.생산일자)[PROD_COLS.생산수량])

        with st.expander("원천 데이터 보기(전월+당월, cutoff 반영)"):
            st.dataframe(
                pd.concat([views.prev_month.df, views.curr_month.df], ignore_index=True)
                .sort_values([PROD_COLS.생산일자, PROD_COLS.공정, PROD_COLS.품목코드]),
                use_container_width=True,
            )
