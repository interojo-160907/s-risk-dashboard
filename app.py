from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from risk_dashboard.io import default_data_paths, load_aps_snapshot, load_production_actuals
from risk_dashboard.logging_utils import get_logger
from risk_dashboard.logic import available_snapshot_dates, build_dashboard_table
from risk_dashboard.master_products import build_name_to_code, filter_sgwan, load_master_table, s_product_name_set
from risk_dashboard.order_progress import load_order_progress_sheet, parse_sheet_date, to_aps_snapshot
from risk_dashboard.production import build_cutoff_views, summarize_by_process, summarize_daily_total
from risk_dashboard.schema import APS_COLS
from risk_dashboard.schema import PROD_COLS, PROD_REQUIRED_COLS


st.set_page_config(page_title="S관 납기 리스크 관리 대시보드", layout="wide")
st.title("S관 납기 리스크 관리 대시보드")

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
def load_aps_from_excels(master_xlsx: str, order_progress_xlsx: str) -> pd.DataFrame:
    master_path = Path(master_xlsx)
    progress_path = Path(order_progress_xlsx)
    if not master_path.exists():
        raise FileNotFoundError(str(master_path))
    if not progress_path.exists():
        raise FileNotFoundError(str(progress_path))

    mdf = load_master_table(master_path)
    mdf_s = filter_sgwan(mdf)
    s_names = s_product_name_set(mdf_s)
    name_to_code = build_name_to_code(mdf_s)

    xl = pd.ExcelFile(progress_path)
    combined: list[pd.DataFrame] = []
    for sh in xl.sheet_names:
        sh_date = parse_sheet_date(str(sh))
        if sh_date is None:
            continue
        df_multi = load_order_progress_sheet(progress_path, sh)
        aps, _stats = to_aps_snapshot(
            df_multi,
            기준일=sh_date,
            s_product_names=s_names,
            name_to_code=name_to_code,
        )
        combined.append(aps)

    if not combined:
        return pd.DataFrame()
    out = pd.concat(combined, ignore_index=True)
    return out


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
    default_source = "CSV" if Path(paths.aps_snapshot_csv).exists() else "엑셀"
    source = st.radio("소스", ["CSV", "엑셀"], index=0 if default_source == "CSV" else 1, horizontal=True)

    if source == "CSV":
        aps_path = st.text_input("APS 스냅샷 CSV", value=str(paths.aps_snapshot_csv))
        prod_path = st.text_input(
            "생산실적 CSV",
            value=str(default_prod if default_prod.exists() else paths.production_actuals_csv),
        )
        st.caption("CSV 인코딩은 `utf-8-sig` 권장(엑셀 호환).")
    else:
        master_xlsx = st.text_input("S관 제품 마스터", value="S관 생산 제품 리스트.xlsx")
        progress_xlsx = st.text_input("주문별 공정진도", value="주문별 공정진도.xlsx")
        prod_xlsx = st.text_input("생산실적(간편)", value="생산실적현황(간편)_S관.xlsx")
        st.caption("엑셀 파일이 리포지토리(같은 폴더)에 있으면 바로 동작합니다.")

if source == "CSV":
    logger.info("source=CSV | aps=%s | prod=%s", aps_path, prod_path)
    try:
        aps_df = load_aps_snapshot(Path(aps_path))
    except Exception as e:
        logger.exception("APS load failed")
        st.error(f"APS 스냅샷 로드 실패: {e}")
        st.stop()

    try:
        prod_df = load_production_actuals(Path(prod_path))
    except Exception as e:
        logger.exception("production load failed")
        st.error(f"생산실적 로드 실패: {e}")
        prod_df = pd.DataFrame()
else:
    logger.info("source=EXCEL | master=%s | progress=%s | prod=%s", master_xlsx, progress_xlsx, prod_xlsx)
    with st.spinner("엑셀에서 APS 스냅샷 생성 중..."):
        try:
            aps_df = load_aps_from_excels(master_xlsx, progress_xlsx)
        except Exception as e:
            logger.exception("excel->aps failed")
            st.error(f"엑셀 → APS 변환 실패: {e}")
            st.stop()
    with st.spinner("엑셀에서 생산실적 생성 중..."):
        try:
            prod_df = load_prod_from_excel(prod_xlsx, status="확인", qty_col="양품수량")
        except Exception as e:
            logger.exception("excel->production failed")
            st.error(f"엑셀 → 생산실적 변환 실패: {e}")
            prod_df = pd.DataFrame(columns=PROD_REQUIRED_COLS)

dates = available_snapshot_dates(aps_df)
if not dates:
    st.warning("APS 스냅샷 데이터가 없습니다. CSV 소스면 append/적재를 먼저 하고, 엑셀 소스면 날짜형 시트가 있는지 확인하세요.")
    logger.info("no aps snapshot dates")
    st.stop()

with st.sidebar:
    asof = st.selectbox("기준일", options=dates, index=len(dates) - 1, format_func=lambda d: d.isoformat())
    status_filter = st.multiselect("상태 필터", ["정상", "긴급", "지연", "미정"], default=["긴급", "지연"])
    change_filter = st.multiselect("변경상태 필터", ["지연", "개선", "유지", "신규"], default=["지연", "개선"])
    search_order = st.text_input("수주번호 검색", value="")

pair, table = build_dashboard_table(aps_df, asof=asof)
if table.empty:
    st.info("선택한 기준일 데이터가 비어 있습니다.")
    st.stop()

today_all = table.copy()
st.caption(f"비교 기준일: {pair.asof_date.isoformat()} / 전일(이전 기준일): {pair.prev_date.isoformat() if pair.prev_date else '없음'}")

if status_filter:
    table = table[table["상태"].isin(status_filter)].copy()
if change_filter and "변경상태" in table.columns:
    table = table[table["변경상태"].isin(change_filter)].copy()
if search_order.strip():
    table = table[table[APS_COLS.수주번호].astype(str).str.contains(search_order.strip(), na=False)].copy()

col1, col2, col3, col4 = st.columns(4)
col1.metric("총 수주(기준일)", f"{len(today_all):,}")
col2.metric("지연 수주", f"{(today_all['상태']=='지연').sum():,}")
col3.metric("긴급 수주", f"{(today_all['상태']=='긴급').sum():,}")
col4.metric("일정 변경(지연+개선)", f"{today_all['변경상태'].isin(['지연','개선']).sum():,}")

tabs = st.tabs(["납기 리스크", "일정 변경", "공정 병목", "리스크 리스트", "S관 실적"])

with tabs[0]:
    st.subheader("납기 리스크 현황")
    status_counts = today_all["상태"].value_counts().reindex(["지연", "긴급", "정상", "미정"]).fillna(0).astype(int)
    st.bar_chart(status_counts)
    st.dataframe(today_all.sort_values(["상태", "납기차이일"], ascending=[True, False]), use_container_width=True)

with tabs[1]:
    st.subheader("전일 대비 일정 변경")
    if pair.prev_date is None:
        st.info("전일(이전 기준일) 데이터가 없어 변경 비교가 제한됩니다.")
    change_counts = today_all["변경상태"].value_counts().reindex(["지연", "개선", "유지", "신규"]).fillna(0).astype(int)
    st.bar_chart(change_counts)
    st.dataframe(today_all[today_all["변경상태"].isin(["지연", "개선"])].sort_values("포장변경일수", ascending=False), use_container_width=True)

with tabs[2]:
    st.subheader("공정 병목 분석")
    top = (
        today_all[today_all["병목공정"] != "없음"]["병목공정"]
        .value_counts()
        .sort_values(ascending=False)
        .head(10)
    )
    if len(top) == 0:
        st.info("병목으로 판단된 공정이 없습니다(전일 대비 지연 없음).")
    else:
        st.bar_chart(top)
    st.dataframe(today_all.sort_values("병목변경일수", ascending=False), use_container_width=True)

with tabs[3]:
    st.subheader("리스크 수주 리스트")
    risky = today_all[(today_all["상태"].isin(["지연", "긴급"])) | (today_all["변경상태"] == "지연")].copy()
    risky = risky.sort_values(["상태", "납기차이일", "포장변경일수"], ascending=[True, False, False])
    st.dataframe(risky, use_container_width=True)
    st.download_button(
        "CSV 다운로드(필터 반영)",
        data=table.to_csv(index=False, encoding="utf-8-sig"),
        file_name=f"risk_orders_{asof.isoformat()}.csv",
        mime="text/csv",
    )

with tabs[4]:
    st.subheader("S관 생산실적 현황(간편)")
    st.caption("컨셉: 전월은 월 전체 / 당월은 기준일-1(전일)까지 집계(당일 제외).")

    if prod_df.empty:
        st.info("생산실적 데이터가 없습니다.")
    else:
        views = build_cutoff_views(prod_df, asof=asof)
        st.caption(f"기준일: {views.asof.isoformat()} / 집계 cutoff: {views.cutoff.isoformat()}")

        def _sum_qty(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(pd.to_numeric(df[PROD_COLS.생산수량], errors="coerce").fillna(0).sum())

        def _n_days(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(df[PROD_COLS.생산일자].nunique())

        def _n_items(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(df[PROD_COLS.품목코드].nunique())

        prev_total = _sum_qty(views.prev_month.df)
        curr_total = _sum_qty(views.curr_month.df)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("전월 생산수량", f"{prev_total:,}")
        m2.metric("당월 생산수량(MTD)", f"{curr_total:,}")
        m3.metric("당월 생산일수", f"{_n_days(views.curr_month.df):,}")
        m4.metric("당월 품목수", f"{_n_items(views.curr_month.df):,}")

        left, right = st.columns(2)

        with left:
            st.markdown(f"**전월 ({views.prev_month.start.isoformat()} ~ {views.prev_month.end.isoformat()})**")
            prev_proc = summarize_by_process(views.prev_month.df)
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
                curr_proc = summarize_by_process(views.curr_month.df)
                if curr_proc.empty:
                    st.info("당월 실적이 없습니다(당일 제외/데이터를 확인하세요).")
                else:
                    st.bar_chart(curr_proc.set_index(PROD_COLS.공정)[PROD_COLS.생산수량])
                    st.dataframe(curr_proc, use_container_width=True, hide_index=True)

                daily = summarize_daily_total(views.curr_month.df)
                if not daily.empty:
                    st.markdown("**당월 일자별 합계**")
                    st.line_chart(daily.set_index(PROD_COLS.생산일자)[PROD_COLS.생산수량])

        with st.expander("원천 데이터 보기(전월+당월, cutoff 반영)"):
            st.dataframe(
                pd.concat([views.prev_month.df, views.curr_month.df], ignore_index=True)
                .sort_values([PROD_COLS.생산일자, PROD_COLS.공정, PROD_COLS.품목코드]),
                use_container_width=True,
            )
