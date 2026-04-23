from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from risk_dashboard.io import default_data_paths, load_production_actuals
from risk_dashboard.logging_utils import get_logger
from risk_dashboard.production import month_start, prev_month_range
from risk_dashboard.production import build_views_with_ranges
from risk_dashboard.schema import PROD_COLS, PROD_REQUIRED_COLS


st.set_page_config(page_title="S관 생산실적 대시보드", layout="wide", initial_sidebar_state="collapsed")
st.title("S관 생산실적 대시보드")

paths = default_data_paths("data")
logger = get_logger("streamlit.app", log_file="logs/streamlit_app.log")

st.markdown(
    """
<style>
  .stApp { background: #f7f2ea; }
  /* pills/segment buttons 느낌 */
  .stButton > button { border-radius: 999px; padding: 0.28rem 0.9rem; border: 1px solid rgba(0,0,0,0.18); background: #fff; }
  .stButton > button:hover { border-color: rgba(0,0,0,0.35); }
  div[data-testid="stVerticalBlockBorderWrapper"] { background: rgba(255,255,255,0.55); border-radius: 14px; border: 1px solid rgba(0,0,0,0.06); }
  div[data-testid="stVerticalBlockBorderWrapper"] > div { padding: 0.6rem 0.9rem; }
  /* pills 선택 강조(버전별 DOM 차이가 있어 범용 selector로 커버) */
  div[data-testid="stPills"] [aria-selected="true"] { background: rgba(46,125,50,0.16) !important; border-color: rgba(46,125,50,0.55) !important; color: #1b5e20 !important; }
  div[data-testid="stPills"] [aria-selected="false"] { background: #fff !important; }
</style>
""",
    unsafe_allow_html=True,
)

# 기본 사이드바 접기


def _norm_text(x: object) -> str:
    if pd.isna(x):
        return ""
    return " ".join(str(x).strip().split())


def _map_process(code: object) -> str:
    s = _norm_text(code)
    if s.startswith("[10]"):
        return "사출조립"
    if s.startswith("[20]"):
        return "분리"
    if s.startswith("[45]"):
        return "하이드레이션/전면검"
    if s.startswith("[55]"):
        return "접착/멸균"
    if s.startswith("[80]"):
        return "누수/규격검사"
    if s.startswith("[85]"):
        return "포장"
    return s


@st.cache_data(show_spinner=False)
def load_prod_from_excel(prod_xlsx: str, *, status: str = "확인", cache_bust: float | None = None) -> pd.DataFrame:
    # cache_bust: 파일 수정시간 등을 넘겨 캐시가 파일 변경을 감지하도록 함.
    _ = cache_bust
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
    # prefer 샘플 제외 양품수량(있으면) else 양품수량
    good_col = "샘플제외 양품수량" if "샘플제외 양품수량" in raw.columns else "양품수량"
    required = ["생산일자", "공정코드", "품목코드", "생산수량", good_col, "상태"]
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
            PROD_COLS.생산수량: pd.to_numeric(raw["생산수량"], errors="coerce"),
            PROD_COLS.양품수량: pd.to_numeric(raw[good_col], errors="coerce"),
        }
    )
    out = out.dropna(subset=[PROD_COLS.생산일자, PROD_COLS.생산수량]).copy()
    out = out[out[PROD_COLS.품목코드].astype(str).str.len() > 0].copy()
    out[PROD_COLS.생산수량] = out[PROD_COLS.생산수량].astype(int)
    out[PROD_COLS.양품수량] = pd.to_numeric(out[PROD_COLS.양품수량], errors="coerce").fillna(0).astype(int)
    cols = PROD_REQUIRED_COLS + [PROD_COLS.양품수량]
    return out[cols].copy()


with st.sidebar:
    st.header("데이터")
    default_prod = Path("data/production_actuals_recent.csv")
    default_excel = Path("생산실적현황(간편)_S관.xlsx")
    # 원천은 '생산실적현황(간편)_S관.xlsx'를 우선으로 사용(존재하면 기본값을 엑셀로).
    default_source_idx = 1 if default_excel.exists() else 0
    source = st.radio("소스", ["CSV", "엑셀"], index=default_source_idx, horizontal=True)

    if source == "CSV":
        prod_path = st.text_input(
            "생산실적 CSV",
            value=str(default_prod if default_prod.exists() else paths.production_actuals_csv),
        )
        st.caption("CSV 인코딩은 `utf-8-sig` 권장(엑셀 호환).")
    else:
        prod_xlsx = st.text_input("생산실적(간편)", value=str(default_excel))
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
            excel_mtime = Path(prod_xlsx).stat().st_mtime if Path(prod_xlsx).exists() else None
            prod_df = load_prod_from_excel(prod_xlsx, status="확인", cache_bust=excel_mtime)
        except Exception as e:
            logger.exception("excel->production failed")
            st.error(f"엑셀 → 생산실적 변환 실패: {e}")
            prod_df = pd.DataFrame(columns=PROD_REQUIRED_COLS)

def _normalize_process_names(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or PROD_COLS.공정 not in df.columns:
        return df
    m = {
        "사출": "사출조립",
        "하이드": "하이드레이션/전면검",
        "접착": "접착/멸균",
        "누수": "누수/규격검사",
    }
    out = df.copy()
    out[PROD_COLS.공정] = out[PROD_COLS.공정].map(lambda x: m.get(str(x), str(x)))
    return out

prod_df = _normalize_process_names(prod_df)

with st.sidebar:
    asof = st.date_input("기준일", value=date.today())

tabs = st.tabs(["S관 실적"])

with tabs[0]:
    st.subheader("S관 생산실적 현황(간편)")
    # 컨셉 문구는 숨김(요청사항)

    if prod_df.empty:
        st.info("생산실적 데이터가 없습니다.")
    else:
        cutoff = asof - timedelta(days=1)

        # 업로드(원천 엑셀 파일 수정시간 기준) 표시
        try:
            excel_path = Path(prod_xlsx)
            if excel_path.exists():
                dt = datetime.fromtimestamp(excel_path.stat().st_mtime, tz=ZoneInfo("Asia/Seoul"))
                st.caption(f"생산실적 업로드 시간 : {dt.strftime('%Y-%m-%d %H:%M:%S')} (한국시간)")
        except Exception:
            pass

        # 전월은 월 단위 고정, 당월만 기간 필터(전체 / 직접 선택)
        prev_start, prev_end = prev_month_range(asof)
        curr_start_default = month_start(asof)
        curr_end_default = cutoff

        left_col, right_col = st.columns(2)
        with right_col:
            right_card = st.container(border=True)
        with left_col:
            left_card = st.container(border=True)

        with right_card:
            st.markdown("**당월**")
            curr_filter_mode = st.radio(
                "당월 기간",
                options=["전체", "직접(며칠~며칠)"],
                index=0,
                horizontal=True,
                label_visibility="collapsed",
            )
            curr_start = curr_start_default
            curr_end = curr_end_default
            if curr_filter_mode.startswith("직접"):
                c1, c2 = st.columns(2)
                with c1:
                    curr_start = st.date_input("당월 시작", value=curr_start_default, key="curr_start_filter")
                with c2:
                    curr_end = st.date_input("당월 종료", value=curr_end_default, key="curr_end_filter")

                if curr_end > cutoff:
                    curr_end = cutoff
                if curr_start < curr_start_default:
                    curr_start = curr_start_default
                if curr_start > curr_end:
                    st.warning("당월 기간이 올바르지 않습니다(시작일 > 종료일).")

        # 전월/당월 범위 집계
        # - 전월: 월 전체
        # - 당월: 선택된 범위(기본=전체 MTD, cutoff까지)
        curr_start = curr_start if curr_start <= curr_end else curr_start_default
        curr_end = curr_end if curr_start <= curr_end else curr_end_default

        views = build_views_with_ranges(
            prod_df,
            asof=asof,
            prev_start=prev_start,
            prev_end=prev_end,
            curr_start=curr_start,
            curr_end=curr_end,
        )

        def _n_days(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(df[PROD_COLS.생산일자].nunique())

        def _n_items(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(df[PROD_COLS.품목코드].nunique())

        process_order = ["사출조립", "분리", "하이드레이션/전면검", "접착/멸균", "누수/규격검사"]

        def _process_summary(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=[PROD_COLS.공정, "생산수량", "양품수량", "수율"])

            gross_col = PROD_COLS.생산수량
            good_col = PROD_COLS.양품수량 if PROD_COLS.양품수량 in df.columns else gross_col
            g = (
                df.groupby(PROD_COLS.공정, dropna=False)[[gross_col, good_col]]
                .sum()
                .reset_index()
                .rename(columns={gross_col: "생산수량", good_col: "양품수량"})
            )
            g[PROD_COLS.공정] = pd.Categorical(g[PROD_COLS.공정].astype(str), categories=process_order, ordered=True)
            g = g.sort_values(PROD_COLS.공정).copy()
            g["생산수량"] = pd.to_numeric(g["생산수량"], errors="coerce").fillna(0).astype(int)
            g["양품수량"] = pd.to_numeric(g["양품수량"], errors="coerce").fillna(0).astype(int)
            g["수율"] = g.apply(lambda r: (r["양품수량"] / r["생산수량"]) if r["생산수량"] > 0 else None, axis=1)
            return g.reset_index(drop=True)

        def _daily_process_summary(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=[PROD_COLS.생산일자, PROD_COLS.공정, "생산수량", "양품수량", "수율"])

            gross_col = PROD_COLS.생산수량
            good_col = PROD_COLS.양품수량 if PROD_COLS.양품수량 in df.columns else gross_col

            out = (
                df.groupby([PROD_COLS.생산일자, PROD_COLS.공정], dropna=False)[[gross_col, good_col]]
                .sum()
                .reset_index()
                .rename(columns={gross_col: "생산수량", good_col: "양품수량"})
            )
            out[PROD_COLS.공정] = pd.Categorical(out[PROD_COLS.공정].astype(str), categories=process_order, ordered=True)
            out = out.sort_values([PROD_COLS.생산일자, PROD_COLS.공정]).copy()
            out["생산수량"] = pd.to_numeric(out["생산수량"], errors="coerce").fillna(0).astype(int)
            out["양품수량"] = pd.to_numeric(out["양품수량"], errors="coerce").fillna(0).astype(int)
            out["수율"] = out.apply(lambda r: (r["양품수량"] / r["생산수량"]) if r["생산수량"] > 0 else None, axis=1)
            return out.reset_index(drop=True)

        def _total_output_and_yield(df: pd.DataFrame) -> tuple[int, float | None]:
            if df.empty:
                return 0, None

            gross_col = PROD_COLS.생산수량
            good_col = PROD_COLS.양품수량 if PROD_COLS.양품수량 in df.columns else gross_col
            final_proc = "누수/규격검사"

            final_df = df[df[PROD_COLS.공정].astype(str) == final_proc].copy()
            total_output = int(pd.to_numeric(final_df[good_col], errors="coerce").fillna(0).sum()) if not final_df.empty else 0

            comp = None
            comp_val = 1.0
            for proc in process_order:
                sub = df[df[PROD_COLS.공정].astype(str) == proc].copy()
                gross = int(pd.to_numeric(sub[gross_col], errors="coerce").fillna(0).sum()) if not sub.empty else 0
                good = int(pd.to_numeric(sub[good_col], errors="coerce").fillna(0).sum()) if not sub.empty else 0
                if gross <= 0:
                    comp = None
                    break
                comp_val *= (good / gross)
                comp = float(comp_val)

            return total_output, comp

        with left_card:
            st.markdown("**전월**")
            st.caption(f"생산일수: {_n_days(views.prev_month.df):,} / 품목수: {_n_items(views.prev_month.df):,}")

            st.markdown("**공정별 요약**")
            prev_proc = _process_summary(views.prev_month.df)
            prev_total, prev_comp = _total_output_and_yield(views.prev_month.df)
            k1, k2 = st.columns(2)
            k1.metric("총 생산실적(누수/규격검사 양품)", f"{prev_total:,}")
            k2.metric("종합 수율", f"{prev_comp*100:.1f}%" if prev_comp is not None else "-")
            st.dataframe(
                prev_proc.style.format({"생산수량": "{:,.0f}", "양품수량": "{:,.0f}", "수율": "{:.1%}"}),
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("**일자별 집계**")
            prev_daily = _daily_process_summary(views.prev_month.df)
            st.dataframe(
                prev_daily.style.format({"생산수량": "{:,.0f}", "양품수량": "{:,.0f}", "수율": "{:.1%}"}),
                use_container_width=True,
                hide_index=True,
            )

        with right_card:
            st.caption(f"생산일수: {_n_days(views.curr_month.df):,} / 품목수: {_n_items(views.curr_month.df):,}")

            st.markdown("**공정별 요약**")
            curr_proc = _process_summary(views.curr_month.df)
            curr_total, curr_comp = _total_output_and_yield(views.curr_month.df)
            k1, k2 = st.columns(2)
            k1.metric("총 생산실적(누수/규격검사 양품)", f"{curr_total:,}")
            k2.metric("종합 수율", f"{curr_comp*100:.1f}%" if curr_comp is not None else "-")
            st.dataframe(
                curr_proc.style.format({"생산수량": "{:,.0f}", "양품수량": "{:,.0f}", "수율": "{:.1%}"}),
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("**일자별 집계**")
            curr_daily = _daily_process_summary(views.curr_month.df)
            st.dataframe(
                curr_daily.style.format({"생산수량": "{:,.0f}", "양품수량": "{:,.0f}", "수율": "{:.1%}"}),
                use_container_width=True,
                hide_index=True,
            )

        # 원천 데이터 보기 제거(요청사항)
