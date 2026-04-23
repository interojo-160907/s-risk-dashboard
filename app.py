from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

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
def load_prod_from_excel(prod_xlsx: str, *, status: str = "확인") -> pd.DataFrame:
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
            prod_df = load_prod_from_excel(prod_xlsx, status="확인")
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

    if prod_df.empty:
        st.info("생산실적 데이터가 없습니다.")
    else:
        cutoff = asof - timedelta(days=1)
        st.caption(f"기준일: {asof.isoformat()} / 집계 cutoff(당일 제외): {cutoff.isoformat()}")

        # 업데이트 시간(파일 수정시간 기준)
        try:
            src_path = Path(prod_xlsx) if source == "엑셀" else Path(prod_path)
            if src_path.exists():
                dt = datetime.fromtimestamp(src_path.stat().st_mtime, tz=ZoneInfo("Asia/Seoul"))
                st.caption(f"업데이트(파일 수정시간): {dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        except Exception:
            pass

        with st.container(border=True):
            # 공정 선택(일자별 차트/표에 적용) - KPI는 누수 기준 고정
            view_proc = st.pills(
                "공정",
                options=["사출조립", "분리", "하이드레이션/전면검", "접착/멸균", "누수/규격검사"],
                default="누수/규격검사",
                selection_mode="single",
                label_visibility="collapsed",
            )

            # 기간조회(해제/직접/당월/+7일/+14일)
            preset = st.pills(
                "기간조회",
                options=["해제", "직접", "당월", "+7일", "+14일"],
                default=st.session_state.get("range_preset", "당월"),
                selection_mode="single",
                label_visibility="collapsed",
            )

            if preset == "해제":
                st.session_state["range_preset"] = "당월"
                for k in ["prev_start", "prev_end", "curr_start", "curr_end"]:
                    st.session_state.pop(k, None)
                preset = "당월"
            else:
                st.session_state["range_preset"] = preset

            prev_start = prev_end = curr_start = curr_end = None
            if preset == "직접":
                if "prev_start" not in st.session_state:
                    p0, p1 = prev_month_range(asof)
                    st.session_state["prev_start"] = p0
                    st.session_state["prev_end"] = p1
                if "curr_start" not in st.session_state:
                    c0 = month_start(asof)
                    st.session_state["curr_start"] = c0
                    st.session_state["curr_end"] = cutoff if cutoff >= c0 else c0

                left, right = st.columns(2)
                with left:
                    prev_start = st.date_input("전월 시작", key="prev_start")
                    prev_end = st.date_input("전월 종료", key="prev_end")
                with right:
                    curr_start = st.date_input("당월 시작", key="curr_start")
                    curr_end = st.date_input("당월 종료", key="curr_end")
            elif preset == "당월":
                prev_start, prev_end = prev_month_range(asof)
                curr_start = month_start(asof)
                curr_end = cutoff
            elif preset == "+7일":
                curr_end = cutoff
                curr_start = cutoff - timedelta(days=6)
                prev_end = curr_start - timedelta(days=1)
                prev_start = prev_end - timedelta(days=6)
            elif preset == "+14일":
                curr_end = cutoff
                curr_start = cutoff - timedelta(days=13)
                prev_end = curr_start - timedelta(days=1)
                prev_start = prev_end - timedelta(days=13)

            # end가 cutoff를 넘으면 클리핑
            if prev_start and prev_end and prev_end > cutoff:
                prev_end = cutoff
            if curr_start and curr_end and curr_end > cutoff:
                curr_end = cutoff

        views = build_views_with_ranges(
            prod_df,
            asof=asof,
            prev_start=prev_start,
            prev_end=prev_end,
            curr_start=curr_start,
            curr_end=curr_end,
        )

        def _sum_qty(df: pd.DataFrame, col: str) -> int:
            if df.empty:
                return 0
            if col not in df.columns:
                return 0
            return int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())

        def _yield_table(df: pd.DataFrame, *, process_order: list[str]) -> tuple[pd.DataFrame, float | None]:
            if df.empty:
                empty = pd.DataFrame(columns=["공정", "생산수량", "양품수량", "수율"])
                return empty, None

            gross_col = PROD_COLS.생산수량
            good_col = PROD_COLS.양품수량 if PROD_COLS.양품수량 in df.columns else gross_col

            rows: list[dict[str, object]] = []
            yields_order: list[float | None] = []
            for proc in process_order:
                sub = df[df[PROD_COLS.공정].astype(str) == proc].copy()
                gross = _sum_qty(sub, gross_col)
                good = _sum_qty(sub, good_col)
                y = (good / gross) if gross > 0 else None
                yields_order.append(float(y) if y is not None else None)
                rows.append(
                    {
                        "공정": proc,
                        "생산수량": f"{gross:,}",
                        "양품수량": f"{good:,}",
                        "수율": (f"{y*100:.1f}%" if y is not None else ""),
                    }
                )

            total_gross = _sum_qty(df, gross_col)
            total_good = _sum_qty(df, good_col)
            rows.append({"공정": "TOTAL", "생산수량": f"{total_gross:,}", "양품수량": f"{total_good:,}", "수율": ""})

            comp = None
            if yields_order and all(y is not None for y in yields_order):
                comp_val = 1.0
                for y in yields_order:
                    comp_val *= float(y)
                comp = float(comp_val)

            return pd.DataFrame(rows), comp

        def _n_days(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(df[PROD_COLS.생산일자].nunique())

        def _n_items(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            return int(df[PROD_COLS.품목코드].nunique())

        process_order = ["사출조립", "분리", "하이드레이션/전면검", "접착/멸균", "누수/규격검사"]
        final_proc = "누수/규격검사"
        gross_col = PROD_COLS.생산수량
        good_col = PROD_COLS.양품수량 if PROD_COLS.양품수량 in views.curr_month.df.columns else gross_col

        def _final_output(df: pd.DataFrame) -> int:
            if df.empty:
                return 0
            good_col_local = PROD_COLS.양품수량 if PROD_COLS.양품수량 in df.columns else gross_col
            sub = df[df[PROD_COLS.공정].astype(str) == final_proc].copy()
            return _sum_qty(sub, good_col_local)

        left, right = st.columns(2)
        with left:
            with st.container(border=True):
                st.markdown(f"**전월 ({views.prev_month.start.isoformat()} ~ {views.prev_month.end.isoformat()})**")
                prev_table, prev_comp = _yield_table(views.prev_month.df, process_order=process_order)
                top1, top2 = st.columns(2)
                top1.metric("총 생산수량", f"{_final_output(views.prev_month.df):,}")
                top2.metric("종합 수율", f"{prev_comp*100:.1f}%" if prev_comp is not None else "-")
                st.caption("※ 각 공정 수율 곱 = 종합수율")
                st.dataframe(prev_table, use_container_width=True, hide_index=True)

        with right:
            with st.container(border=True):
                st.markdown(f"**당월 ({views.curr_month.start.isoformat()} ~ {views.curr_month.end.isoformat()})**")
                curr_table, curr_comp = _yield_table(views.curr_month.df, process_order=process_order)
                top1, top2 = st.columns(2)
                top1.metric("총 생산수량", f"{_final_output(views.curr_month.df):,}")
                top2.metric("종합 수율", f"{curr_comp*100:.1f}%" if curr_comp is not None else "-")
                st.caption("※ 각 공정 수율 곱 = 종합수율")
                st.dataframe(curr_table, use_container_width=True, hide_index=True)

                daily = summarize_daily_total(
                    views.curr_month.df[views.curr_month.df[PROD_COLS.공정].astype(str) == str(view_proc)].copy()
                )
                if not daily.empty:
                    st.markdown(f"**일자별 합계({view_proc})**")
                    st.line_chart(daily.set_index(PROD_COLS.생산일자)[gross_col])

        with st.expander("원천 데이터 보기(전월+당월, cutoff 반영)"):
            st.dataframe(
                pd.concat([views.prev_month.df, views.curr_month.df], ignore_index=True)
                .sort_values([PROD_COLS.생산일자, PROD_COLS.공정, PROD_COLS.품목코드]),
                use_container_width=True,
            )
