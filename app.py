from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from risk_dashboard.io import default_data_paths, load_production_actuals
from risk_dashboard.logging_utils import get_logger
from risk_dashboard.production import month_end, month_start, prev_month_range
from risk_dashboard.production import build_views_with_ranges
from risk_dashboard.master_products import filter_sgwan, load_master_table
from risk_dashboard.schema import PROD_COLS, PROD_REQUIRED_COLS


st.set_page_config(page_title="S관 생산실적 대시보드", layout="wide", initial_sidebar_state="collapsed")
st.title("S관 생산실적 대시보드")

paths = default_data_paths("data")
logger = get_logger("streamlit.app", log_file="logs/streamlit_app.log")

st.markdown(
    """
<style>
  .stApp { background: #f7f2ea; }
  /* 상단 헤더 색상(다른 대시보드처럼) */
  header[data-testid="stHeader"] {
    background: #f7f2ea;
    border-bottom: 1px solid rgba(0,0,0,0.06);
  }
  /* 상단 우측 툴바/메뉴 배경도 투명 처리 */
  div[data-testid="stToolbar"] { background: transparent; }
  div[data-testid="stDecoration"] { background: transparent; }
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


def _excel_upload_time_kst(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        import openpyxl  # type: ignore[import-not-found]

        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        dt = wb.properties.modified or wb.properties.created
        wb.close()
        if dt is None:
            return None
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        else:
            dt = dt.astimezone(ZoneInfo("Asia/Seoul"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # fallback (may reflect deploy/copy time on some environments)
        try:
            dt = datetime.fromtimestamp(path.stat().st_mtime, tz=ZoneInfo("Asia/Seoul"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None


@st.cache_data(show_spinner=False)
def load_order_status_sgwan(
    order_xlsx: str,
    master_xlsx: str,
    *,
    sheet_name: str = "data",
    header_row_1based: int = 2,
    item_name_col: str = "품명",
    master_name_col: str = "제품명",
    cache_bust: float | None = None,
) -> pd.DataFrame:
    _ = cache_bust
    op = Path(order_xlsx)
    mp = Path(master_xlsx)
    if not op.exists():
        raise FileNotFoundError(str(op))
    if not mp.exists():
        raise FileNotFoundError(str(mp))

    raw = pd.read_excel(op, sheet_name=sheet_name, header=None)  # type: ignore[call-arg]
    if raw.empty:
        return pd.DataFrame()
    header_idx = max(0, header_row_1based - 1)
    if header_idx >= len(raw):
        raise ValueError(f"header_row out of range: {header_row_1based} (rows={len(raw)})")
    header = raw.iloc[header_idx].tolist()
    df = raw.iloc[header_idx + 1 :].copy()
    df.columns = [str(c).strip() for c in header]
    df = df.dropna(axis=1, how="all").copy()

    if item_name_col not in df.columns:
        raise ValueError(f"입력에 품명 컬럼이 없습니다: {item_name_col} (컬럼={list(df.columns)})")

    mdf = load_master_table(mp)
    mdf_s = filter_sgwan(mdf)
    if master_name_col not in mdf_s.columns:
        raise ValueError(f"마스터에 제품명 컬럼이 없습니다: {master_name_col} (컬럼={list(mdf_s.columns)})")

    master_names = {_norm_text(x) for x in mdf_s[master_name_col].tolist() if _norm_text(x)}
    summary_col = "분류요약" if "분류요약" in mdf_s.columns else None
    master_summary_map: dict[str, str] = {}
    if summary_col:
        for _, row in mdf_s[[master_name_col, summary_col]].iterrows():
            n = _norm_text(row.get(master_name_col))
            s = _norm_text(row.get(summary_col))
            if n and n not in master_summary_map:
                master_summary_map[n] = s
    if not master_names:
        return pd.DataFrame()

    df2 = df.copy()
    df2["_품명_norm"] = df2[item_name_col].map(_norm_text)
    out = df2[df2["_품명_norm"].isin(master_names)].copy()
    if master_summary_map:
        out["분류요약"] = out["_품명_norm"].map(master_summary_map).fillna("")
    out = out.drop(columns=["_품명_norm"], errors="ignore")
    return out


@st.cache_data(show_spinner=False)
def load_order_status_raw(
    order_xlsx: str,
    *,
    sheet_name: str = "data",
    header_row_1based: int = 2,
    cache_bust: float | None = None,
) -> pd.DataFrame:
    _ = cache_bust
    op = Path(order_xlsx)
    if not op.exists():
        raise FileNotFoundError(str(op))

    raw = pd.read_excel(op, sheet_name=sheet_name, header=None)  # type: ignore[call-arg]
    if raw.empty:
        return pd.DataFrame()
    header_idx = max(0, header_row_1based - 1)
    if header_idx >= len(raw):
        raise ValueError(f"header_row out of range: {header_row_1based} (rows={len(raw)})")
    header = raw.iloc[header_idx].tolist()
    df = raw.iloc[header_idx + 1 :].copy()
    df.columns = [str(c).strip() for c in header]
    df = df.dropna(axis=1, how="all").copy()
    return df


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
            PROD_COLS.신규분류요약: raw.get("신규분류요약", pd.Series([None] * len(raw))).map(_norm_text),
        }
    )
    out = out.dropna(subset=[PROD_COLS.생산일자, PROD_COLS.생산수량]).copy()
    out = out[out[PROD_COLS.품목코드].astype(str).str.len() > 0].copy()
    out[PROD_COLS.생산수량] = out[PROD_COLS.생산수량].astype(int)
    out[PROD_COLS.양품수량] = pd.to_numeric(out[PROD_COLS.양품수량], errors="coerce").fillna(0).astype(int)
    cols = PROD_REQUIRED_COLS + [PROD_COLS.양품수량]
    if PROD_COLS.신규분류요약 in out.columns:
        cols.append(PROD_COLS.신규분류요약)
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

    with st.expander("수주현황 데이터", expanded=False):
        order_book_xlsx = st.text_input("수주현황 포함 엑셀", value=str(default_excel))
        order_sheet = st.text_input("수주현황 시트명", value="수주현황")
        master_xlsx = st.text_input("S관 제품 마스터", value="S관 생산 제품 리스트.xlsx")

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

tabs = st.tabs(["S관 실적", "S관 수주현황"])

with tabs[0]:
    st.subheader("S관 생산실적 현황(간편)")
    # 컨셉 문구는 숨김(요청사항)

    if prod_df.empty:
        st.info("생산실적 데이터가 없습니다.")
    else:
        cutoff = asof - timedelta(days=1)

        # 업로드(원천 엑셀 파일 수정시간 기준) 표시
        excel_path = Path(prod_xlsx)
        uploaded_at = _excel_upload_time_kst(excel_path)
        if uploaded_at:
            st.caption(f"생산실적 업로드 시간 : {uploaded_at} (한국시간)")

        # 전월/당월은 월 단위 고정(당월은 cutoff=전일까지만)
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

        curr_start = curr_start_default
        curr_end = curr_end_default

        # 전월/당월 범위 집계
        # - 전월: 월 전체
        # - 당월: 월 시작 ~ cutoff(전일) 고정

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

        def _category_final_and_comp_yield(df: pd.DataFrame, *, asof: date) -> pd.DataFrame:
            if df.empty or PROD_COLS.신규분류요약 not in df.columns:
                return pd.DataFrame(columns=[PROD_COLS.신규분류요약, "생산수량", "양품수량", "종합수율"])

            gross_col = PROD_COLS.생산수량
            good_col = PROD_COLS.양품수량 if PROD_COLS.양품수량 in df.columns else gross_col
            final_proc = "누수/규격검사"

            base = df.copy()
            base[PROD_COLS.신규분류요약] = base[PROD_COLS.신규분류요약].map(_norm_text)
            base = base[base[PROD_COLS.신규분류요약].astype(str).str.len() > 0].copy()
            if base.empty:
                return pd.DataFrame(columns=[PROD_COLS.신규분류요약, "생산수량", "양품수량", "종합수율"])

            # final quantities (누수/규격검사 기준)
            final_df = base[base[PROD_COLS.공정].astype(str) == final_proc].copy()
            final_sum = (
                final_df.groupby(PROD_COLS.신규분류요약, dropna=False)[[gross_col, good_col]]
                .sum()
                .rename(columns={gross_col: "생산수량", good_col: "양품수량"})
            )

            # composite yield per category = product of per-process yields
            proc_sum = (
                base.groupby([PROD_COLS.신규분류요약, PROD_COLS.공정], dropna=False)[[gross_col, good_col]]
                .sum()
                .reset_index()
            )
            proc_sum["수율"] = proc_sum.apply(
                lambda r: (r[good_col] / r[gross_col]) if float(r[gross_col]) > 0 else None,
                axis=1,
            )

            # pivot to ensure all processes exist
            yield_map = proc_sum.pivot_table(
                index=PROD_COLS.신규분류요약,
                columns=PROD_COLS.공정,
                values="수율",
                aggfunc="first",
            )

            comp_vals: dict[str, float] = {}
            for cat, row in yield_map.iterrows():
                ys: list[float] = []
                for proc in process_order:
                    y = row.get(proc)
                    if y is None or pd.isna(y):
                        continue
                    ys.append(float(y))
                if not ys:
                    continue
                prod_val = 1.0
                for y in ys:
                    prod_val *= y
                comp_vals[str(cat)] = float(prod_val)

            comp_series = pd.Series(comp_vals, name="종합수율")

            out = final_sum.join(comp_series, how="left").reset_index()
            out["생산수량"] = pd.to_numeric(out["생산수량"], errors="coerce").fillna(0).astype(int)
            out["양품수량"] = pd.to_numeric(out["양품수량"], errors="coerce").fillna(0).astype(int)
            out = out.sort_values("양품수량", ascending=False).reset_index(drop=True)
            return out[[PROD_COLS.신규분류요약, "생산수량", "양품수량", "종합수율"]].copy()

        def _forecast_eom_output_mtd(df: pd.DataFrame, *, asof: date) -> int | None:
            # Forecast end-of-month output using final-process good qty MTD.
            # - MTD uses cutoff (asof-1) already in views.curr_month.df
            # - average per observed production day (final process) * estimated remaining production days
            if df.empty:
                return None

            gross_col = PROD_COLS.생산수량
            good_col = PROD_COLS.양품수량 if PROD_COLS.양품수량 in df.columns else gross_col
            final_proc = "누수/규격검사"

            cutoff_local = asof - timedelta(days=1)
            ms = month_start(asof)
            me = month_end(asof)
            if cutoff_local < ms:
                return 0

            final_df = df[df[PROD_COLS.공정].astype(str) == final_proc].copy()
            if final_df.empty:
                return 0

            mtd_qty = int(pd.to_numeric(final_df[good_col], errors="coerce").fillna(0).sum())
            prod_days = int(pd.to_datetime(final_df[PROD_COLS.생산일자], errors="coerce").dt.date.nunique())
            if prod_days <= 0:
                return None

            avg_per_day = mtd_qty / prod_days

            elapsed_days = (cutoff_local - ms).days + 1
            if elapsed_days <= 0:
                return None

            # Estimate remaining production days by observed ratio of production-days to elapsed calendar-days.
            prod_day_ratio = min(1.0, max(0.0, prod_days / elapsed_days))
            remaining_days = max(0, (me - cutoff_local).days)
            remaining_prod_days_est = int(round(remaining_days * prod_day_ratio))
            forecast = int(round(mtd_qty + avg_per_day * remaining_prod_days_est))
            return forecast

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

            st.markdown("**신규분류요약별**")
            prev_cat = _category_final_and_comp_yield(views.prev_month.df, asof=asof)
            if prev_cat.empty:
                st.info("신규분류요약 데이터가 없어 그룹 요약을 표시할 수 없습니다.")
            else:
                st.dataframe(
                    prev_cat.style.format({"생산수량": "{:,.0f}", "양품수량": "{:,.0f}", "종합수율": "{:.1%}"}),
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
            k1, k2, k3 = st.columns(3)
            k1.metric("총 생산실적(누수/규격검사 양품)", f"{curr_total:,}")
            k2.metric("종합 수율", f"{curr_comp*100:.1f}%" if curr_comp is not None else "-")
            curr_forecast = _forecast_eom_output_mtd(views.curr_month.df, asof=asof)
            k3.metric("월말 예상(누수/규격검사 양품)", f"{curr_forecast:,}" if curr_forecast is not None else "-")
            st.dataframe(
                curr_proc.style.format({"생산수량": "{:,.0f}", "양품수량": "{:,.0f}", "수율": "{:.1%}"}),
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("**신규분류요약별**")
            curr_cat = _category_final_and_comp_yield(views.curr_month.df, asof=asof)
            if curr_cat.empty:
                st.info("신규분류요약 데이터가 없어 그룹 요약을 표시할 수 없습니다.")
            else:
                st.dataframe(
                    curr_cat.style.format({"생산수량": "{:,.0f}", "양품수량": "{:,.0f}", "종합수율": "{:.1%}"}),
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

with tabs[1]:
    st.subheader("S관 수주현황(월별 집계)")
    st.caption("수주현황 시트의 품명을 S관 제품 마스터(제품명)와 매칭한 행만 집계합니다.")

    try:
        order_mtime = Path(order_book_xlsx).stat().st_mtime if Path(order_book_xlsx).exists() else None
        orders_raw = load_order_status_raw(order_book_xlsx, sheet_name=order_sheet, cache_bust=order_mtime)
        orders_s = load_order_status_sgwan(
            order_book_xlsx,
            master_xlsx,
            sheet_name=order_sheet,
            cache_bust=order_mtime,
        )
    except Exception as e:
        st.error(f"수주현황 로드 실패: {e}")
        orders_raw = pd.DataFrame()
        orders_s = pd.DataFrame()

    if orders_s.empty:
        # If raw exists but filtered is empty, most likely sheet/headers mismatch or master matching fails.
        msg = "S관 수주현황 데이터가 없습니다(매칭 결과 0행이거나 파일/시트/컬럼을 확인하세요)."
        try:
            p = Path(order_book_xlsx)
            if p.exists():
                xl = pd.ExcelFile(p)
                if order_sheet not in xl.sheet_names:
                    msg = f"'{order_sheet}' 시트가 없습니다. 현재 시트: {xl.sheet_names}"
        except Exception:
            pass
        st.info(msg)
    else:
        if not orders_raw.empty:
            st.caption(f"S관 제품 매칭 행수: {len(orders_s):,} / 전체 행수: {len(orders_raw):,}")
        df = orders_s.copy()

        # Parse month
        if "__month_date__" in df.columns:
            m = pd.to_datetime(df["__month_date__"], errors="coerce")
            df["월"] = m.dt.to_period("M").dt.strftime("%Y-%m")
            if "연도" not in df.columns:
                df["연도"] = m.dt.year
        else:
            df["월"] = df.get("월", pd.Series([""] * len(df))).astype("string")

        # Normalize numeric columns
        for col in ["오더수량", "수주금액", "수주금액(원)", "수주금액(달러)", "포장 진도율"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Normalize date columns for view
        for col in ["수주 전송일", "영업출고요청일", "영업협의출고일", "포장완료일"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        # ===== 월별 집계 =====
        with st.container(border=True):
            st.markdown("**월별 집계**")

            group_cols = ["월"]
            if "분류요약" in df.columns:
                group_cols.append("분류요약")
            monthly_agg = {}
            if "작지번호" in df.columns:
                monthly_agg["작지건수"] = ("작지번호", "nunique")
            if "오더수량" in df.columns:
                monthly_agg["오더수량 합계"] = ("오더수량", "sum")
            if "수주금액(원)" in df.columns:
                monthly_agg["수주금액(원) 합계"] = ("수주금액(원)", "sum")
            if "수주금액(달러)" in df.columns:
                monthly_agg["수주금액(달러) 합계"] = ("수주금액(달러)", "sum")

            monthly = df.groupby(group_cols, dropna=False).agg(**monthly_agg).reset_index()
            monthly = monthly.sort_values(group_cols).reset_index(drop=True)
            # 합계 행 추가(월별 집계용)
            total_row: dict[str, object] = {c: "" for c in group_cols}
            total_row["월"] = "합계"
            for c in ["작지건수", "오더수량 합계", "수주금액(원) 합계", "수주금액(달러) 합계"]:
                if c in monthly.columns:
                    total_row[c] = float(monthly[c].sum()) if c != "작지건수" else int(monthly[c].sum())
            monthly2 = pd.concat([monthly, pd.DataFrame([total_row])], ignore_index=True)
            fmt = {c: "{:,.0f}" for c in monthly2.columns if c not in group_cols}
            st.dataframe(
                monthly2.style.format(fmt),
                use_container_width=True,
                hide_index=True,
            )

        # ===== 원천(필터) =====
        with st.container(border=True):
            st.markdown("**상세 수주 내역**")

        df_view = df.copy()

        show_cols = [
            c
            for c in [
                "월",
                "분류요약",
                "구분",
                "작지번호",
                "고객",
                "담당자",
                "국가",
                "오더수량",
                "수주금액",
                "화폐",
                "수주금액(원)",
                "수주금액(달러)",
                "수주 전송일",
                "영업출고요청일",
                "현재상태",
                "포장 진도율",
                "포장완료일",
            ]
            if c in df_view.columns
        ]
        df_view = df_view.sort_values(["월", "구분", "작지번호"], na_position="last") if all(
            c in df_view.columns for c in ["월", "구분", "작지번호"]
        ) else df_view

        fmt = {}
        for col in ["오더수량", "수주금액", "수주금액(원)", "수주금액(달러)"]:
            if col in df_view.columns:
                fmt[col] = "{:,.0f}"
        if "포장 진도율" in df_view.columns:
            fmt["포장 진도율"] = "{:,.1f}"

        st.dataframe(
            df_view[show_cols].style.format(fmt),
            use_container_width=True,
            hide_index=True,
        )
