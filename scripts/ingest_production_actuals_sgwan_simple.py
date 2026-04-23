from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk_dashboard.logging_utils import get_logger  # noqa: E402
from risk_dashboard.schema import PROD_COLS, PROD_REQUIRED_COLS  # noqa: E402


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


def _load_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name)  # type: ignore[call-arg]
    return df


def main() -> int:
    logger = get_logger("ingest.production_simple", log_file="logs/ingest_production_simple.log")
    ap = argparse.ArgumentParser(
        description="생산실적현황(간편)_S관.xlsx(전월/당월) → production_actuals(rolling 2개월) 생성"
    )
    ap.add_argument("--input", default="생산실적현황(간편)_S관.xlsx", help="엑셀 경로")
    ap.add_argument("--sheets", default="전월,당월", help="처리할 시트(쉼표구분)")
    ap.add_argument("--status", default="확인", help="상태 필터(기본: 확인)")
    ap.add_argument("--good-col", default="샘플제외 양품수량", help="양품(실적) 컬럼(기본: 샘플제외 양품수량)")
    ap.add_argument("--prod-col", default="생산수량", help="생산(총)수량 컬럼(기본: 생산수량)")
    ap.add_argument("--output", default="data/production_actuals_recent.csv", help="출력 CSV 경로")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        logger.error("missing file: %s", in_path)
        raise FileNotFoundError(str(in_path))

    logger.info(
        "start | input=%s | sheets=%s | status=%s | good_col=%s | prod_col=%s | output=%s",
        in_path,
        args.sheets,
        args.status,
        args.good_col,
        args.prod_col,
        args.output,
    )

    sheets = [s.strip() for s in str(args.sheets).split(",") if s.strip()]
    frames: list[pd.DataFrame] = []
    for sh in sheets:
        df = _load_sheet(in_path, sh)
        frames.append(df)
    raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if raw.empty:
        logger.info("no data: empty sheets")
        print("NO DATA: empty sheets")
        return 0

    good_col = str(args.good_col)
    prod_col = str(args.prod_col)
    if good_col not in raw.columns and "양품수량" in raw.columns:
        good_col = "양품수량"

    required = ["생산일자", "공정코드", "품목코드", prod_col, good_col, "상태"]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        logger.error("missing columns: %s", missing)
        raise ValueError(f"필수 컬럼 누락: {missing} (현재 컬럼: {list(raw.columns)})")

    raw["상태_norm"] = raw["상태"].map(_norm_text)
    if args.status:
        raw = raw[raw["상태_norm"] == _norm_text(args.status)].copy()
    logger.info("after status filter | rows=%s", len(raw))

    out = pd.DataFrame(
        {
            PROD_COLS.생산일자: pd.to_datetime(raw["생산일자"], errors="coerce").dt.date,
            PROD_COLS.공정: raw["공정코드"].map(_map_process),
            PROD_COLS.품목코드: raw["품목코드"].map(_norm_text),
            PROD_COLS.생산수량: pd.to_numeric(raw[prod_col], errors="coerce"),
            PROD_COLS.양품수량: pd.to_numeric(raw[good_col], errors="coerce"),
        }
    )

    out = out.dropna(subset=[PROD_COLS.생산일자, PROD_COLS.생산수량]).copy()
    out = out[out[PROD_COLS.품목코드].astype(str).str.len() > 0].copy()
    out[PROD_COLS.생산수량] = out[PROD_COLS.생산수량].astype(int)
    if PROD_COLS.양품수량 in out.columns:
        out[PROD_COLS.양품수량] = pd.to_numeric(out[PROD_COLS.양품수량], errors="coerce").fillna(0).astype(int)

    # 결과 저장(rolling 2개월 파일이므로 overwrite)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = PROD_REQUIRED_COLS + ([PROD_COLS.양품수량] if PROD_COLS.양품수량 in out.columns else [])
    out[cols].to_csv(out_path, index=False, encoding="utf-8-sig")

    logger.info(
        "write done | rows=%s | out=%s | process_counts=%s",
        len(out),
        out_path,
        out[PROD_COLS.공정].value_counts().to_dict(),
    )
    print(f"OK: wrote {len(out):,} rows -> {out_path}")
    print("process_counts:", out[PROD_COLS.공정].value_counts().to_dict())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
