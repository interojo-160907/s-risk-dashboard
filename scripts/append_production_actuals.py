from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk_dashboard.io import append_csv, default_data_paths, read_input_table, validate_required_columns
from risk_dashboard.logging_utils import get_logger
from risk_dashboard.schema import PROD_REQUIRED_COLS


def main() -> int:
    logger = get_logger("append.production_actuals", log_file="logs/append_production_actuals.log")
    ap = argparse.ArgumentParser(description="생산실적(일별) CSV에 append")
    ap.add_argument("--input", required=True, help="생산실적 원본 파일 경로(.xlsx/.csv)")
    ap.add_argument("--data-dir", default="data", help="저장 폴더(기본: data)")
    args = ap.parse_args()
    logger.info("start | input=%s | data_dir=%s", args.input, args.data_dir)

    df = read_input_table(args.input).copy()
    validate_required_columns(df, PROD_REQUIRED_COLS, name="생산실적 입력")

    df["생산일자"] = pd.to_datetime(df["생산일자"], errors="coerce").dt.date
    df["생산수량"] = pd.to_numeric(df["생산수량"], errors="coerce")

    paths = default_data_paths(args.data_dir)
    append_csv(Path(paths.production_actuals_csv), df[PROD_REQUIRED_COLS])
    logger.info("append done | rows=%s | out=%s", len(df), paths.production_actuals_csv)
    print(f"OK: appended {len(df):,} rows -> {paths.production_actuals_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
