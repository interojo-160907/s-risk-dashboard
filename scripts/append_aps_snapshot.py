from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk_dashboard.io import append_csv, default_data_paths, read_input_table, validate_required_columns
from risk_dashboard.logging_utils import get_logger
from risk_dashboard.schema import APS_COLS, APS_REQUIRED_COLS


def _parse_date(s: str) -> date:
    return pd.to_datetime(s, errors="raise").date()  # type: ignore[return-value]


def main() -> int:
    logger = get_logger("append.aps_snapshot", log_file="logs/append_aps_snapshot.log")
    ap = argparse.ArgumentParser(description="APS 스냅샷(일별) CSV에 append")
    ap.add_argument("--input", required=True, help="APS 원본 파일 경로(.xlsx/.csv)")
    ap.add_argument("--asof", default=None, help="기준일(YYYY-MM-DD). 미지정 시 오늘")
    ap.add_argument("--data-dir", default="data", help="저장 폴더(기본: data)")
    args = ap.parse_args()

    asof = _parse_date(args.asof) if args.asof else datetime.now().date()
    logger.info("start | input=%s | asof=%s | data_dir=%s", args.input, asof.isoformat(), args.data_dir)

    df = read_input_table(args.input)
    df = df.copy()
    df[APS_COLS.기준일] = asof

    validate_required_columns(df, [c for c in APS_REQUIRED_COLS if c != APS_COLS.기준일], name="APS 스냅샷 입력")

    # 날짜 컬럼 정규화
    for col in [c for c in APS_REQUIRED_COLS if c.endswith("일") and c != APS_COLS.기준일]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    paths = default_data_paths(args.data_dir)
    append_csv(Path(paths.aps_snapshot_csv), df[APS_REQUIRED_COLS + ([APS_COLS.필요수량] if APS_COLS.필요수량 in df.columns else [])])
    logger.info("append done | rows=%s | out=%s", len(df), paths.aps_snapshot_csv)
    print(f"OK: appended {len(df):,} rows -> {paths.aps_snapshot_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
