from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk_dashboard.io import append_csv, default_data_paths, load_aps_snapshot  # noqa: E402
from risk_dashboard.logging_utils import get_logger  # noqa: E402
from risk_dashboard.master_products import (  # noqa: E402
    build_name_to_code,
    filter_sgwan,
    load_master_table,
    s_product_name_set,
)
from risk_dashboard.logic import available_snapshot_dates  # noqa: E402
from risk_dashboard.order_progress import (  # noqa: E402
    load_order_progress_sheet,
    parse_sheet_date,
    to_aps_snapshot,
)
from risk_dashboard.schema import APS_REQUIRED_COLS  # noqa: E402


def main() -> int:
    logger = get_logger("ingest.order_progress", log_file="logs/ingest_order_progress.log")
    ap = argparse.ArgumentParser(description="주문별 공정진도 엑셀(날짜 시트) → S관 APS 스냅샷 append")
    ap.add_argument("--progress-xlsx", default="업데이트 데이터/주문별 공정진도.xlsx", help="주문별 공정진도 엑셀 경로")
    ap.add_argument("--master-xlsx", default="업데이트 데이터/S관 생산 제품 리스트.xlsx", help="S관 제품 마스터 엑셀 경로")
    ap.add_argument("--data-dir", default="data", help="저장 폴더(기본: data)")
    ap.add_argument("--sheet", default=None, help="특정 시트명만 처리(미지정 시 날짜형 시트 전부)")
    ap.add_argument("--dry-run", action="store_true", help="append 하지 않고 요약만 출력")
    ap.add_argument(
        "--force",
        action="store_true",
        help="이미 적재된 기준일(시트 날짜)도 다시 append (중복 가능)",
    )
    args = ap.parse_args()

    progress_path = Path(args.progress_xlsx)
    master_path = Path(args.master_xlsx)

    logger.info("start | progress=%s | master=%s | data_dir=%s | sheet=%s | dry_run=%s | force=%s", progress_path, master_path, args.data_dir, args.sheet, args.dry_run, args.force)

    if not progress_path.exists():
        logger.error("missing file: %s", progress_path)
        raise FileNotFoundError(str(progress_path))
    if not master_path.exists():
        logger.error("missing file: %s", master_path)
        raise FileNotFoundError(str(master_path))

    mdf = load_master_table(master_path)
    mdf_s = filter_sgwan(mdf)
    s_names = s_product_name_set(mdf_s)
    name_to_code = build_name_to_code(mdf_s)
    logger.info("master loaded | rows=%s | s_rows=%s | s_names=%s", len(mdf), len(mdf_s), len(s_names))

    xl = pd.ExcelFile(progress_path)
    sheets = [args.sheet] if args.sheet else xl.sheet_names

    paths = default_data_paths(args.data_dir)
    existing = load_aps_snapshot(paths.aps_snapshot_csv)
    existing_dates = set(available_snapshot_dates(existing))

    combined: list[pd.DataFrame] = []
    total_stats = {"sheets": 0, "rows_in_sheet": 0, "rows_after_s_filter": 0, "missing_item_code": 0, "skipped_sheets": 0}
    skipped: list[str] = []
    skipped_existing: list[str] = []

    for sh in sheets:
        sh_date = parse_sheet_date(str(sh))
        if sh_date is None:
            skipped.append(str(sh))
            total_stats["skipped_sheets"] += 1
            continue
        if (not args.force) and (sh_date in existing_dates):
            skipped_existing.append(str(sh))
            continue

        df = load_order_progress_sheet(progress_path, sh)
        aps, stats = to_aps_snapshot(df, 기준일=sh_date, s_product_names=s_names, name_to_code=name_to_code)
        combined.append(aps)
        total_stats["sheets"] += 1
        for k in ["rows_in_sheet", "rows_after_s_filter", "missing_item_code"]:
            total_stats[k] += int(stats[k])

    if skipped:
        logger.info("skip non-date sheets | count=%s | samples=%s", len(skipped), skipped[:10])
        print(f"SKIP: 날짜형 시트가 아닌 시트 {len(skipped)}개 -> {skipped[:10]}")
    if skipped_existing:
        logger.info("skip existing dates | count=%s | samples=%s", len(skipped_existing), skipped_existing[:10])
        print(f"SKIP: 이미 적재된 기준일 시트 {len(skipped_existing)}개 -> {skipped_existing[:10]}")

    if not combined:
        logger.info("no data to ingest")
        print("NO DATA: 처리 가능한 날짜 시트가 없거나, S관 필터 결과가 비었습니다.")
        return 0

    out = pd.concat(combined, ignore_index=True)

    # 품목코드 누락 행 제거(키 유지)
    before_drop = len(out)
    out = out[out["품목코드"].astype(str).str.len() > 0].copy()
    dropped = before_drop - len(out)

    logger.info(
        "stats | %s | dropped_missing_code=%s | final_rows=%s",
        total_stats,
        dropped,
        len(out),
    )
    print("STATS:", total_stats, f"dropped_missing_code={dropped:,}", f"final_rows={len(out):,}")
    if len(out) > 0:
        print("PREVIEW:")
        print(out.head(5).to_string(index=False))

    if args.dry_run:
        logger.info("dry-run done")
        return 0

    append_csv(paths.aps_snapshot_csv, out[APS_REQUIRED_COLS])
    logger.info("append done | rows=%s | out=%s", len(out), paths.aps_snapshot_csv)
    print(f"OK: appended {len(out):,} rows -> {paths.aps_snapshot_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
