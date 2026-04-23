from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk_dashboard.logging_utils import get_logger  # noqa: E402
from risk_dashboard.master_products import filter_sgwan, load_master_table  # noqa: E402


def _norm_text(x: object) -> str:
    if pd.isna(x):
        return ""
    return " ".join(str(x).strip().split())


def _read_order_status_excel(path: Path, *, sheet_name: str, header_row_1based: int) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=sheet_name, header=None)  # type: ignore[call-arg]
    if raw.empty:
        return pd.DataFrame()

    header_idx = max(0, header_row_1based - 1)
    if header_idx >= len(raw):
        raise ValueError(f"header_row out of range: {header_row_1based} (rows={len(raw)})")

    header = raw.iloc[header_idx].tolist()
    df = raw.iloc[header_idx + 1 :].copy()
    df.columns = [str(c).strip() for c in header]

    # Drop fully-empty columns (common when Excel has trailing columns).
    df = df.dropna(axis=1, how="all").copy()
    return df


def _match_exact(item_names: pd.Series, master_names: set[str]) -> pd.Series:
    normed = item_names.map(_norm_text)
    return normed.isin(master_names)


def _match_contains(item_names: pd.Series, master_names: set[str]) -> pd.Series:
    # Relaxed match: either side contains the other (after normalization).
    masters = [m for m in {_norm_text(x) for x in master_names} if m]
    normed = item_names.map(_norm_text)

    def _one(s: str) -> bool:
        if not s:
            return False
        for m in masters:
            if m in s or s in m:
                return True
        return False

    return normed.map(_one)


def main() -> int:
    logger = get_logger("ingest.order_status_sgwan", log_file="logs/ingest_order_status_sgwan.log")
    ap = argparse.ArgumentParser(
        description="order_status_by_item_filtered.xlsx에서 '품명'을 마스터('S관 생산 제품 리스트.xlsx'의 '제품명')와 매칭해 S관 생산 제품 행만 추출"
    )
    ap.add_argument("--input", default="order_status_by_item_filtered.xlsx", help="입력 엑셀 경로")
    ap.add_argument("--sheet", default="data", help="시트명")
    ap.add_argument("--header-row", type=int, default=2, help="컬럼행(1-based). 기본: 2행")
    ap.add_argument("--item-name-col", default="품명", help="품명 컬럼명(기본: 품명)")

    ap.add_argument("--master", default="S관 생산 제품 리스트.xlsx", help="마스터 엑셀 경로")
    ap.add_argument("--master-name-col", default="제품명", help="마스터 제품명 컬럼명(기본: 제품명)")
    ap.add_argument("--master-plant-filter", default="S관", help="공장구분 필터 문자열(기본: S관)")

    ap.add_argument(
        "--match-mode",
        choices=["exact", "contains"],
        default="exact",
        help="매칭 방식(기본: exact). contains는 완화 매칭(부분포함)으로 오탐 가능",
    )

    ap.add_argument("--output", default="data/order_status_by_item_filtered_sgwan.csv", help="출력 CSV 경로")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(str(in_path))

    master_path = Path(args.master)
    if not master_path.exists():
        raise FileNotFoundError(str(master_path))

    logger.info(
        "start | input=%s | sheet=%s | header_row=%s | item_name_col=%s | master=%s | master_name_col=%s | match_mode=%s",
        in_path,
        args.sheet,
        args.header_row,
        args.item_name_col,
        master_path,
        args.master_name_col,
        args.match_mode,
    )

    mdf = load_master_table(master_path)
    mdf_s = filter_sgwan(mdf)
    if args.master_name_col not in mdf_s.columns:
        raise ValueError(f"마스터에 제품명 컬럼이 없습니다: {args.master_name_col} (컬럼={list(mdf_s.columns)})")

    master_names = {_norm_text(x) for x in mdf_s[args.master_name_col].tolist() if _norm_text(x)}
    logger.info("master rows=%s | master_names=%s", len(mdf_s), len(master_names))

    df = _read_order_status_excel(in_path, sheet_name=args.sheet, header_row_1based=args.header_row)
    if df.empty:
        logger.info("no data: empty order status")
        print("NO DATA: empty input")
        return 0

    if args.item_name_col not in df.columns:
        raise ValueError(f"입력에 품명 컬럼이 없습니다: {args.item_name_col} (컬럼={list(df.columns)})")

    if args.match_mode == "exact":
        mask = _match_exact(df[args.item_name_col], master_names)
    else:
        mask = _match_contains(df[args.item_name_col], master_names)

    out = df[mask].copy()
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    logger.info("done | input_rows=%s | matched_rows=%s | out=%s", len(df), len(out), out_path)
    print(f"OK: wrote {len(out):,}/{len(df):,} rows -> {out_path} (match_mode={args.match_mode})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

