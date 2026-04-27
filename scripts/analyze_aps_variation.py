from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk_dashboard.aps_variation import analyze_workbook, write_analysis_to_excel


def main() -> int:
    p = argparse.ArgumentParser(description="APS 변동사항 체크(원복 엑셀 1개) 분석")
    p.add_argument("--input", "-i", default="업데이트 데이터/APS 변동사항 체크.xlsx", help="입력 엑셀 파일 경로")
    p.add_argument(
        "--output",
        "-o",
        default="data/aps_variation_result.xlsx",
        help="출력 엑셀 파일 경로",
    )
    p.add_argument(
        "--scope",
        default="default",
        choices=["default", "all"],
        help="default=총합계+[85]포장, all=모든 공정",
    )
    args = p.parse_args()

    xlsx_path = Path(args.input)
    if not xlsx_path.exists():
        raise FileNotFoundError(str(xlsx_path))

    if args.scope == "all":
        # 워크북에 존재하는 공정들을 전부 자동 사용: 첫 날짜 시트의 1레벨 헤더 기반
        import pandas as pd

        xl = pd.ExcelFile(xlsx_path)
        date_sheets = [s for s in xl.sheet_names if s != "제품명등록" and s.isdigit()]
        if not date_sheets:
            raise ValueError("날짜 시트를 찾지 못했습니다.")
        sample = pd.read_excel(xlsx_path, sheet_name=date_sheets[-1], header=[0, 1], nrows=1)
        procs = [c[0] for c in sample.columns if c[0] != "공정 코드:"]
        scope_processes = tuple(dict.fromkeys(procs).keys())
    else:
        scope_processes = ("총합계", "[85]포장")

    tables = analyze_workbook(xlsx_path, scope_processes=scope_processes)
    out = write_analysis_to_excel(args.output, tables, xlsx_path=xlsx_path, scope_processes=scope_processes)
    print(f"OK: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
