from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk_dashboard.aps_cache import default_cache_paths, save_cached_tables, signature  # noqa: E402
from risk_dashboard.aps_variation import analyze_workbook  # noqa: E402


def _status_path(data_dir: str | Path) -> Path:
    return Path(data_dir) / "aps_risk_cache_status.json"


def _lock_path(data_dir: str | Path) -> Path:
    return Path(data_dir) / "aps_risk_cache.lock"


def _write_status(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="APS 리스크 캐시 빌드(백그라운드용)")
    ap.add_argument("--input", "-i", default="업데이트 데이터/APS 변동사항 체크.xlsx", help="입력 엑셀 경로")
    ap.add_argument("--scope", choices=["default", "all"], default="default", help="공정 스코프")
    ap.add_argument("--days", type=int, default=8, help="최근 N일 시트만 분석(기본: 8일; 전체는 0)")
    ap.add_argument("--data-dir", default="data", help="캐시 저장 폴더")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(str(in_path))

    data_dir = Path(args.data_dir)
    lock = _lock_path(data_dir)
    status = _status_path(data_dir)

    # single-flight lock
    try:
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock_fp = lock.open("x", encoding="utf-8")
    except FileExistsError:
        # already running
        return 0

    try:
        _write_status(
            status,
            {
                "state": "running",
                "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "input": str(in_path.resolve()),
                "scope": args.scope,
            },
        )

        if args.scope == "all":
            import pandas as pd

            xl = pd.ExcelFile(in_path)
            date_sheets = [s for s in xl.sheet_names if s != "제품명등록" and str(s).isdigit()]
            if not date_sheets:
                raise ValueError("날짜 시트를 찾지 못했습니다.")
            sample = pd.read_excel(in_path, sheet_name=date_sheets[-1], header=[0, 1], nrows=1)
            procs = [c[0] for c in sample.columns if c[0] != "공정 코드:"]
            scope_processes = tuple(dict.fromkeys(procs).keys())
            scope_key = "all"
        else:
            scope_processes = ("총합계", "[85]포장")
            scope_key = "default"

        window_days = None if int(args.days) <= 0 else int(args.days)
        tables = analyze_workbook(in_path, scope_processes=scope_processes, window_days=window_days)
        sig = signature(in_path)
        cache_paths = default_cache_paths(data_dir)
        save_cached_tables(cache_paths, sig=sig, scope_key=scope_key, tables=tables)

        _write_status(
            status,
            {
                "state": "done",
                "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "input": str(in_path.resolve()),
                "scope": args.scope,
                "cache_pkl": str(cache_paths.data_pkl),
                "cache_meta": str(cache_paths.meta_json),
            },
        )
        return 0
    except Exception as e:
        _write_status(
            status,
            {
                "state": "error",
                "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "input": str(in_path.resolve()),
                "scope": args.scope,
                "error": str(e),
            },
        )
        raise
    finally:
        try:
            lock_fp.close()
        except Exception:
            pass
        try:
            lock.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
