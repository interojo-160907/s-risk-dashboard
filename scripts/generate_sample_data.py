from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from risk_dashboard.io import default_data_paths
from risk_dashboard.schema import APS_COLS, APS_REQUIRED_COLS, PROD_REQUIRED_COLS


def main() -> int:
    paths = default_data_paths("data")
    Path("data").mkdir(parents=True, exist_ok=True)

    base = date.today()
    d1 = base - timedelta(days=1)
    d2 = base

    orders = [f"SO{1000+i}" for i in range(1, 21)]
    skus = [f"SKU{200+i}" for i in range(1, 21)]

    rows = []
    for 기준일 in [d1, d2]:
        for i, (so, sku) in enumerate(zip(orders, skus), start=1):
            납기 = base + timedelta(days=(i % 8) - 4)
            shift = 0 if 기준일 == d1 else int(np.random.choice([-1, 0, 0, 1, 2]))
            포장 = 납기 + timedelta(days=max(-2, min(6, (i % 5) - 2 + shift)))
            공정오프셋 = {
                APS_COLS.사출종료일: 8,
                APS_COLS.분리종료일: 6,
                APS_COLS.하이드종료일: 4,
                APS_COLS.접착종료일: 3,
                APS_COLS.누수종료일: 2,
                APS_COLS.포장종료일: 0,
            }
            row = {
                APS_COLS.기준일: 기준일,
                APS_COLS.수주번호: so,
                APS_COLS.품목코드: sku,
                APS_COLS.품명: f"제품{i}",
                APS_COLS.납기일: 납기,
            }
            for col, off in 공정오프셋.items():
                row[col] = 포장 - timedelta(days=off)
            rows.append(row)

    aps = pd.DataFrame(rows)[APS_REQUIRED_COLS]
    aps.to_csv(paths.aps_snapshot_csv, index=False, encoding="utf-8-sig")

    prows = []
    for dd in [d1, d2]:
        for proc in ["사출", "분리", "하이드", "접착", "누수", "포장"]:
            for sku in skus:
                prows.append(
                    {
                        "생산일자": dd,
                        "공정": proc,
                        "품목코드": sku,
                        "생산수량": int(np.random.randint(10, 80)),
                    }
                )
    prod = pd.DataFrame(prows)[PROD_REQUIRED_COLS]
    prod.to_csv(paths.production_actuals_csv, index=False, encoding="utf-8-sig")

    print("OK: wrote sample data -> data/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
