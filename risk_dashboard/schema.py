from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class APSSnapshotCols:
    기준일: str = "기준일"
    수주번호: str = "수주번호"
    품목코드: str = "품목코드"
    품명: str = "품명"
    납기일: str = "납기일"
    사출종료일: str = "사출종료일"
    분리종료일: str = "분리종료일"
    하이드종료일: str = "하이드종료일"
    접착종료일: str = "접착종료일"
    누수종료일: str = "누수종료일"
    포장종료일: str = "포장종료일"

    # Optional (원인분석 고도화용)
    필요수량: str = "필요수량"


@dataclass(frozen=True)
class ProductionActualCols:
    생산일자: str = "생산일자"
    공정: str = "공정"
    품목코드: str = "품목코드"
    생산수량: str = "생산수량"  # Gross
    양품수량: str = "양품수량"  # Good (optional)


APS_COLS = APSSnapshotCols()
PROD_COLS = ProductionActualCols()

APS_REQUIRED_COLS = [
    APS_COLS.기준일,
    APS_COLS.수주번호,
    APS_COLS.품목코드,
    APS_COLS.품명,
    APS_COLS.납기일,
    APS_COLS.사출종료일,
    APS_COLS.분리종료일,
    APS_COLS.하이드종료일,
    APS_COLS.접착종료일,
    APS_COLS.누수종료일,
    APS_COLS.포장종료일,
]

PROD_REQUIRED_COLS = [
    PROD_COLS.생산일자,
    PROD_COLS.공정,
    PROD_COLS.품목코드,
    PROD_COLS.생산수량,
]

PROCESS_COLS_IN_ORDER = [
    ("사출", APS_COLS.사출종료일),
    ("분리", APS_COLS.분리종료일),
    ("하이드", APS_COLS.하이드종료일),
    ("접착", APS_COLS.접착종료일),
    ("누수", APS_COLS.누수종료일),
    ("포장", APS_COLS.포장종료일),
]

