# S관 납기 리스크 관리 대시보드

APS 스냅샷(일별 append) 데이터를 기준으로 **납기 상태/전일 대비 변경/병목공정/원인(간이)** 를 계산해 Streamlit 대시보드로 보여줍니다.

## 빠른 시작

0) 의존성 설치

```powershell
pip install -r .\requirements.txt
```

1) (선택) 샘플 데이터 생성

```powershell
python .\scripts\generate_sample_data.py
```

2) 실행

```powershell
python -m streamlit run .\app.py
```

또는 한 번에:

```powershell
.\run_dashboard.ps1
```

## 일일 운영(append)

### APS 스냅샷 적재

```powershell
python .\scripts\append_aps_snapshot.py --input "C:\path\aps.xlsx" --asof 2026-04-20
```

### 주문별 공정진도(날짜 시트)로 S관 APS 스냅샷 생성/적재

`주문별 공정진도.xlsx`의 **제품 이름(3번째 컬럼)** 을 기준으로 `S관 생산 제품 리스트.xlsx`(마스터)와 매칭해서 S관 품목만 추출합니다.

```powershell
python .\scripts\ingest_order_progress_sgwan.py --dry-run
python .\scripts\ingest_order_progress_sgwan.py
```

기본 동작은 `data/aps_snapshot.csv`에 이미 존재하는 `기준일`(=시트 날짜)은 **자동 스킵**해서, 엑셀에 날짜 시트를 계속 추가해도 **신규 날짜만 누적 append** 됩니다.

이미 적재된 날짜도 다시 넣고 싶으면(중복 가능):

```powershell
python .\scripts\ingest_order_progress_sgwan.py --force
```

### 생산실적 적재

```powershell
python .\scripts\append_production_actuals.py --input "C:\path\prod.xlsx"
```

### 생산실적현황(간편)_S관.xlsx(전월/당월)로 2개월 Rolling 데이터 생성

엑셀의 `전월`, `당월` 시트를 읽어서 `상태=확인`만 남기고 `양품수량`을 생산수량으로 사용해 `data/production_actuals_recent.csv`를 **덮어쓰기 생성**합니다.

```powershell
python .\scripts\ingest_production_actuals_sgwan_simple.py
```

참고: 대시보드의 `S관 실적` 탭은 **기준일(사이드바 선택) 당일 실적을 제외**하고, `기준일-1(전일)`까지 집계합니다(당일은 생산 중일 수 있음).

## GitHub에 올리기(로컬 → 원격)

엑셀 원본은 용량/보안 이슈가 있을 수 있어 **코드만 먼저 업로드**를 권장합니다(`~$*.xlsx`는 자동 제외).

```powershell
git init
git add .
git commit -m "Initial dashboard"

# GitHub에서 빈 repo 만든 뒤, 아래 remote URL만 본인 것으로 교체
git remote add origin https://github.com/<owner>/<repo>.git
git branch -M main
git push -u origin main
```

## 컬럼

- `data/aps_snapshot.csv`: 설계서의 APS 스냅샷 컬럼(필수) + 선택(`필요수량`)
- `data/production_actuals.csv`: `생산일자/공정/품목코드/생산수량`

## 메모

- 비교 기준 키는 `수주번호 + 품목코드` 입니다.
- `포장종료일`을 최종 기준으로 상태/변경을 계산합니다.
