# S관 생산실적 대시보드

`생산실적현황(간편)_S관.xlsx` 또는 CSV를 기반으로, **전월(월 전체) vs 당월(MTD, 기준일-1까지/당일 제외)** 생산실적을 Streamlit 대시보드로 보여줍니다.

## 빠른 시작

0) 의존성 설치

```powershell
pip install -r .\requirements.txt
```

1) 실행

```powershell
python -m streamlit run .\app.py
```

또는 한 번에:

```powershell
.\run_dashboard.ps1
```

## 데이터 준비

### 생산실적현황(간편)_S관.xlsx(전월/당월)로 2개월 Rolling 데이터 생성

엑셀의 `전월`, `당월` 시트를 읽어서 `상태=확인`만 남기고 `양품수량`을 생산수량으로 사용해 `data/production_actuals_recent.csv`를 **덮어쓰기 생성**합니다.

```powershell
python .\scripts\ingest_production_actuals_sgwan_simple.py
```

참고: 대시보드는 **기준일 당일 실적을 제외**하고, `기준일-1(전일)`까지 집계합니다(당일은 생산 중일 수 있음).

### order_status_by_item_filtered.xlsx에서 S관 제품만 추출

`order_status_by_item_filtered.xlsx`의 `품명`을 `S관 생산 제품 리스트.xlsx`의 `제품명`과 매칭해서, S관 생산 제품 행만 `data/order_status_by_item_filtered_sgwan.csv`로 생성합니다.

```powershell
python .\scripts\ingest_order_status_by_item_filtered_sgwan.py
```

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

- `data/production_actuals_recent.csv` (또는 `data/production_actuals.csv`): `생산일자/공정/품목코드/생산수량`
