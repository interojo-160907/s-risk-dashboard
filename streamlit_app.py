import streamlit as st

st.set_page_config(page_title="S-Risk Dashboard", layout="wide")

st.title("📊 S관 납기 리스크 관리 대시보드")

st.markdown("---")

# KPI 샘플
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("총 수주 수", "0")

with col2:
    st.metric("지연 수주", "0")

with col3:
    st.metric("긴급 수주", "0")

st.markdown("## 📌 대시보드 초기 상태")

st.info("데이터 연결 전 테스트 화면입니다.")
