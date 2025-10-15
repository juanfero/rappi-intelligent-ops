# app/ui/Insights.py
import streamlit as st
import pandas as pd
import requests
from pathlib import Path

st.set_page_config(page_title="Insights Automáticos", layout="wide")
st.title("Insights Automáticos — Rappi Intelligent Ops")

API_BASE = "http://127.0.0.1:8001"  # tu API en 8001

col1, col2, col3 = st.columns(3)
country = col1.text_input("País (opcional, ej. CO/MX/PE)")
city    = col2.text_input("Ciudad (opcional)")
zone    = col3.text_input("Zona (opcional)")

if st.button("Generar insights"):
    with st.spinner("Analizando..."):
        params = {"save": True}
        if country: params["country"] = country
        if city: params["city"] = city
        if zone: params["zone"] = zone
        try:
            r = requests.get(f"{API_BASE}/insights/", params=params, timeout=180)
            r.raise_for_status()
            payload = r.json()
            insights = payload["insights"]
            files = payload.get("files", {})

            st.subheader("Resumen Ejecutivo")
            exec_df = pd.DataFrame(insights.get("executive_summary", []))
            if exec_df.empty:
                st.info("Sin hallazgos críticos.")
            else:
                st.dataframe(exec_df[["category","country","city","zone","metric","title","summary","severity"]],
                             use_container_width=True)

            def section(name, label):
                st.markdown(f"### {label}")
                df = pd.DataFrame(insights.get(name, []))
                if df.empty:
                    st.caption("Sin hallazgos.")
                else:
                    st.dataframe(df.drop(columns=["extra"], errors="ignore"), use_container_width=True)

            section("anomalies", "Anomalías (WoW)")
            section("trends", "Tendencias (8 semanas)")
            section("benchmarking", "Benchmarking (pares)")
            section("correlations", "Correlaciones")
            section("opportunities", "Oportunidades")

            if files:
                st.markdown("#### Descargas")
                md = files.get("markdown"); html = files.get("html"); js = files.get("json")
                if md and Path(md).exists():
                    with open(md, "r", encoding="utf-8") as f:
                        st.download_button("Descargar Markdown", f.read().encode("utf-8"),
                                           file_name=Path(md).name, mime="text/markdown", key="dl_md")
                if html and Path(html).exists():
                    with open(html, "r", encoding="utf-8") as f:
                        st.download_button("Descargar HTML", f.read().encode("utf-8"),
                                           file_name=Path(html).name, mime="text/html", key="dl_html")
                if js and Path(js).exists():
                    with open(js, "r", encoding="utf-8") as f:
                        st.download_button("Descargar JSON", f.read().encode("utf-8"),
                                           file_name=Path(js).name, mime="application/json", key="dl_json")
        except Exception as e:
            st.error(f"Error llamando API: {e}")

