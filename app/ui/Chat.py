import os
import json
import hashlib
import streamlit as st
import pandas as pd
import plotly.express as px
import requests

st.set_page_config(page_title="Chat de Datos - Rappi", layout="wide")
st.title("Chat de Datos — Rappi Intelligent Ops")

API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8001")  # ajusta si tu API está en otro puerto

# Estado inicial
if "history" not in st.session_state:
    st.session_state.history = []
if "plot_counter" not in st.session_state:
    st.session_state.plot_counter = 0
if "table_counter" not in st.session_state:
    st.session_state.table_counter = 0

q = st.text_input("Escribe tu pregunta (ej.: Top 5 zonas con mayor Lead Penetration esta semana en Colombia)")
use_llm = st.toggle("Usar LLM para parseo", value=False)  # pon True si ya tienes OPENAI_API_KEY

if st.button("Enviar") and q:
    try:
        r = requests.post(f"{API_BASE}/chat", json={"question": q, "use_llm": use_llm}, timeout=60)
        r.raise_for_status()
        payload = r.json()
        st.session_state.history.append({"q": q, "payload": payload})
    except Exception as e:
        st.error(f"Error llamando API: {e}")

# ---------- Helpers UI ----------
def _chart_key(title: str, payload_obj: dict, suffix: str) -> str:
    raw = json.dumps({"t": title, "s": payload_obj, "sfx": suffix}, sort_keys=True)
    return "plot-" + hashlib.md5(raw.encode()).hexdigest()[:10]

def _table_key(idx: int) -> str:
    st.session_state.table_counter += 1
    return f"table-{idx}-{st.session_state.table_counter}"

def _next_plot_key(idx: int, suffix: str) -> str:
    st.session_state.plot_counter += 1
    return f"plot-{idx}-{st.session_state.plot_counter}-{suffix}"

# ---------- Render historial ----------
for i, turn in enumerate(reversed(st.session_state.history)):
    st.markdown(f"**Tú:** {turn['q']}")
    payload = turn["payload"]

    # Manejo de payloads de error
    if "error" in payload:
        st.error(payload["error"])
        continue

    res = payload.get("result", {})
    spec = payload.get("spec", {})

    # Encabezado
    metric_list = spec.get("metrics", [])
    metric_label = ", ".join(metric_list) if metric_list else "—"
    time_range = spec.get("time", {}).get("range", "L0W")
    task = spec.get("task", "—")
    st.caption(f"Task: {task} | Metric(s): {metric_label} | Time: {time_range}")
    st.subheader(res.get("title", "Respuesta"))

    # Datos
    data = res.get("data", [])
    if not data:
        st.info("Sin datos para mostrar con este filtro/consulta.")
        continue

    df = pd.DataFrame(data)

    # ---- Tabla (con key único) ----
    st.dataframe(df, use_container_width=True, key=_table_key(i))

    # ---- Gráfica (con key único y columnas tolerantes) ----
    viz = res.get("visualization")
    # Normaliza nombres potenciales
    has_value = "value" in df.columns or "VALUE" in df.columns
    value_col = "value" if "value" in df.columns else ("VALUE" if "VALUE" in df.columns else None)

    if viz == "bar" and has_value:
        # Eje X: preferimos 'grp', si no, ZONE, CITY, COUNTRY (en ese orden)
        x_candidates = [c for c in ["grp", "ZONE", "CITY", "COUNTRY"] if c in df.columns]
        xcol = x_candidates[0] if x_candidates else None
        if xcol and value_col:
            fig = px.bar(df, x=xcol, y=value_col)
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=_next_plot_key(i, f"bar-{xcol}-{value_col}")
            )

    elif viz == "line":
        # Trend: puede venir como ('week','value') o ('WEEK_OFFSET','VALUE')
        xcol = "week" if "week" in df.columns else ("WEEK_OFFSET" if "WEEK_OFFSET" in df.columns else None)
        ycol = "value" if "value" in df.columns else ("VALUE" if "VALUE" in df.columns else None)
        color = None
        for c in ["ZONE", "grp", "CITY", "COUNTRY"]:
            if c in df.columns:
                color = c
                break
        if xcol and ycol:
            fig = px.line(df, x=xcol, y=ycol, color=color)
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=_next_plot_key(i, f"line-{xcol}-{ycol}-{color or 'none'}")
            )

    # ---- Descarga (key único) ----
    st.download_button(
        "Descargar CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"resultado_{i}.csv",
        mime="text/csv",
        key=f"download_csv_{i}",
    )

    # ---- Sugerencias ----
    sugg = res.get("suggestions", [])
    if sugg:
        st.markdown("**Siguientes pasos sugeridos:**")
        for s in sugg:
            st.markdown(f"- {s}")