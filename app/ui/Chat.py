import streamlit as st, pandas as pd, requests, plotly.express as px, os
st.set_page_config(page_title="Chat de Datos - Rappi", layout="wide")
st.title("Chat de Datos — Rappi Intelligent Ops")

API_BASE = os.getenv("API_BASE","http://127.0.0.1:8001")  # ajusta puerto si tu API está en 8000

if "history" not in st.session_state: st.session_state.history=[]
q = st.text_input("Escribe tu pregunta (ej.: Top 5 zonas con mayor Lead Penetration esta semana en Colombia)")
use_llm = st.toggle("Usar LLM para parseo", value=False)  # pon True si ya tienes OPENAI_API_KEY

if st.button("Enviar") and q:
    try:
        r = requests.post(f"{API_BASE}/chat", json={"question": q, "use_llm": use_llm}, timeout=60)
        r.raise_for_status()
        st.session_state.history.append({"q": q, "payload": r.json()})
    except Exception as e:
        st.error(f"Error llamando API: {e}")

# ... arriba va todo igual ...

# donde antes tenías: for turn in reversed(st.session_state.history):
for i, turn in enumerate(reversed(st.session_state.history)):
    st.markdown(f"**Tú:** {turn['q']}")
    res = turn["payload"]["result"]
    spec = turn["payload"]["spec"]

    st.caption(f"Task: {spec['task']} | Metric(s): {', '.join(spec['metrics'])} | Time: {spec['time']['range']}")
    st.subheader(res.get("title","Respuesta"))

    data = res.get("data", [])
    if data:
        import pandas as pd
        df = pd.DataFrame(data)
        st.dataframe(df, use_container_width=True)

        viz = res.get("visualization")
        if viz == "bar" and "value" in df.columns and ("grp" in df.columns or "ZONE" in df.columns):
            import plotly.express as px
            xcol = "grp" if "grp" in df.columns else "ZONE"
            fig = px.bar(df, x=xcol, y="value")
            st.plotly_chart(fig, use_container_width=True)
        elif viz == "line" and "WEEK_OFFSET" in df.columns and "VALUE" in df.columns:
            import plotly.express as px
            fig = px.line(df, x="WEEK_OFFSET", y="VALUE", color="ZONE" if "ZONE" in df.columns else None)
            st.plotly_chart(fig, use_container_width=True)

        # 👇 clave ÚNICA por turno y nombre de archivo distinto
        st.download_button(
            "Descargar CSV",
            df.to_csv(index=False).encode("utf-8"),
            file_name=f"resultado_{i}.csv",
            mime="text/csv",
            key=f"download_csv_{i}",
        )
    else:
        st.info("Sin datos para mostrar con este filtro/consulta.")

    sugg = res.get("suggestions", [])
    if sugg:
        st.markdown("**Siguientes pasos sugeridos:**")
        for s in sugg:
            st.markdown(f"- {s}")

