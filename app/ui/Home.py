import streamlit as st
import requests
import os

st.set_page_config(page_title="Rappi Intelligent Ops", layout="wide")
st.title("Rappi Intelligent Ops - Demo")

# Base de la API: cambia el puerto si lanzaste uvicorn en otro (8001, etc.)
API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8001")  # <- usa este

with st.sidebar:
    st.header("Filtros")
    country = st.text_input("Country (CO/MX/AR...)", value="CO")
    zone = st.text_input("Zone (BOG/LIM...)", value="BOG")
    week = st.number_input("Week (0..52)", min_value=0, max_value=52, value=40)
    run = st.button("Consultar")

q = {"country": country, "zone": zone, "week": int(week)}

if run:
    try:
        r = requests.get(f"{API_BASE}/metrics", params=q, timeout=10)
        if r.ok:
            st.success("OK")
            st.write(r.json())
        else:
            st.error(f"Error API: {r.status_code} - {r.text}")
    except Exception as e:
        st.error(f"No pude conectar a la API: {e}")

