# Rappi Intelligent Ops — Caso Técnico AI Engineer

MVP funcional para **analizar métricas operativas** de Rappi por país, ciudad y zona.  
Incluye:
- **Chat de datos (NL→SQL)** sobre DuckDB/Parquet.
- **API FastAPI** con endpoints de análisis.
- **UI Streamlit** para chat y reportes.
- **Sistema de Insights Automáticos (30%)**: anomalías WoW, tendencias, benchmarking, correlaciones y oportunidades.
- **Reportes** descargables en Markdown / HTML / JSON.

---

## Requisitos

- Ubuntu 20.04+ (probado en 22.04) o macOS
- Python 3.10+
- Git y `venv`

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git build-essential
```

---

## Instalación

```bash
git clone https://github.com/TU_USUARIO/NOMBRE_REPO.git
cd NOMBRE_REPO
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## Variables de Entorno

Copia el archivo `.env.example` a `.env`:

```bash
cp .env.example .env
```

Ejemplo de contenido:

```
# Clave opcional para usar LLMs (si no, se usa parser basado en reglas)
OPENAI_API_KEY=tu_api_key_aqui

# Configuración de entorno
ENV=dev
LOG_LEVEL=INFO

# Dirección de la API (para Streamlit)
API_BASE=http://127.0.0.1:8001
```

> Si no defines `OPENAI_API_KEY`, todo sigue funcionando con reglas internas.

---

## Datos de Ejemplo

Coloca el archivo en:

```
data/raw/Sistema de Análisis Inteligente para Operaciones Rappi - Dummy Data.xlsx
```

### Preparar datos

Convierte el Excel en Parquet + DuckDB:

```bash
PYTHONPATH=. python src/data/prepare_data.py
```

Esto genera:

* `data/processed/metrics.parquet`
* `data/processed/warehouse.duckdb`

---

## Estructura del Proyecto

```
app/
  api/
    main.py         # API principal FastAPI
    insights.py     # Endpoint de insights automáticos
  ui/
    Home.py         # Pantalla de inicio Streamlit
    Chat.py         # Chat NL→SQL
    Insights.py     # Pestaña de Insights automáticos
src/
  bot/
    schema.py
    parser.py
    executor.py
    memory.py
  data/
    prepare_data.py # Limpieza y carga en DuckDB
  insights/
    config.py       # Umbrales y reglas de negocio
    engine.py       # Motor de insights
    report.py       # Reportes MD/HTML/JSON
data/
  raw/              # Excel original
  processed/        # Parquet + DuckDB
reports/            # Reportes generados
```

---

## Ejecución

### 1) API FastAPI

```bash
PYTHONPATH=. uvicorn app.api.main:app --reload --port 8001
```

Endpoints principales:

* Health: [http://127.0.0.1:8001/health](http://127.0.0.1:8001/health)
* Insights: [http://127.0.0.1:8001/insights/](http://127.0.0.1:8001/insights/)

### 2) UI Streamlit

En otra terminal (con venv activado):

```bash
# Chat de datos
streamlit run app/ui/Chat.py

# Insights automáticos
streamlit run app/ui/Insights.py
```

---

## Casos de prueba

1. **Chat de datos**

   * *“Top 5 zonas con mayor Lead Penetration esta semana en Colombia”*
   * *“Comparar Perfect Orders entre Medellín y Bogotá”*
   * *“Promedio de Gross Profit UE por país”*

2. **Insights automáticos**

   * Abre `app/ui/Insights.py`, ingresa:

     * País: `CO`
     * Ciudad: `Medellín`
   * Debe mostrar anomalías, tendencias, benchmarking, correlaciones y oportunidades.

3. **API directo**

   ```bash
   curl "http://127.0.0.1:8001/insights/?country=CO&save=true"
   ```

   Generará:

   * `reports/insights_report_YYYYMMDD_HHMM.md`
   * `reports/insights_report_YYYYMMDD_HHMM.html`
   * `reports/insights_report_YYYYMMDD_HHMM.json`

---

## Troubleshooting

* **500 Internal Server Error en Insights** → revisa los logs de Uvicorn (generalmente falta `warehouse.duckdb` o los filtros no tienen datos).
* **DuckDB locked** → cierra procesos que lo usen (Streamlit o API duplicada).
* **Nombres de columnas no encontrados** → revisa `prepare_data.py` y ajusta si tu Excel tiene headers distintos.




