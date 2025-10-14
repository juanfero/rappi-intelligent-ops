cat > README.md <<'MD'
# Rappi Intelligent Ops

## Setup rápido
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env    # pon tu API key en .env
PYTHONPATH=. python -m src.data.prepare_data
PYTHONPATH=. uvicorn app.api.main:app --reload --port 8000
python -m streamlit run app/ui/Home.py
# rappi-intelligent-ops
