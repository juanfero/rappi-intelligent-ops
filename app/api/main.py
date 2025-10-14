from fastapi import FastAPI
from src.config import ENV, LOG_LEVEL
from app.api.routers.metrics import router as metrics_router

app = FastAPI(title="Rappi Intelligent Ops API")

@app.get("/health")
def health():
    return {"status": "ok", "env": ENV, "log_level": LOG_LEVEL}

# 👉 registra las rutas
app.include_router(metrics_router)

