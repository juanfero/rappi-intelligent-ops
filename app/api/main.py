# app/api/main.py
from fastapi import FastAPI
from pydantic import BaseModel

from src.bot.memory import Memory
from src.bot.parser import to_spec_llm, to_spec
from src.bot.executor import execute

# importa el router de insights
from app.api.insights import router as insights_router

# 1) crea la app primero
app = FastAPI(title="Rappi Intelligent Ops - API")

# 2) registra routers después de crear la app
app.include_router(insights_router)

# --- Chat API ---
MEM = Memory()

class ChatIn(BaseModel):
    question: str
    use_llm: bool = True

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/chat")
def chat(inp: ChatIn):
    memory = MEM.get()
    spec = to_spec_llm(inp.question, memory) if inp.use_llm else to_spec(inp.question, memory)
    MEM.update_from_spec(spec)
    result = execute(spec)
    return {"spec": spec.model_dump(), "result": result}

