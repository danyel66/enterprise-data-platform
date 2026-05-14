"""
Integration Hub API
-------------------
FastAPI application that serves as the entry point for:
1. Incoming webhooks (Freshdesk, future sources)
2. Event Grid push delivery endpoint
3. Gold layer data serving (read from Project 1 Delta Lake)
"""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import router as api_router
from src.ingestion.freshdesk_webhook import router as freshdesk_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

app = FastAPI(
    title="Enterprise Integration Hub",
    description="Event-driven integration layer connecting SaaS platforms to the Delta Lake lakehouse.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(freshdesk_router)
app.include_router(api_router)


@app.get("/health")
def health():
    return {"status": "ok", "service": "integration-hub"}
