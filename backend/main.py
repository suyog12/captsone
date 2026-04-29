from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import settings
from backend.db.database import engine
from backend.routers import (
    assignments,
    auth,
    cart,
    customers,
    health,
    purchase_history,
    recommendations,
    stats,
    users,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """App startup and shutdown hooks."""
    yield
    await engine.dispose()


app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    debug=settings.api_debug,
    description=(
        "API for the recommendation dashboard capstone project. "
        "Reads precomputed recommendations from parquet files via DuckDB "
        "and live transactional state from Postgres via SQLAlchemy."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(health.router)
app.include_router(auth.router)
app.include_router(users.router)
app.include_router(customers.router)
app.include_router(purchase_history.router)
app.include_router(recommendations.router)
app.include_router(assignments.router)
app.include_router(cart.router)
app.include_router(stats.router)


@app.get("/", tags=["root"])
async def root() -> dict[str, str]:
    """Simple root endpoint that points to the API docs."""
    return {
        "service": settings.api_title,
        "version": settings.api_version,
        "docs": "/docs",
        "health": "/health",
    }