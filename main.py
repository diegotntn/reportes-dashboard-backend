"""
Punto de entrada del backend ReporteSurtido (PRODUCCIÓN).

RESPONSABILIDADES:
- Crear la aplicación FastAPI
- Configurar middlewares (CORS)
- Registrar rutas de la API (solo reportes)
- Exponer la app para Render / Uvicorn

NOTA:
- Este archivo NO se usa en desarrollo local
- Es exclusivo para despliegue remoto
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from api.routes import reportes   # 🔑 SIN 'backend.' en producción


# ─────────────────────────────────────────
# CREACIÓN DE LA APLICACIÓN
# ─────────────────────────────────────────
def create_app() -> FastAPI:
    app = FastAPI(
        title="ReporteSurtido · Dashboard API",
        description="API de solo lectura para reportes y visualización de gráficas",
        version="2.0.0",
    )

    # ─────────────────────────────────────────
    # CORS (FRONTEND REMOTO)
    # ─────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://diegotntn.github.io",   # GitHub Pages
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ─────────────────────────────────────────
    # REGISTRO DE RUTAS
    # ─────────────────────────────────────────
    app.include_router(
        reportes.router,
        prefix="/api/reportes",
        tags=["Reportes"],
    )

    # ─────────────────────────────────────────
    # HEALTH CHECK (Render)
    # ─────────────────────────────────────────
    @app.get("/api/health", tags=["Health"])
    def health_check():
        return {"status": "ok"}

    return app


# ─────────────────────────────────────────
# APP EXPORTADA (Render la usa)
# ─────────────────────────────────────────
app = create_app()
