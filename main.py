"""
Punto de entrada del backend ReporteSurtido.

RESPONSABILIDADES:
- Crear la aplicación FastAPI
- Configurar middlewares (CORS)
- Registrar rutas de la API (solo reportes)
- Exponer la app para Render / Uvicorn
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import reportes


# ─────────────────────────────────────────
# 🔁 BANDERA DE ENTORNO
# ─────────────────────────────────────────
MODE = 1  # 0 = LOCALHOST | 1 = RENDER


# ─────────────────────────────────────────
# CONFIGURACIÓN SEGÚN MODO
# ─────────────────────────────────────────
if MODE == 0:
    ALLOW_ORIGINS = [
        "http://localhost",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]
else:
    ALLOW_ORIGINS = [
        "https://diegotntn.github.io",
    ]


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
    # CORS
    # ─────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOW_ORIGINS,
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
    # HEALTH CHECK
    # ─────────────────────────────────────────
    @app.get("/api/health", tags=["Health"])
    def health_check():
        return {
            "status": "ok",
            "mode": "local" if MODE == 0 else "render"
        }

    return app


# ─────────────────────────────────────────
# APP EXPORTADA
# ─────────────────────────────────────────
app = create_app()
