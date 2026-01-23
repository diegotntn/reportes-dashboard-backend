"""
Dependencias de la API (SOLO REPORTES).

RESPONSABILIDAD:
- Proveer acceso a MongoDB (solo lectura)
- Inyectar el MongoClientProvider correcto
- Construir Queries analíticas
- Inyectar el Service de reportes

NO HACE:
- CRUD
- Escritura de datos
- Lógica de negocio
- Agregaciones analíticas

GRAFO CORRECTO:
MongoClientProvider → ReportesQueries → ReportesService
"""

# ─────────────────────────────────────────
# DB PROVIDER (SOLO LECTURA)
# ─────────────────────────────────────────
from db.factory import get_db
from db.mongo.client import MongoClientProvider


def get_database() -> MongoClientProvider:
    """
    Devuelve el proveedor Mongo en modo SOLO LECTURA.
    """
    return get_db()


# ─────────────────────────────────────────
# QUERIES ANALÍTICAS
# ─────────────────────────────────────────
from db.mongo.reportes.access import ReportesAccess


def get_reportes_queries() -> ReportesAccess:
    """
    Construye las queries analíticas de reportes.

    ⚠️ CLAVE:
    - Se inyecta el MongoClientProvider COMPLETO
    - NO se pasa una colección suelta
    """
    provider = get_database()
    return ReportesAccess(provider)


# ─────────────────────────────────────────
# SERVICE (ORQUESTADOR)
# ─────────────────────────────────────────
from services.reportes.service import ReportesService


def get_reportes_service() -> ReportesService:
    """
    Proveedor del servicio de reportes.

    Inyecta:
    - ReportesQueries (lectura Mongo)
    """
    queries = get_reportes_queries()
    return ReportesService(reportes_queries=queries)