import pandas as pd
from typing import Dict, List

from .pipelines import (
    pipeline_devoluciones_detalle,
    pipeline_devoluciones_resumen,
    pipeline_devolucion_articulos,
)


class ReportesAccess:
    """
    Ejecuta consultas especializadas para REPORTES.

    RESPONSABILIDAD:
    - Construir pipelines Mongo
    - Ejecutar aggregate / find sobre colecciones reales
    - Devolver datos CRUDOS (DataFrame o list)

    NO HACE:
    - Lógica de negocio
    - Inferencias temporales
    - Agrupaciones analíticas finales
    """

    # ─────────────────────────────
    # INIT
    # ─────────────────────────────
    def __init__(self, provider):
        """
        provider: MongoClientProvider
        """
        self.provider = provider

        # Colecciones reales (PyMongo Collection)
        self.devoluciones = provider.get_collection("devoluciones")
        self.personas = provider.get_collection("personal")
        self.asignaciones = provider.get_collection("asignaciones")

    # ─────────────────────────────
    # DEVOLUCIONES (BASE ANALÍTICA)
    # ─────────────────────────────
    def devoluciones_detalle(self, filtros: Dict) -> pd.DataFrame:
        """
        Devuelve eventos base de devoluciones
        (UNA FILA POR ARTÍCULO).
        """
        pipeline = pipeline_devoluciones_detalle(filtros)
        data = list(self.devoluciones.aggregate(pipeline))

        if not data:
            return pd.DataFrame(
                columns=[
                    "fecha",
                    "zona",
                    "pasillo",
                    "piezas",
                    "importe",
                    "devoluciones",
                ]
            )

        return pd.DataFrame(data)

    # ─────────────────────────────
    # RESUMEN ADMINISTRATIVO
    # ─────────────────────────────
    def devoluciones_resumen(self, filtros: Dict) -> pd.DataFrame:
        """
        Devuelve resumen administrativo
        (UNA FILA POR DEVOLUCIÓN).
        """
        pipeline = pipeline_devoluciones_resumen(filtros)
        data = list(self.devoluciones.aggregate(pipeline))

        if not data:
            return pd.DataFrame(
                columns=[
                    "id",
                    "fecha",
                    "folio",
                    "cliente",
                    "zona",
                    "estatus",
                    "total",
                ]
            )

        return pd.DataFrame(data)

    # ─────────────────────────────
    # ARTÍCULOS POR DEVOLUCIÓN
    # ─────────────────────────────
    def devolucion_articulos(self, devolucion_id: str) -> pd.DataFrame:
        """
        Devuelve artículos de una devolución específica.
        """
        pipeline = pipeline_devolucion_articulos(devolucion_id)
        data = list(self.devoluciones.aggregate(pipeline))

        if not data:
            return pd.DataFrame(
                columns=[
                    "nombre",
                    "codigo",
                    "pasillo",
                    "cantidad",
                    "unitario",
                ]
            )

        return pd.DataFrame(data)

    # ─────────────────────────────
    # PERSONAS (DIMENSIÓN)
    # ─────────────────────────────
    def personas_activas(self) -> Dict[str, str]:
        """
        Devuelve un MAPA de personas activas.

        RETURN:
        { persona_id: nombre }
        """
        cursor = self.personas.find(
            {"activo": True},
            {"_id": 1, "nombre": 1}
        )

        return {
            str(p["_id"]): p["nombre"]
            for p in cursor
        }

    # ─────────────────────────────
    # ASIGNACIONES (DIMENSIÓN)
    # ─────────────────────────────
    def asignaciones_personal(self) -> List[Dict]:
        """
        Devuelve TODAS las asignaciones de personal
        (SIN lógica temporal).
        """
        cursor = self.asignaciones.find(
            {},
            {
                "_id": 0,
                "pasillo": 1,
                "persona_id": 1,
                "fecha_desde": 1,
                "fecha_hasta": 1,
            }
        )

        return list(cursor)

    # ─────────────────────────────
    # DEBUG DIRECTO (SIN PIPELINE)
    # ─────────────────────────────
    def debug_find_devoluciones(self, filtros: Dict):
        """
        DEBUG PURO:
        Acceso directo a Mongo para validar filtros.
        """
        docs = list(self.devoluciones.find(filtros).limit(1))
        return docs
