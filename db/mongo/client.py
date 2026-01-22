from pymongo import MongoClient
from datetime import datetime
from typing import Any, Dict, List


class MongoClientProvider:
    """
    Proveedor de acceso a MongoDB (SOLO LECTURA).

    RESPONSABILIDADES:
    - Crear y cerrar la conexión
    - Exponer acceso controlado a colecciones
    - Ejecutar consultas find / aggregate
    - NO escribir datos
    - NO contener lógica de negocio
    """

    # ─────────────────────────────
    # INIT
    # ─────────────────────────────
    def __init__(self, uri: str, db_name: str):
        self._client = MongoClient(uri)
        self._db = self._client[db_name]

    # ─────────────────────────────
    # ACCESO GENÉRICO
    # ─────────────────────────────
    def get_collection(self, name: str):
        """
        Devuelve una colección Mongo (uso interno por services / queries).
        """
        return self._db[name]

    # ─────────────────────────────
    # DEVOLUCIONES (LECTURA)
    # ─────────────────────────────
    def find_devoluciones(
        self,
        *,
        filtro: Dict[str, Any] | None = None,
        desde: datetime | None = None,
        hasta: datetime | None = None,
        vendedor_id: str | None = None,
        estatus: str | None = None,
    ) -> List[Dict]:
        """
        Consulta devoluciones mediante Mongo.find().
        """
        query: Dict[str, Any] = {}

        if isinstance(filtro, dict):
            query.update(filtro)

        if desde or hasta:
            query["fecha"] = {}
            if desde:
                query["fecha"]["$gte"] = desde
            if hasta:
                query["fecha"]["$lte"] = hasta

        if vendedor_id:
            query["vendedor_id"] = vendedor_id

        if estatus:
            query["estatus"] = estatus

        try:
            return list(self._db.devoluciones.find(query))
        except Exception:
            return []

    # ─────────────────────────────
    # AGGREGATE DEVOLUCIONES
    # ─────────────────────────────
    def aggregate_devoluciones(self, pipeline: List[Dict]) -> List[Dict]:
        """
        Ejecuta un aggregate sobre la colección devoluciones.
        """
        try:
            return list(self._db.devoluciones.aggregate(pipeline))
        except Exception:
            return []

    # ─────────────────────────────
    # DEVOLUCIÓN COMPLETA
    # ─────────────────────────────
    def get_devolucion_completa(self, devolucion_id) -> Dict | None:
        try:
            return self._db.devoluciones.find_one({"_id": devolucion_id})
        except Exception:
            return None

    # ─────────────────────────────
    # PERSONAL (LECTURA)
    # ─────────────────────────────
    def listar_personal(self, solo_activos: bool = True) -> List[Dict]:
        query = {"activo": True} if solo_activos else {}
        return list(self._db.personal.find(query))

    # ─────────────────────────────
    # ASIGNACIONES (LECTURA)
    # ─────────────────────────────
    def listar_asignaciones(self) -> List[Dict]:
        return list(self._db.asignaciones.find())

    # ─────────────────────────────
    # VENDEDORES (LECTURA)
    # ─────────────────────────────
    def listar_vendedores(self, solo_activos: bool = True) -> List[Dict]:
        query = {"activo": True} if solo_activos else {}
        return list(self._db.vendedores.find(query))

    # ─────────────────────────────
    # LIFECYCLE
    # ─────────────────────────────
    def close(self):
        self._client.close()
