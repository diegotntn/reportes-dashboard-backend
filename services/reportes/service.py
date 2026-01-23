import pandas as pd

from db.mongo.reportes.predicates import (
    rango_fechas,
    combinar_filtros,
)

# ─── DATA ─────────────────────────────────────────────
from services.reportes.data.loader import (
    cargar_devoluciones_detalle,
)

from services.reportes.data.dataframe import (
    obtener_dataframe,
)

# ─── NORMALIZATION FLOW ───────────────────────────────
from services.reportes.normalization.flow import (
    normalizar_dataframe,
)

# ─── AGGREGATIONS ─────────────────────────────────────
from services.reportes.aggregations import (
    agrupa_por_zona,
    agrupa_por_pasillo,
    tabla_final,
)

# ─── GENERAL ──────────────────────────────────────────
from services.reportes.aggregations.general import (
    agrupa_general,
)

# ─── PERSONAS ─────────────────────────────────────────
from services.reportes.personas import (
    agrupar_por_persona,
)

from services.reportes.personas.agrupacion import (
    agrupar_personas_por_fecha,
)

# ─── TEMPORAL ─────────────────────────────────────────
from services.reportes.temporal import (
    map_periodo,
    serie_por_dia,
    serie_por_semana,
    serie_por_mes,
    serie_por_anio,
)


class ReportesService:
    """
    Servicio central de reportes (solo lectura).

    RESPONSABILIDAD:
    - Orquestar queries
    - Delegar normalización a flow
    - Construir agregaciones
    - Preparar payload FINAL para frontend
    """

    def __init__(self, reportes_queries):
        self.reportes_queries = reportes_queries

    # ─────────────────────────────
    # API PÚBLICA
    # ─────────────────────────────
    def generar(self, desde, hasta, agrupar="Mes", kpis=None):

        kpis = self._normalizar_kpis(kpis)

        desde, hasta = self._normalizar_fechas(desde, hasta)
        if not desde or not hasta or desde > hasta:
            return self._resultado_error(kpis, "Rango de fechas inválido")

        filtros = combinar_filtros(
            rango_fechas(desde, hasta)
        )

        raw = cargar_devoluciones_detalle(
            self.reportes_queries,
            filtros
        )

        if raw is None or raw.empty:
            return self._resultado_vacio(kpis, desde, hasta, agrupar)

        asignaciones = self.reportes_queries.asignaciones_personal()
        personas_map = self.reportes_queries.personas_activas()

        df = obtener_dataframe(
            raw,
            asignaciones=asignaciones,
            personas_map=personas_map,
        )

        if df is None or df.empty:
            return self._resultado_vacio(kpis, desde, hasta, agrupar)

        # ✅ NORMALIZACIÓN CENTRALIZADA
        df = normalizar_dataframe(df, kpis)

        # ─── KPIs globales
        resumen = {
            "importe_total": float(df["importe"].sum()) if kpis.get("importe") else 0.0,
            "piezas_total": int(df["piezas"].sum()) if kpis.get("piezas") else 0,
            "devoluciones_total": int(df["devoluciones"].sum()) if kpis.get("devoluciones") else 0,
        }

        periodo = map_periodo(agrupar)

        if periodo == "dia":
            general = serie_por_dia(df, desde, hasta)
        elif periodo == "semana":
            general = serie_por_semana(df, desde, hasta)
        elif periodo == "anio":
            general = serie_por_anio(df, desde, hasta)
        else:
            general = serie_por_mes(df, desde, hasta)

        por_persona = agrupar_por_persona(
            self.reportes_queries,
            df,
            desde,
            hasta,
            kpis,
        )

        personas_series = agrupar_personas_por_fecha(
            df,
            kpis
        )

        por_zona = agrupa_por_zona(df, kpis)
        por_pasillo = agrupa_por_pasillo(df, kpis)

        return {
            "kpis": kpis,
            "resumen": resumen,

            "general": {
                "periodo": periodo,
                "serie": general,
            },

            "por_zona": por_zona,
            "por_pasillo": por_pasillo,

            "personas": personas_map,
            "por_persona": por_persona,
            "personas_series": personas_series,

            "tabla": tabla_final(df),
        }

    # ─────────────────────────────
    # HELPERS
    # ─────────────────────────────
    def _normalizar_kpis(self, kpis):
        if not kpis:
            return {
                "importe": True,
                "piezas": True,
                "devoluciones": True,
            }
        return {
            "importe": bool(kpis.get("importe", True)),
            "piezas": bool(kpis.get("piezas", True)),
            "devoluciones": bool(kpis.get("devoluciones", True)),
        }

    def _normalizar_fechas(self, desde, hasta):
        d = pd.to_datetime(desde, errors="coerce")
        h = pd.to_datetime(hasta, errors="coerce")
        if pd.isna(d) or pd.isna(h):
            return None, None
        return d.date(), h.date()

    def _resultado_vacio(self, kpis, desde, hasta, agrupar):
        return {
            "kpis": kpis,
            "resumen": {
                "importe_total": 0.0,
                "piezas_total": 0,
                "devoluciones_total": 0,
            },
            "general": {
                "periodo": map_periodo(agrupar),
                "serie": [],
            },
            "por_zona": {},
            "por_pasillo": {},
            "personas": {},
            "por_persona": {},
            "personas_series": {},
            "tabla": [],
        }

    def _resultado_error(self, kpis, mensaje):
        return {
            "kpis": kpis,
            "error": mensaje,
            "general": None,
            "por_zona": {},
            "por_pasillo": {},
            "personas": {},
            "por_persona": {},
            "personas_series": {},
            "tabla": [],
        }
