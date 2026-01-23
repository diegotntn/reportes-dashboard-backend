import pandas as pd
from typing import Dict, Any, List
from datetime import date

from services.reportes.aggregations.tabla import tabla_final
from services.reportes.personas.asignaciones import obtener_asignaciones_activas


# ─────────────────────────────
# Agrupación principal
# ─────────────────────────────
def agrupar_por_persona(
    reportes_queries,
    df: pd.DataFrame,
    desde: date,
    hasta: date,
    kpis: Dict[str, bool],
) -> Dict[str, Any]:
    """
    Agrupa devoluciones por persona usando asignaciones históricas.

    RESPONSABILIDAD:
    - Cruza devoluciones (DataFrame ya normalizado)
    - Aplica lógica temporal de asignaciones activas
    - Calcula KPIs y genera tablas finales

    REGLAS:
    - NO consulta Mongo directamente
    - NO construye pipelines
    - Queries SOLO proveen datos crudos
    """

    # ─────────────────────────────
    # Validaciones base
    # ─────────────────────────────
    if df is None or df.empty:
        return {}

    if not kpis:
        return {}

    if not isinstance(desde, date) or not isinstance(hasta, date):
        raise ValueError("`desde` y `hasta` deben ser datetime.date")

    # ─────────────────────────────
    # Obtener asignaciones CRUDAS
    # ─────────────────────────────
    asignaciones: List[Dict] = reportes_queries.asignaciones_personal()

    if not asignaciones:
        return {}

    # ─────────────────────────────
    # Resolver asignaciones activas
    # ─────────────────────────────
    pasillo_a_persona = obtener_asignaciones_activas(
        asignaciones=asignaciones,
        desde=desde,
        hasta=hasta,
    )

    if not pasillo_a_persona:
        return {}

    # ─────────────────────────────
    # Agrupar índices por persona
    # ─────────────────────────────
    indices_por_persona: Dict[str, List[int]] = {}

    for idx, row in df.iterrows():
        pasillo = str(row.get("pasillo", "")).strip()
        persona_id = pasillo_a_persona.get(pasillo)

        if not persona_id:
            continue

        indices_por_persona.setdefault(persona_id, []).append(idx)

    if not indices_por_persona:
        return {}

    # ─────────────────────────────
    # Construir resultado final
    # ─────────────────────────────
    resultado: Dict[str, Any] = {}

    for persona_id, idxs in indices_por_persona.items():
        df_persona = df.loc[idxs]

        resumen: Dict[str, Any] = {}

        if kpis.get("importe") and "importe" in df_persona:
            resumen["importe"] = float(df_persona["importe"].sum())

        if kpis.get("piezas") and "piezas" in df_persona:
            resumen["piezas"] = int(df_persona["piezas"].sum())

        if kpis.get("devoluciones") and "devoluciones" in df_persona:
            resumen["devoluciones"] = int(df_persona["devoluciones"].sum())

        resultado[persona_id] = {
            "resumen": resumen,
            "tabla": tabla_final(df_persona),
        }

    return resultado


# ─────────────────────────────
# Series temporales (charts)
# ─────────────────────────────
def agrupar_personas_por_fecha(df, kpis):
    """
    Agrupa por PERSONA y FECHA (series temporales).
    Pensado EXCLUSIVAMENTE para charts.
    """

    if df is None or df.empty:
        return {}

    if "persona_id" not in df.columns or "fecha" not in df.columns:
        return {}

    kpi_cols = [
        c for c in ("importe", "piezas", "devoluciones")
        if kpis.get(c) and c in df.columns
    ]

    resultado = {}

    for persona_id, df_persona in df.groupby("persona_id"):

        if not persona_id:
            continue

        nombre = (
            df_persona["persona_nombre"].iloc[0]
            if "persona_nombre" in df_persona.columns
            else "Sin nombre"
        )

        series = []

        for fecha, df_fecha in df_persona.groupby("fecha"):
            kpis_fecha = {}

            for c in kpi_cols:
                total = df_fecha[c].sum()
                kpis_fecha[c] = float(total) if c == "importe" else int(total)

            series.append({
                "fecha": fecha,
                "key": fecha.isoformat(),
                "label": fecha.isoformat(),
                "kpis": kpis_fecha,
            })

        series.sort(key=lambda x: x["fecha"])

        resultado[persona_id] = {
            "nombre": nombre,
            "series": series,
        }

    return resultado
