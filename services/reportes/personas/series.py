from typing import Dict, Any
import pandas as pd


# ─────────────────────────────
# Series temporales (charts)
# ─────────────────────────────
def agrupar_personas_por_fecha(
    df: pd.DataFrame,
    kpis: Dict[str, bool],
) -> Dict[str, Any]:
    """
    Agrupa por PERSONA y FECHA.
    Uso exclusivo para gráficas.
    """

    if df is None or df.empty:
        return {}

    if "persona_id" not in df.columns or "fecha" not in df.columns:
        return {}

    kpi_cols = [
        c for c in ("importe", "piezas", "devoluciones")
        if kpis.get(c) and c in df.columns
    ]

    resultado: Dict[str, Any] = {}

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
            kpis_fecha = {
                c: float(df_fecha[c].sum())
                if c == "importe"
                else int(df_fecha[c].sum())
                for c in kpi_cols
            }

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
