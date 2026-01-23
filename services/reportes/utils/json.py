# services/reportes/utils/json.py

import math
import numpy as np
from datetime import datetime, date
from decimal import Decimal

try:
    import pandas as pd
except ImportError:
    pd = None


# ======================================================
# RESULTADOS ESTÁNDAR DE LA API
# ======================================================

def resultado_vacio(kpis, desde, hasta, agrupar):
    return {
        "kpis": kpis,
        "resumen": {
            "importe_total": 0.0,
            "piezas_total": 0,
            "devoluciones_total": 0,
        },
        "general": {
            "periodo": agrupar,
            "serie": [],
        },
        "por_zona": {},
        "por_pasillo": {},
        "personas": {},
        "por_persona": {},
        "personas_series": {},
        "tabla": [],
    }


def resultado_error(kpis, mensaje):
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


# ======================================================
# SERIALIZACIÓN SEGURA A JSON
# ======================================================

def limpiar_json(obj):
    """
    Limpia y serializa datos para JSON sin romper
    reportes existentes.
    """

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    if isinstance(obj, np.generic):
        return limpiar_json(obj.item())

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if pd:
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, pd.Period):
            return str(obj)

    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, dict):
        return {k: limpiar_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [limpiar_json(v) for v in obj]

    return obj
