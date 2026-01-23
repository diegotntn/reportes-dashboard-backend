from typing import Dict, List
from datetime import date

from services.reportes.personas.utils import normalizar_fecha


# ─────────────────────────────
# Reglas temporales de asignación
# ─────────────────────────────
def obtener_asignaciones_activas(
    asignaciones: List[dict],
    desde: date,
    hasta: date,
) -> Dict[str, str]:
    """
    Devuelve un mapa pasillo → persona_id
    considerando solo asignaciones activas en el rango.
    """

    activas: Dict[str, str] = {}

    for a in asignaciones:
        pasillo = (a.get("pasillo") or "").strip()
        persona_id = (a.get("persona_id") or "").strip()

        if not pasillo or not persona_id:
            continue

        fecha_desde = normalizar_fecha(a.get("fecha_desde"))
        fecha_hasta = normalizar_fecha(a.get("fecha_hasta"))

        # Si empieza después del rango, no aplica
        if fecha_desde and fecha_desde > hasta:
            continue

        # Si terminó antes del rango, no aplica
        if fecha_hasta and fecha_hasta < desde:
            continue

        activas[pasillo] = persona_id

    return activas
