# services/reportes/personas/utils.py
from datetime import date, datetime
from typing import Optional


# ─────────────────────────────
# Utilidades temporales
# ─────────────────────────────
def normalizar_fecha(valor) -> Optional[date]:
    """
    Convierte valores de fecha provenientes de Mongo a `datetime.date`.

    Acepta:
    - None
    - datetime.date
    - datetime.datetime
    - str ISO (YYYY-MM-DD o YYYY-MM-DDTHH:MM:SS)

    Retorna:
    - date o None
    """

    if valor is None:
        return None

    # Ya es date (pero no datetime)
    if isinstance(valor, date) and not isinstance(valor, datetime):
        return valor

    # datetime → date
    if isinstance(valor, datetime):
        return valor.date()

    # ISO string
    if isinstance(valor, str):
        try:
            return datetime.fromisoformat(valor).date()
        except ValueError:
            return None

    return None
