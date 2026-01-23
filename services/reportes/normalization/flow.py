from services.reportes.normalization.ids import normalizar_ids
from services.reportes.normalization.columnas import normalizar_columnas
from services.reportes.normalization.tipos import normalizar_tipos


def normalizar_dataframe(df, kpis):
    """
    Flujo central de normalización del DataFrame de reportes.

    RESPONSABILIDAD:
    - Normalizar IDs
    - Normalizar columnas según KPIs
    - Normalizar tipos
    - Garantizar columnas mínimas
    """

    df = normalizar_ids(df)
    df = normalizar_columnas(df, kpis)
    df = normalizar_tipos(df)

    if "devoluciones" not in df.columns:
        df["devoluciones"] = 1

    if "persona_nombre" not in df.columns:
        df["persona_nombre"] = "Sin asignación"
    else:
        df["persona_nombre"] = (
            df["persona_nombre"]
            .fillna("Sin asignación")
        )

    return df
