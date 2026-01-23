def calcular_kpis_globales(df, kpis):
    """
    KPIs globales del dashboard.
    """
    if df is None or df.empty:
        return {
            "importe_total": 0.0,
            "piezas_total": 0,
            "devoluciones_total": 0,
        }

    return {
        "importe_total": float(df["importe"].sum()) if kpis.get("importe") else 0.0,
        "piezas_total": int(df["piezas"].sum()) if kpis.get("piezas") else 0,
        "devoluciones_total": int(df["devoluciones"].sum()) if kpis.get("devoluciones") else 0,
    }


def calcular_kpis(df):
    """
    KPIs por devolución (legacy / analítico).
    """
    dev = df.groupby("devolucion_id")["total_devolucion"].first()

    return {
        "devoluciones": int(dev.count()),
        "importe": float(dev.sum()),
        "promedio": float(dev.mean()) if not dev.empty else 0.0,
    }
