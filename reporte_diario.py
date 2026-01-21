from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from typing import Dict, Optional, Tuple

import pandas as pd

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas


@dataclass
class DailyReportMeta:
    """Metadata shown in the PDF header."""

    equipo: str = ""
    pozo: str = ""
    etapa: str = ""
    corrida: str = ""
    turno: str = ""
    tipo_agujero: str = ""


def _coerce_date_series(s: pd.Series) -> pd.Series:
    """Accepts 'Fecha' as date/datetime/string and returns datetime.date."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.date
    # try parse
    return pd.to_datetime(s, errors="coerce").dt.date


def split_day(df: pd.DataFrame, day: date, *, date_col: str = "Fecha") -> pd.DataFrame:
    """Filters df to a single day. Returns a copy."""
    if date_col not in df.columns:
        raise KeyError(f"Missing column '{date_col}'")
    s = _coerce_date_series(df[date_col])
    out = df.loc[s == day].copy()
    return out


def summarize_times(df_day: pd.DataFrame) -> Dict[str, float]:
    """Returns totals by Tipo: TP, TNPI, TNP (hours)."""
    if df_day.empty:
        return {"TP": 0.0, "TNPI": 0.0, "TNP": 0.0}

    tipo_col = "Tipo" if "Tipo" in df_day.columns else "Tipo_Tiempo"
    if tipo_col not in df_day.columns:
        # no tipo, assume everything is TP
        total = float(df_day.get("Horas_Reales", pd.Series(dtype=float)).fillna(0).sum())
        return {"TP": total, "TNPI": 0.0, "TNP": 0.0}

    hrs = df_day.get("Horas_Reales", pd.Series(dtype=float)).fillna(0).astype(float)
    by = df_day.assign(_hrs=hrs).groupby(tipo_col, dropna=False)["_hrs"].sum()

    def g(k: str) -> float:
        return float(by.get(k, 0.0))

    return {"TP": g("TP"), "TNPI": g("TNPI"), "TNP": g("TNP")}


def top_causes(
    df_day: pd.DataFrame,
    *,
    tipo: str,
    top_n: int = 10,
) -> pd.DataFrame:
    """Top causes for TNPI or TNP.

    Expects columns:
      - Tipo
      - Categoria_TNPI / Detalle_TNPI or Categoria_TNP / Detalle_TNP
      - Horas_Reales

    Returns a small dataframe with: Categoria, Detalle, Horas.
    """
    if df_day.empty:
        return pd.DataFrame(columns=["Categoria", "Detalle", "Horas"])

    if "Tipo" not in df_day.columns:
        return pd.DataFrame(columns=["Categoria", "Detalle", "Horas"])

    df_t = df_day[df_day["Tipo"] == tipo].copy()
    if df_t.empty:
        return pd.DataFrame(columns=["Categoria", "Detalle", "Horas"])

    if tipo == "TNPI":
        cat_col, det_col = "Categoria_TNPI", "Detalle_TNPI"
    else:
        cat_col, det_col = "Categoria_TNP", "Detalle_TNP"

    for c in (cat_col, det_col):
        if c not in df_t.columns:
            df_t[c] = "-"

    df_t["Horas"] = pd.to_numeric(df_t.get("Horas_Reales", 0), errors="coerce").fillna(0.0)

    g = (
        df_t.groupby([cat_col, det_col], dropna=False)["Horas"]
        .sum()
        .reset_index()
        .sort_values("Horas", ascending=False)
        .head(top_n)
    )
    g.columns = ["Categoria", "Detalle", "Horas"]
    return g


def efficiency(tp: float, tnpi: float, tnp: float) -> float:
    denom = tp + tnpi + tnp
    if denom <= 0:
        return 0.0
    return 100.0 * (tp / denom)


def make_daily_report_pdf(
    df_day: pd.DataFrame,
    *,
    day: date,
    meta: Optional[DailyReportMeta] = None,
    title: str = "Reporte Diario Operativo",
) -> bytes:
    """Generates a simple 1-2 page PDF report as bytes."""

    meta = meta or DailyReportMeta()

    totals = summarize_times(df_day)
    eff = efficiency(totals["TP"], totals["TNPI"], totals["TNP"])

    tnpi_top = top_causes(df_day, tipo="TNPI", top_n=8)
    tnp_top = top_causes(df_day, tipo="TNP", top_n=8)

    # --- PDF
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    width, height = letter

    def hline(y: float):
        c.setLineWidth(0.5)
        c.line(0.75 * inch, y, width - 0.75 * inch, y)

    # Header
    c.setFont("Helvetica-Bold", 16)
    c.drawString(0.75 * inch, height - 0.9 * inch, title)
    c.setFont("Helvetica", 10)
    c.drawString(0.75 * inch, height - 1.15 * inch, f"Fecha: {day.isoformat()}")

    header_lines = [
        ("Equipo", meta.equipo),
        ("Pozo", meta.pozo),
        ("Etapa", meta.etapa),
        ("Corrida", meta.corrida),
        ("Turno", meta.turno),
        ("Tipo agujero", meta.tipo_agujero),
    ]
    x0 = 0.75 * inch
    y0 = height - 1.35 * inch
    c.setFont("Helvetica", 9)
    for i, (k, v) in enumerate(header_lines):
        if not v:
            continue
        c.drawString(x0, y0 - (i * 0.18 * inch), f"{k}: {v}")

    hline(height - 1.75 * inch)

    # Totals
    y = height - 2.1 * inch
    c.setFont("Helvetica-Bold", 12)
    c.drawString(0.75 * inch, y, "Resumen de tiempos")

    y -= 0.25 * inch
    c.setFont("Helvetica", 10)
    c.drawString(0.75 * inch, y, f"TP: {totals['TP']:.2f} h")
    c.drawString(2.4 * inch, y, f"TNPI: {totals['TNPI']:.2f} h")
    c.drawString(4.2 * inch, y, f"TNP: {totals['TNP']:.2f} h")
    c.drawString(5.7 * inch, y, f"Eficiencia: {eff:.1f}%")

    # Helper to draw a simple table
    def draw_table(df: pd.DataFrame, x: float, y: float, w: float, title_: str) -> float:
        c.setFont("Helvetica-Bold", 11)
        c.drawString(x, y, title_)
        y -= 0.2 * inch

        # table header
        c.setFont("Helvetica-Bold", 9)
        c.drawString(x, y, "Categoría")
        c.drawString(x + 2.6 * inch, y, "Detalle")
        c.drawRightString(x + w, y, "Horas")
        y -= 0.15 * inch
        c.setFont("Helvetica", 9)

        # rows
        max_rows = min(len(df), 8)
        for i in range(max_rows):
            row = df.iloc[i]
            cat = str(row.get("Categoria", "-"))[:32]
            det = str(row.get("Detalle", "-"))[:38]
            hrs = float(row.get("Horas", 0.0))
            c.drawString(x, y, cat)
            c.drawString(x + 2.6 * inch, y, det)
            c.drawRightString(x + w, y, f"{hrs:.2f}")
            y -= 0.16 * inch

        return y

    y -= 0.45 * inch
    y_left = draw_table(tnpi_top, 0.75 * inch, y, 3.1 * inch, "Top causas TNPI")
    y_right = draw_table(tnp_top, 4.0 * inch, y, 3.1 * inch, "Top causas TNP")

    y = min(y_left, y_right) - 0.25 * inch

    # Activities snapshot (first 12)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(0.75 * inch, y, "Actividades del día (extracto)")
    y -= 0.25 * inch

    cols = [c for c in ["Operacion", "Actividad", "Tipo", "Horas_Prog", "Horas_Reales"] if c in df_day.columns]
    df_small = df_day[cols].copy() if cols else pd.DataFrame()
    df_small = df_small.head(12)

    # column headers
    c.setFont("Helvetica-Bold", 8)
    x = 0.75 * inch
    col_w = [1.1 * inch, 2.2 * inch, 0.5 * inch, 0.75 * inch, 0.75 * inch]
    for i, col in enumerate(cols):
        c.drawString(x, y, col)
        x += col_w[min(i, len(col_w) - 1)]

    y -= 0.14 * inch
    c.setFont("Helvetica", 8)
    for _, r in df_small.iterrows():
        x = 0.75 * inch
        for i, col in enumerate(cols):
            txt = str(r.get(col, ""))
            txt = txt.replace("\n", " ")
            if col in ("Horas_Prog", "Horas_Reales"):
                try:
                    txt = f"{float(txt):.2f}"
                except Exception:
                    pass
            c.drawString(x, y, txt[:36])
            x += col_w[min(i, len(col_w) - 1)]
        y -= 0.14 * inch
        if y < 1.2 * inch:
            c.showPage()
            y = height - 1.0 * inch
            c.setFont("Helvetica", 8)

    c.showPage()
    c.save()

    return buf.getvalue()


def make_daily_excel(
    df_day: pd.DataFrame,
    *,
    meta: Optional[DailyReportMeta] = None,
) -> bytes:
    """Creates an Excel file (xlsx) with raw data + summary sheets."""
    meta = meta or DailyReportMeta()
    out = BytesIO()

    totals = summarize_times(df_day)
    eff = efficiency(totals["TP"], totals["TNPI"], totals["TNP"])

    summary = pd.DataFrame(
        [
            {"TP_h": totals["TP"], "TNPI_h": totals["TNPI"], "TNP_h": totals["TNP"], "Eficiencia_pct": eff},
        ]
    )

    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        summary.to_excel(xw, index=False, sheet_name="Resumen")
        df_day.to_excel(xw, index=False, sheet_name="Detalle")
        top_causes(df_day, tipo="TNPI").to_excel(xw, index=False, sheet_name="Top_TNPI")
        top_causes(df_day, tipo="TNP").to_excel(xw, index=False, sheet_name="Top_TNP")

        # Metadata
        meta_df = pd.DataFrame(
            [
                {
                    "Equipo": meta.equipo,
                    "Pozo": meta.pozo,
                    "Etapa": meta.etapa,
                    "Corrida": meta.corrida,
                    "Turno": meta.turno,
                    "Tipo_Agujero": meta.tipo_agujero,
                }
            ]
        )
        meta_df.to_excel(xw, index=False, sheet_name="Meta")

    return out.getvalue()
