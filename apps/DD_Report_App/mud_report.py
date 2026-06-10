"""Mud Report – bitácora de propiedades de fluidos."""
import io
import os
import re
import time
import smtplib
from datetime import datetime
from email.message import EmailMessage

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PLOTLY_CONFIG = {"displayModeBar": False, "displaylogo": False}
PLOTLY_TEMPLATE = "plotly_white"
MUD_SRC_FILES = "files"
MUD_SRC_EMAIL = "email"


def _mud_secret(name: str, default=""):
    env_val = os.getenv(name)
    if env_val is not None and str(env_val).strip():
        return str(env_val).strip()
    try:
        if name in st.secrets:
            return str(st.secrets[name]).strip()
    except Exception:
        pass
    return default


MUD_SMTP_SERVER = _mud_secret("MUD_SMTP_SERVER", _mud_secret("SMTP_SERVER", "smtp.gmail.com"))
MUD_SMTP_PORT = int(_mud_secret("MUD_SMTP_PORT", _mud_secret("SMTP_PORT", "587")))
MUD_SMTP_USER = _mud_secret("MUD_SMTP_USER", _mud_secret("SMTP_USER", ""))
MUD_SMTP_PASS = _mud_secret("MUD_SMTP_PASS", _mud_secret("SMTP_PASS", ""))
MUD_SMTP_FROM = _mud_secret("MUD_SMTP_FROM", MUD_SMTP_USER)
MUD_SMTP_TO = _mud_secret("MUD_SMTP_TO", _mud_secret("SMTP_TO", _mud_secret("TO_EMAIL", "solobox+pemex@rogii.com")))

MUD_IMAP_SERVER = _mud_secret("MUD_IMAP_SERVER", _mud_secret("IMAP_SERVER", "imap.gmail.com"))
MUD_IMAP_USER = _mud_secret("MUD_IMAP_USER", _mud_secret("IMAP_USER", MUD_SMTP_USER))
MUD_IMAP_PASS = _mud_secret("MUD_IMAP_PASS", _mud_secret("IMAP_PASS", MUD_SMTP_PASS))
MUD_IMAP_FILTER = _mud_secret("MUD_IMAP_FILTER", "")


def _safe_numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(dtype=float)
    vals = df[col]
    if isinstance(vals, pd.DataFrame):
        best = vals.iloc[:, 0]
        best_n = pd.to_numeric(best, errors="coerce").notna().sum()
        for i in range(1, vals.shape[1]):
            cand = vals.iloc[:, i]
            cand_n = pd.to_numeric(cand, errors="coerce").notna().sum()
            if cand_n > best_n:
                best, best_n = cand, cand_n
        vals = best
    return pd.to_numeric(vals, errors="coerce")


def is_streamlit_dark_mode() -> bool:
    try:
        return str(st.get_option("theme.base")).lower() == "dark"
    except Exception:
        return False


def apply_pro_theme(fig, h: int = 420):
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        height=h,
        margin=dict(l=50, r=30, t=40, b=55),
        title=dict(x=0.02, xanchor="left"),
        font=dict(family="Segoe UI", size=12, color="#2A2A2A"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)", zeroline=False)
    return fig


def apply_pro_theme_dark(fig, h: int = 420):
    fig.update_layout(
        template="plotly_dark",
        height=h,
        margin=dict(l=50, r=30, t=40, b=55),
        title=dict(x=0.02, xanchor="left"),
        font=dict(family="Segoe UI", size=12, color="#E5E7EB"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)", zeroline=False)
    return fig


def prettify_auto(fig, h: int = 420):
    return apply_pro_theme_dark(fig, h=h) if is_streamlit_dark_mode() else apply_pro_theme(fig, h=h)


def prettify(fig, h: int = 420):
    return apply_pro_theme(fig, h=h)


def prettify_hist(fig, h: int = 420):
    fig.update_layout(template=PLOTLY_TEMPLATE, height=h, margin=dict(l=50, r=30, t=18, b=55))
    return fig


def prettify_heatmap(fig, h: int = 520):
    fig.update_layout(template=PLOTLY_TEMPLATE, height=h, margin=dict(l=60, r=30, t=48, b=60))
    return fig


def prettify_heatmap_auto(fig, h: int = 520):
    if is_streamlit_dark_mode():
        fig.update_layout(template="plotly_dark", height=h, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(15,23,42,0.5)")
        return fig
    return prettify_heatmap(fig, h=h)


def apply_line_area_fill(fig, line_color: str | None = None, fill_alpha: float = 0.22, line_width: float = 2.0, skip_dashed: bool = False) -> go.Figure:
    from plotly.colors import hex_to_rgb

    def _rgba(color, alpha: float) -> str:
        if not color or not str(color).startswith("#"):
            return f"rgba(37,99,235,{alpha})"
        try:
            t = hex_to_rgb(color)
            return f"rgba({int(t[0])},{int(t[1])},{int(t[2])},{alpha})"
        except Exception:
            return f"rgba(37,99,235,{alpha})"

    palette = ["#2563EB", "#EA580C", "#10B981", "#8B5CF6"]
    for i, trace in enumerate(fig.data):
        if getattr(trace, "type", None) != "scatter" or "lines" not in (trace.mode or "lines"):
            continue
        lc = getattr(getattr(trace, "line", None), "color", None) or line_color or palette[i % len(palette)]
        try:
            fig.data[i].update(fill="tozeroy", fillcolor=_rgba(lc, fill_alpha), line=dict(width=line_width, color=lc))
        except Exception:
            pass
    return fig


def format_num(val: float | int | None, digits: int = 2) -> str:
    if val is None or pd.isna(val):
        return "—"
    return f"{val:.{digits}f}"


def series_summary(series: pd.Series) -> str:
    return f"min {format_num(series.min())}, max {format_num(series.max())}, avg {format_num(series.mean())}"


def _render_chips_row(items: list[tuple[str, str]]) -> None:
    if not items:
        return
    try:
        cols = st.columns(len(items))
        for i, (label, color) in enumerate(items):
            with cols[i]:
                st.badge(label, color=color, width="content")
    except Exception:
        st.markdown(" ".join(f":{c}-badge[{l}]" for l, c in items))


def heatmap_numeric_stats(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    cols = [c for c in cols if c in df.columns]
    rows = []
    for c in cols:
        s = _safe_numeric_series(df, c).dropna()
        if s.empty:
            rows.append({"Parámetro": str(c), "Mínimo": np.nan, "Promedio": np.nan, "Máximo": np.nan, "N": 0})
        else:
            rows.append({"Parámetro": str(c), "Mínimo": float(s.min()), "Promedio": float(s.mean()), "Máximo": float(s.max()), "N": int(len(s))})
    return pd.DataFrame(rows)


def stats_df_to_heatmap_chips(stats_df: pd.DataFrame, max_chips: int = 12) -> list[tuple[str, str]]:
    items = []
    for _, r in stats_df.iterrows():
        if int(r.get("N", 0) or 0) < 1:
            continue
        name = str(r["Parámetro"])
        if len(name) > 18:
            name = name[:16] + "…"
        lo, mid, hi = r["Mínimo"], r["Promedio"], r["Máximo"]
        sub = f"{format_num(lo, 1)}–{format_num(hi, 1)} · μ{format_num(mid, 1)}"
        items.append((f"{name}: {sub}", "blue"))
        if len(items) >= max_chips:
            break
    return items


def build_minmax_mean_spine_figure(stats_df: pd.DataFrame, title: str = "Rango por parámetro") -> go.Figure | None:
    if stats_df is None or stats_df.empty or "Parámetro" not in stats_df.columns:
        return None
    fig = go.Figure()
    for _, r in stats_df.iterrows():
        lo, mid, hi = r.get("Mínimo"), r.get("Promedio"), r.get("Máximo")
        p = str(r["Parámetro"])
        if pd.isna(lo) or pd.isna(mid) or pd.isna(hi):
            continue
        lo_f, mid_f, hi_f = float(lo), float(mid), float(hi)
        span = hi_f - lo_f
        ym = 0.5 if span <= 0 or not np.isfinite(span) else float(min(1.0, max(0.0, (mid_f - lo_f) / span)))
        fig.add_trace(go.Scatter(x=[p, p], y=[0.0, 1.0], mode="lines", line=dict(width=3, color="rgba(148,163,184,0.9)"), showlegend=False))
        fig.add_trace(go.Scatter(x=[p], y=[ym], mode="markers", marker=dict(size=11, color="#0ea5e9"), showlegend=False))
    fig.update_layout(title=dict(text=title, x=0.02, xanchor="left"), height=400, template=PLOTLY_TEMPLATE)
    return fig


def build_hist_with_trend(values, title: str, x_label: str, nbins: int = 30) -> go.Figure:
    vals = pd.Series(values).dropna()
    if vals.empty:
        return go.Figure()
    return px.histogram(vals, nbins=nbins, title=title, labels={"value": x_label})


def _sanitize_filename(value: str, default: str = "mud_bitacora") -> str:
    value = (value or "").strip()
    if not value:
        return default
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    value = value.strip("._-")
    return value or default


def _default_mud_bitacora_basename(bitacora: pd.DataFrame) -> str:
    date_label = ""
    try:
        if "Date" in bitacora.columns and bitacora["Date"].notna().any():
            dmax = pd.to_datetime(bitacora["Date"], errors="coerce").dropna().max()
            if pd.notna(dmax):
                date_label = dmax.strftime("%Y-%m-%d")
    except Exception:
        pass
    return f"mud_bitacora_{date_label}" if date_label else "mud_bitacora"

# Mud Report – bitácora de propiedades de fluidos
# =========================
# Aliases por propiedad canónica (nombre en reporte -> clave bitácora)
MUD_PROPERTY_ALIASES = {
    "Density": ["density", "densidad", "mw", "mw (g/l)", "density @ c", "density @", "mud weight", "peso lodo", "densidad sp.gr"],
    "Marsh": ["marsh", "visc. marsh", "viscosidad marsh"],
    "Temperature": ["temperatura salida", "temp. de salida", "temperatura", "temp. de analisis", "temp. de análisis", "temp de analisis"],
    "VA": ["va", "visc.aparente", "visc. aparente", "viscosidad aparente"],
    "FV": ["fv", "fv @ c", "fv @ °c", "funnel viscosity", "viscosidad embudo"],
    "PV": ["pv", "pv (cp)", "pv @ c", "pv @ °c", "plastic viscosity", "viscosidad plástica", "viscoplastic", "visc. plastica", "visc.plastica"],
    "YP": ["yp", "yv", "yield point", "punto de cedencia", "yp (lb/100ft2)", "lb/100ft²", "pc"],
    "Gel_10s": ["gel 10s", "gel 10s/10m/30m", "gels 10s", "10s", "gel (10s)", "gel 10s/10m", "geles"],
    "Gel_10min": ["gel 10m", "gel 10min", "10min", "10m"],
    "Gel_30min": ["gel 30m", "gel 30min", "30min", "30m"],
    "L600": ["lectura 600", "l600"],
    "L300": ["lectura 300", "l300"],
    "L200": ["lectura 200", "l200"],
    "L100": ["lectura 100", "l100"],
    "L6": ["lectura 6", "l6"],
    "L3": ["lectura 3", "l3"],
    "Filtrado": ["filtrado", "filtrate", "fl temp", "hthp", "api filtrate", "fluid loss", "cake (hthp)", "filtrado hpht", "filtrado apat"],
    "Enjarre": ["enjarre", "cake"],
    "LGS": ["lgs", "lgs/hgs", "low gravity solids", "lgs (%)"],
    "HGS": ["hgs", "high gravity solids", "hgs (%)"],
    "Chlorides": ["chlorides", "cloruros", "chlorides (ppm)", "chlorides / calcium"],
    "Solids": ["solids", "corr solid", "solids content %", "sand %", "% sólidos", "% solidos", "sólidos no corregidos", "solidos no corregidos", "solidos corregidos"],
    "Oil": ["% aceite", "aceite %vol", "%oil", "oil"],
    "Water": ["% agua", "agua no correg", "%water", "water"],
    "RAA": ["raa", "r. aceite / agua", "rel. aceite/agua", "aceite/agua"],
    "AgNO3": ["agno3"],
    "Salinity": ["salinidad"],
    "Electrical_Stability": ["est. electrica", "estabilidad", "est. elect", "elec. stability"],
    "Alkalinity": ["alcalinidad"],
    "Excess_Cal": ["exceso de cal", "exc.cal", "exc cal"],
}
MUD_CANONICAL_ORDER = [
    "Date", "DateTime", "Properties", "Depth (MD)", "Depth (TVD)", "Fluid set", "Source", "Time", "FL Temp",
    "Density @ °C", "FV", "FV Temp", "FV @ °C", "PV", "PV Temp", "PV @ °C", "YP",
    "Gel_10s", "Gel_10min", "Gel_30min", "tau0",
    "L600", "L300", "L200", "L100", "L6", "L3",
    "HTHP", "HTHP @ °C", "Corr Solid", "NAP", "Water", "NAP Ratio", "Water Ratio",
    "Sand", "Cake (HTHP)", "Chlorides", "Calcium", "CaCl2", "Water Phase Salinity",
    "NaCL (Sol/Insol)", "Excess Lime", "Electrical_Stability",
    "LGS (%)", "HGS (%)", "LGS (kg/m³)", "HGS (kg/m³)", "ASG",
    "Additional Properties", "n (HB)", "K (HB)", "Viscometer Sag Shoe Test", "(VSST)",
    "Marsh", "Temperature", "VA", "Filtrado", "Enjarre", "LGS", "HGS", "Solids", "Oil", "RAA",
    "AgNO3", "Salinity", "Alkalinity", "Excess_Cal",
]
MUD_METADATA_COLUMNS = {
    "Date", "DateTime", "Properties", "Depth (MD)", "Depth (TVD)", "Fluid set", "Source", "Time",
    "Additional Properties",
}
MUD_ANALYTIC_EXCLUDE = {
    "DateTime", "Properties", "Depth (MD)", "Depth (TVD)", "Fluid set", "Source", "Time",
    "Density @ °C", "FV @ °C", "PV @ °C",
}
MUD_EXPORT_HEADER_SPECS = [
    ("Depth (MD)", "Depth (MD)", "m"),
    ("Depth (TVD)", "Depth (TVD)", "m"),
    ("Properties", "Properties", "N°"),
    ("Fluid set", "Fluid set", "Fluid"),
    ("Source", "Source", "Source"),
    ("Time", "Time", "time"),
    ("DateTime", "DateTime", "YYYY-MM-DDTHH:MM:SS"),
    ("FL Temp", "FL Temp", "°C"),
    ("Density @ °C", "D @ °C", "kg/m³"),
    ("FV @ °C", "Fv @ °C", "s/qt"),
    ("PV @ °C", "PV @ °C", "cP"),
    ("YP", "YP", "lb/100ft²"),
    ("Gel_10s", "GELS 10s", "lb/100ft²"),
    ("Gel_10min", "GELS 10min", "lb/100ft²"),
    ("Gel_30min", "GELS 30min", "lb/100ft²"),
    ("tau0", "tau0", "lb/100ft²"),
    ("L600", "600", "600"),
    ("L300", "300", "300"),
    ("L200", "200", "200"),
    ("L100", "100", "100"),
    ("L6", "6", "6"),
    ("L3", "3", "3"),
    ("HTHP", "HTHP", "HTHP"),
    ("HTHP @ °C", "°C", "°C"),
    ("Corr Solid", "Corr Solid", "%"),
    ("NAP", "NAP", "%"),
    ("Water", "Water", "%"),
    ("NAP Ratio", "NAP", "%"),
    ("Water Ratio", "Water Ratio", "%"),
    ("Sand", "Sand", "%"),
    ("Cake (HTHP)", "Cake (HTHP)", "32nd"),
    ("Chlorides", "Chlorides", "mg/L"),
    ("Calcium", "Calcium", "mg/L"),
    ("CaCl2", "CaCl2", "mg/L"),
    ("Water Phase Salinity", "Water Phase Salinity", "ppm"),
    ("NaCL (Sol/Insol)", "NaCL (Sol/Insol)", "kg/m³"),
    ("Excess Lime", "Excess Lime", "kg/m³"),
    ("Electrical_Stability", "Elec. Stability", "V"),
    ("LGS (%)", "LGS", "%"),
    ("HGS (%)", "HGS", "%"),
    ("LGS (kg/m³)", "LGS", "kg/m³"),
    ("HGS (kg/m³)", "HGS", "kg/m³"),
    ("ASG", "ASG", "SG"),
    ("Additional Properties", "Additional Properties", "Properties"),
    ("n (HB)", "n (HB)", "dec"),
    ("K (HB)", "K (HB)", "lb*s^n'/100ft2"),
    ("Viscometer Sag Shoe Test", "Viscometer Sag Shoe Test", "lbm/gal"),
    ("(VSST)", "(VSST)", ""),
]


def _normalize_mud_property_name(label: str) -> str | None:
    """Mapea etiqueta de reporte a nombre canónico."""
    if not label or not isinstance(label, str):
        return None
    key = str(label).strip().lower()
    key = re.sub(r"\s+", " ", key)
    for canonical, aliases in MUD_PROPERTY_ALIASES.items():
        for a in aliases:
            if a in key or key in a:
                return canonical
    if "gel" in key and "10s" in key:
        return "Gel_10s"
    if "gel" in key and "10" in key and "30" not in key:
        return "Gel_10min"
    if "gel" in key and "30" in key:
        return "Gel_30min"
    return None


def _extract_numeric(val) -> float | None:
    """Extrae un número de una celda o string libre."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    s = s.replace(" ", " ")
    # quitar miles y normalizar decimales
    s = re.sub(r"(?<=\d),(?=\d{3}(?:\D|$))", "", s)
    s = s.replace(",", ".")
    m = re.search(r"[-+]?\d*\.?\d+", s)
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            return None
    return None


def _extract_all_numbers(val) -> list[float]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return []
    s = str(val).strip().replace(" ", " ")
    if not s:
        return []
    s = re.sub(r"(?<=\d),(?=\d{3}(?:\D|$))", "", s)
    s = s.replace(",", ".")
    nums = []
    for tok in re.findall(r"[-+]?\d*\.?\d+", s):
        try:
            nums.append(float(tok))
        except ValueError:
            pass
    return nums


def _parse_gel_triple(val) -> tuple[float | None, float | None, float | None]:
    """Parsea '10/15/17' -> (10, 15, 17) y variantes '8/16/22'."""
    nums = _extract_all_numbers(val)
    if len(nums) >= 3:
        return nums[0], nums[1], nums[2]
    if len(nums) == 2:
        return nums[0], nums[1], None
    if len(nums) == 1:
        return nums[0], None, None
    return None, None, None


def _extract_date_from_text(text: str) -> pd.Timestamp | None:
    if not text:
        return None
    month_map = {
        "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
        "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
    }
    # yyyy-mm-dd / yyyy.mm.dd / yyyy/mm/dd
    m = re.search(r"((?:19|20)\d{2})[./-](\d{1,2})[./-](\d{1,2})", text)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return pd.Timestamp(y, mo, d).normalize()
        except ValueError:
            pass
    # dd-mm-yyyy / dd/mm/yyyy
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", text)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        try:
            return pd.Timestamp(y, mo, d).normalize()
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})-([A-Za-z]{3})-(\d{2,4})", text)
    if m:
        d = int(m.group(1))
        mo = month_map.get(m.group(2).lower()[:3])
        y = int(m.group(3))
        if y < 100:
            y += 2000
        if mo:
            try:
                return pd.Timestamp(y, mo, d).normalize()
            except ValueError:
                pass
    return None


def _date_from_filename_or_today(name: str) -> pd.Timestamp:
    """Extrae fecha de nombre de archivo o usa hoy."""
    if not name:
        return pd.Timestamp.now().normalize()
    d = _extract_date_from_text(name)
    return d if d is not None else pd.Timestamp.now().normalize()


def _mud_num_to_text(val) -> str:
    num = _extract_numeric(val)
    if num is None:
        s = str(val).strip() if val is not None else ""
        return s
    if abs(num - round(num)) < 1e-9:
        return str(int(round(num)))
    return f"{num:.12g}"


def _mud_clean_cell_text(val) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    try:
        import datetime as _dt
        if isinstance(val, _dt.time):
            return val.strftime("%H:%M")
    except Exception:
        pass
    return str(val).strip().replace("\u00a0", " ")


def _mud_parse_time_value(val):
    s = _mud_clean_cell_text(val)
    if not s:
        return None
    try:
        import datetime as _dt
        if hasattr(val, "hour") and hasattr(val, "minute") and not isinstance(val, pd.Timestamp):
            return _dt.time(val.hour, val.minute, getattr(val, "second", 0))
    except Exception:
        pass
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except Exception:
            pass
    dt = pd.to_datetime(s, errors="coerce")
    if pd.notna(dt):
        return dt.time()
    return None


def _mud_compose_datetime(date_value, time_value) -> pd.Timestamp | None:
    base_date = pd.to_datetime(date_value, errors="coerce")
    if pd.isna(base_date):
        return None
    t = _mud_parse_time_value(time_value)
    if t is None:
        return base_date
    return pd.Timestamp.combine(base_date.normalize().date(), t)


def _mud_isoformat_no_tz(ts) -> str:
    ts = pd.to_datetime(ts, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def _mud_pair_string(raw_value) -> str:
    nums = _extract_all_numbers(raw_value)
    if len(nums) >= 2:
        return f"{_mud_num_to_text(nums[0])} @ {_mud_num_to_text(nums[1])}"
    if len(nums) == 1:
        return _mud_num_to_text(nums[0])
    return _mud_clean_cell_text(raw_value)


def _mud_apply_daily_property(row_record: dict, label: str, unit: str, raw_value) -> None:
    low = re.sub(r"\s+", " ", _mud_clean_cell_text(label).lower())
    unit_low = re.sub(r"\s+", " ", _mud_clean_cell_text(unit).lower())
    nums = _extract_all_numbers(raw_value)
    if not low:
        return
    if low.startswith("depth"):
        if len(nums) >= 1:
            row_record["Depth (MD)"] = nums[0]
        if len(nums) >= 2:
            row_record["Depth (TVD)"] = nums[1]
        return
    if low.startswith("fl temp"):
        if nums:
            row_record["FL Temp"] = nums[0]
        return
    if low.startswith("density"):
        row_record["Density @ °C"] = _mud_pair_string(raw_value)
        if nums:
            row_record["Density"] = nums[0]
        if len(nums) >= 2:
            row_record["Density Temp"] = nums[1]
        return
    if low.startswith("fv @"):
        if nums:
            row_record["FV"] = nums[0]
        if len(nums) >= 2:
            row_record["FV Temp"] = nums[1]
        row_record["FV @ °C"] = _mud_pair_string(raw_value)
        return
    if low.startswith("pv @"):
        if nums:
            row_record["PV"] = nums[0]
        if len(nums) >= 2:
            row_record["PV Temp"] = nums[1]
        row_record["PV @ °C"] = _mud_pair_string(raw_value)
        return
    if low == "yp" or low.startswith("yp "):
        if nums:
            row_record["YP"] = nums[0]
        return
    if low.startswith("gels"):
        g1, g2, g3 = _parse_gel_triple(raw_value)
        if g1 is not None:
            row_record["Gel_10s"] = g1
        if g2 is not None:
            row_record["Gel_10min"] = g2
        if g3 is not None:
            row_record["Gel_30min"] = g3
        return
    if low == "tau0":
        if nums:
            row_record["tau0"] = nums[0]
        return
    if low.startswith("600/300"):
        if len(nums) >= 1:
            row_record["L600"] = nums[0]
        if len(nums) >= 2:
            row_record["L300"] = nums[1]
        return
    if low.startswith("200/100"):
        if len(nums) >= 1:
            row_record["L200"] = nums[0]
        if len(nums) >= 2:
            row_record["L100"] = nums[1]
        return
    if low.startswith("6/3"):
        if len(nums) >= 1:
            row_record["L6"] = nums[0]
        if len(nums) >= 2:
            row_record["L3"] = nums[1]
        return
    if low.startswith("hthp"):
        if nums:
            row_record["HTHP"] = nums[0]
        if len(nums) >= 2:
            row_record["HTHP @ °C"] = nums[1]
        return
    if low.startswith("corr solid"):
        if nums:
            row_record["Corr Solid"] = nums[0]
        return
    if low.startswith("nap / water ratio"):
        if len(nums) >= 1:
            row_record["NAP Ratio"] = nums[0]
        if len(nums) >= 2:
            row_record["Water Ratio"] = nums[1]
        return
    if low.startswith("nap / water"):
        if len(nums) >= 1:
            row_record["NAP"] = nums[0]
        if len(nums) >= 2:
            row_record["Water"] = nums[1]
        return
    if low == "sand" or low.startswith("sand "):
        if nums:
            row_record["Sand"] = nums[0]
        return
    if low.startswith("cake"):
        if nums:
            row_record["Cake (HTHP)"] = nums[0]
        return
    if low.startswith("chlorides / calcium"):
        if len(nums) >= 1:
            row_record["Chlorides"] = nums[0]
        if len(nums) >= 2:
            row_record["Calcium"] = nums[1]
        return
    if low == "cacl2" or low.startswith("cacl2 "):
        if nums:
            row_record["CaCl2"] = nums[0]
        return
    if low.startswith("water phase salinity"):
        if nums:
            row_record["Water Phase Salinity"] = nums[0]
        return
    if low.startswith("nacl"):
        txt = _mud_clean_cell_text(raw_value)
        if txt and txt != "/":
            row_record["NaCL (Sol/Insol)"] = txt
        return
    if low.startswith("excess lime"):
        if nums:
            row_record["Excess Lime"] = nums[0]
        return
    if low.startswith("elec. stability"):
        if nums:
            row_record["Electrical_Stability"] = nums[0]
        return
    if low.startswith("lgs / hgs"):
        if len(nums) >= 1:
            target_lgs = "LGS (kg/m³)" if "kg/" in unit_low else "LGS (%)"
            row_record[target_lgs] = nums[0]
        if len(nums) >= 2:
            target_hgs = "HGS (kg/m³)" if "kg/" in unit_low else "HGS (%)"
            row_record[target_hgs] = nums[1]
        return
    if low == "asg":
        if nums:
            row_record["ASG"] = nums[0]
        return
    if low.startswith("n (hb)"):
        if nums:
            row_record["n (HB)"] = nums[0]
        return
    if low.startswith("k (hb)"):
        if nums:
            row_record["K (HB)"] = nums[0]
        return
    if low.startswith("viscometer sag shoe test"):
        if nums:
            row_record["Viscometer Sag Shoe Test"] = nums[0]
        return
    if low.startswith("(vsst)"):
        if nums:
            row_record["(VSST)"] = nums[0]
        else:
            txt = _mud_clean_cell_text(raw_value)
            if txt:
                row_record["(VSST)"] = txt
        return
    canonical = _normalize_mud_property_name(label)
    if canonical:
        _mud_apply_canonical_value(row_record, canonical, raw_value)


def _parse_mud_daily_report_sheet(df_raw: pd.DataFrame, source_name: str = "") -> list[dict]:
    df = df_raw.copy()
    if df.empty or df.shape[0] < 6 or df.shape[1] < 5:
        return []
    cell_a1 = _mud_clean_cell_text(df.iat[0, 0]) if df.shape[0] > 0 else ""
    cell_a2 = _mud_clean_cell_text(df.iat[1, 0]) if df.shape[0] > 1 else ""
    cell_a5 = _mud_clean_cell_text(df.iat[4, 0]) if df.shape[0] > 4 else ""
    if "daily fluid properties" not in cell_a1.lower() or "properties" not in cell_a2.lower() or "time" not in cell_a5.lower():
        return []

    report_date = _extract_date_from_text(cell_a1) or _date_from_filename_or_today(source_name)
    sample_cols = []
    for j in range(2, df.shape[1]):
        prop_txt = _mud_clean_cell_text(df.iat[1, j]) if df.shape[0] > 1 else ""
        fluid_txt = _mud_clean_cell_text(df.iat[2, j]) if df.shape[0] > 2 else ""
        src_txt = _mud_clean_cell_text(df.iat[3, j]) if df.shape[0] > 3 else ""
        time_txt = _mud_clean_cell_text(df.iat[4, j]) if df.shape[0] > 4 else ""
        if time_txt or fluid_txt or src_txt or (prop_txt and (fluid_txt or src_txt)):
            sample_cols.append(j)
    if not sample_cols:
        return []

    records: list[dict] = []
    for idx, j in enumerate(sample_cols, start=1):
        prop_id = _extract_numeric(df.iat[1, j]) if df.shape[0] > 1 else None
        time_raw = df.iat[4, j] if df.shape[0] > 4 else None
        ts = _mud_compose_datetime(report_date, time_raw)
        rec = {
            "Date": ts if ts is not None else report_date,
            "DateTime": _mud_isoformat_no_tz(ts if ts is not None else report_date),
            "Properties": int(prop_id) if prop_id is not None else idx,
            "Fluid set": _mud_clean_cell_text(df.iat[2, j]) if df.shape[0] > 2 else "",
            "Source": _mud_clean_cell_text(df.iat[3, j]) if df.shape[0] > 3 else source_name,
            "Time": _mud_parse_time_value(time_raw).strftime("%H:%M") if _mud_parse_time_value(time_raw) else _mud_clean_cell_text(time_raw),
            "Additional Properties": int(prop_id) if prop_id is not None else idx,
        }
        records.append(rec)

    row_texts = []
    for i in range(df.shape[0]):
        row_texts.append(" ".join(_mud_clean_cell_text(df.iat[i, c]) for c in range(df.shape[1]) if _mud_clean_cell_text(df.iat[i, c])))

    for i in range(5, df.shape[0]):
        label = _mud_clean_cell_text(df.iat[i, 0])
        if not label:
            continue
        unit = _mud_clean_cell_text(df.iat[i, 1]) if df.shape[1] > 1 else ""
        raw_vals = [df.iat[i, j] for j in sample_cols]
        whole_nums = _extract_all_numbers(row_texts[i])
        use_sequence = len(whole_nums) == len(sample_cols) and any(
            (not _mud_clean_cell_text(v)) or len(_extract_all_numbers(v)) != 1 for v in raw_vals
        )
        for idx, rec in enumerate(records):
            raw_value = whole_nums[idx] if use_sequence else raw_vals[idx]
            if not _mud_clean_cell_text(raw_value) and not isinstance(raw_value, (int, float)):
                continue
            _mud_apply_daily_property(rec, label, unit, raw_value)

    return [r for r in records if any(
        k not in MUD_METADATA_COLUMNS and pd.notna(v) and _mud_clean_cell_text(v) not in ("", "/")
        for k, v in r.items()
    )]


def _mud_apply_canonical_value(row_record: dict, canonical: str, raw_value) -> None:
    if canonical.startswith("Gel"):
        g1, g2, g3 = _parse_gel_triple(raw_value)
        if g1 is not None:
            row_record["Gel_10s"] = g1
        if g2 is not None:
            row_record["Gel_10min"] = g2
        if g3 is not None:
            row_record["Gel_30min"] = g3
        return
    if canonical == "RAA":
        nums = _extract_all_numbers(raw_value)
        if len(nums) >= 1:
            row_record[canonical] = nums[0]
        return
    num = _extract_numeric(raw_value)
    if num is not None:
        row_record[canonical] = num


def _parse_mud_text_block(text: str, row_record: dict) -> None:
    if not text:
        return
    text = text.replace("\u00a0", " ")
    if pd.isna(row_record.get("Date")) or row_record.get("Date") is None:
        d = _extract_date_from_text(text)
        if d is not None:
            row_record["Date"] = d

    patterns = [
        ("Density", [r"densidad[^\n\r:]*[: ]+([0-9.,]+)", r"density[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Marsh", [r"visc\.? marsh[^\n\r:]*[: ]+([0-9.,]+)", r"viscosidad marsh[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Temperature", [r"temperatura salida[^\n\r:]*[: ]+([0-9.,]+)", r"temp\. de salida[^\n\r:]*[: ]+([0-9.,]+)", r"temp\. de an[aá]lisis[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("VA", [r"visc\.?aparente(?:\(va\))?[^\n\r:]*[: ]+([0-9.,]+)", r"visc\.? aparente[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("PV", [r"visc\.?plastica(?:\(vp\))?[^\n\r:]*[: ]+([0-9.,]+)", r"visc\.? plastica[^\n\r:]*[: ]+([0-9.,]+)", r"\bPV\b[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("YP", [r"punto cedente(?:\(yp\))?[^\n\r:]*[: ]+([0-9.,]+)", r"\bPC\b[^\n\r:]*[: ]+([0-9.,]+)", r"\bYP\b[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("L600", [r"lectura 600[^\n\r:]*[: ]+([0-9.,]+)", r"l600[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("L300", [r"lectura 300[^\n\r:]*[: ]+([0-9.,]+)", r"l300[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("L200", [r"lectura 200[^\n\r:]*[: ]+([0-9.,]+)", r"l200[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("L100", [r"lectura 100[^\n\r:]*[: ]+([0-9.,]+)", r"l100[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("L6", [r"lectura 6[^\n\r:]*[: ]+([0-9.,]+)", r"l6[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("L3", [r"lectura 3[^\n\r:]*[: ]+([0-9.,]+)", r"l3[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Filtrado", [r"filtrado hpht[^\n\r:]*[: ]+([0-9.,]+)", r"filtrado apat[^\n\r:]*[: ]+([0-9.,]+)", r"filtrado[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Enjarre", [r"enjarre[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Solids", [r"%\s*s[óo]lidos[^\n\r:]*[: ]+([0-9.,]+)", r"s[óo]lidos no corregidos[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Oil", [r"%\s*aceite[^\n\r:]*[: ]+([0-9.,]+)", r"aceite\s*%vol[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Water", [r"%\s*agua[^\n\r:]*[: ]+([0-9.,]+)", r"agua no correg[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("RAA", [r"raa[^\n\r:]*[: ]+([0-9.,]+)", r"rel\. aceite/agua[^\n\r:]*[: ]+([0-9.,]+)", r"aceite/agua[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("AgNO3", [r"agno3[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Chlorides", [r"cloruros[^\n\r:]*[: ]+([0-9.,]+)", r"chlorides[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Salinity", [r"salinidad[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Electrical_Stability", [r"est\.? electrica[^\n\r:]*[: ]+([0-9.,]+)", r"estabilidad[^\n\r:]*[: ]+([0-9.,]+)", r"est\.? elect\.?[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Alkalinity", [r"alcalinidad[^\n\r:]*[: ]+([0-9.,]+)"]),
        ("Excess_Cal", [r"exceso de cal[^\n\r:]*[: ]+([0-9.,]+)", r"exc\.cal[^\n\r:]*[: ]+([0-9.,]+)"]),
    ]
    for canonical, regexes in patterns:
        if canonical in row_record and pd.notna(row_record.get(canonical)):
            continue
        for pat in regexes:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                _mud_apply_canonical_value(row_record, canonical, m.group(1))
                break

    m = re.search(r"geles?[^\n\r:]*[: ]+([0-9.,/ ]+)", text, flags=re.IGNORECASE)
    if m:
        _mud_apply_canonical_value(row_record, "Gel_10s", m.group(1))
    m = re.search(r"gel\s*10s/10m[^\n\r:]*[: ]+([0-9.,/ ]+)", text, flags=re.IGNORECASE)
    if m:
        _mud_apply_canonical_value(row_record, "Gel_10s", m.group(1))




def _parse_mud_lines(text: str, row_record: dict) -> None:
    if not text:
        return
    for raw_line in text.splitlines():
        line = (raw_line or "").strip()
        if not line:
            continue
        low = re.sub(r"\s+", " ", line.lower())
        nums = _extract_all_numbers(line)
        if not nums:
            continue

        if low.startswith("densidad") or low.startswith("density"):
            row_record["Density @ °C"] = _mud_pair_string(line)
            row_record["Density"] = nums[0] if nums else row_record.get("Density")
            if len(nums) >= 2:
                row_record["Density Temp"] = nums[1]
        elif low.startswith("visc. marsh") or low.startswith("viscosidad marsh"):
            row_record["Marsh"] = nums[-1]
        elif low.startswith("temperatura salida") or low.startswith("temp. de salida"):
            row_record["Temperature"] = nums[-1]
        elif low.startswith("temp. de analisis") or low.startswith("temp. de análisis"):
            row_record["Temperature"] = nums[-1]
        elif low.startswith("visc.aparente") or low.startswith("visc. aparente") or low.startswith("viscosidad aparente"):
            row_record["VA"] = nums[-1]
        elif low.startswith("visc.plastica") or low.startswith("visc. plastica") or low.startswith("pv ") or low == "pv":
            row_record["PV"] = nums[-1]
        elif low.startswith("punto cedente") or low.startswith("pc ") or low == "pc" or low.startswith("yp ") or low == "yp":
            row_record["YP"] = nums[-1]
        elif low.startswith("lectura 600"):
            row_record["L600"] = nums[0]
        elif low.startswith("lectura 300"):
            row_record["L300"] = nums[0]
        elif low.startswith("lectura 200"):
            row_record["L200"] = nums[0]
        elif low.startswith("lectura 100"):
            row_record["L100"] = nums[0]
        elif low.startswith("lectura 6"):
            row_record["L6"] = nums[0]
        elif low.startswith("lectura 3"):
            row_record["L3"] = nums[0]
        elif low.startswith("l600/l300") and len(nums) >= 2:
            row_record["L600"] = nums[0]
            row_record["L300"] = nums[1]
        elif low.startswith("l200/l100") and len(nums) >= 2:
            row_record["L200"] = nums[0]
            row_record["L100"] = nums[1]
        elif low.startswith("l6/l3") and len(nums) >= 2:
            row_record["L6"] = nums[0]
            row_record["L3"] = nums[1]
        elif low.startswith("filtrado hpht") or low.startswith("filtrado apat") or low.startswith("filtrado"):
            row_record["Filtrado"] = nums[-1]
        elif low.startswith("enjarre"):
            row_record["Enjarre"] = nums[-1]
        elif low.startswith("geles") or low.startswith("gel 10s/10m"):
            if len(nums) >= 1:
                row_record["Gel_10s"] = nums[0]
            if len(nums) >= 2:
                row_record["Gel_10min"] = nums[1]
            if len(nums) >= 3:
                row_record["Gel_30min"] = nums[2]
        elif low.startswith("% sólidos") or low.startswith("% solidos") or low.startswith("sólidos no corregidos") or low.startswith("solidos no corregidos"):
            row_record["Solids"] = nums[-1]
        elif low.startswith("% aceite") or low.startswith("aceite %vol"):
            row_record["Oil"] = nums[-1]
        elif low.startswith("% agua") or low.startswith("agua no correg"):
            row_record["Water"] = nums[-1]
        elif low.startswith("raa") or low.startswith("rel. aceite/agua") or low.startswith("aceite/agua"):
            row_record["RAA"] = nums[-2] if len(nums) >= 2 else nums[0]
        elif low.startswith("agno3"):
            row_record["AgNO3"] = nums[-1]
        elif low.startswith("cloruros"):
            row_record["Chlorides"] = nums[-1]
        elif low.startswith("salinidad"):
            row_record["Salinity"] = nums[-1]
        elif low.startswith("est. electrica") or low.startswith("estabilidad") or low.startswith("est. elect"):
            row_record["Electrical_Stability"] = nums[-1]
        elif low.startswith("alcalinidad"):
            row_record["Alkalinity"] = nums[-1]
        elif low.startswith("exceso de cal") or low.startswith("exc.cal"):
            row_record["Excess_Cal"] = nums[-1]
def _parse_mud_excel_sheet(df_raw: pd.DataFrame, source_name: str = "") -> list[dict]:
    """Parsea una hoja Excel de propiedades de lodo (formato filas propiedad/valor o tabla)."""
    daily_rows = _parse_mud_daily_report_sheet(df_raw, source_name)
    if daily_rows:
        return daily_rows

    out: list[dict] = []
    date = _date_from_filename_or_today(source_name)
    row_record: dict = {"Date": date, "Source": source_name}

    df = df_raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    prop_col = None
    for c in df.columns:
        cl = c.lower()
        if "propert" in cl or "parámetro" in cl or "parameter" in cl or c == "Unnamed: 0" or cl == "0":
            prop_col = c
            break
    if prop_col is None and len(df.columns) >= 1:
        prop_col = df.columns[0]

    if prop_col is not None:
        for _, r in df.iterrows():
            label = r.get(prop_col)
            if pd.isna(label):
                continue
            canonical = _normalize_mud_property_name(str(label))
            if canonical:
                for c in df.columns:
                    if c == prop_col:
                        continue
                    v = r.get(c)
                    if pd.isna(v):
                        continue
                    _mud_apply_canonical_value(row_record, canonical, v)
                    if canonical in row_record or canonical.startswith("Gel"):
                        break

    if not any(k for k in row_record if k not in ("Date", "Source")):
        for col in df.columns:
            canonical = _normalize_mud_property_name(col)
            if canonical:
                vals = df[col].dropna().tolist()
                if vals:
                    _mud_apply_canonical_value(row_record, canonical, vals[0])

    text_blob = "\n".join(
        " ".join(str(v) for v in row.tolist() if pd.notna(v))
        for _, row in df.iterrows()
    )
    _parse_mud_text_block(text_blob, row_record)
    _parse_mud_lines(text_blob, row_record)

    if any(k for k in row_record if k not in ("Date", "Source")):
        out.append(row_record)
    return out


def _parse_mud_csv(df_raw: pd.DataFrame, source_name: str = "") -> list[dict]:
    """Parsea CSV de propiedades de lodo (igual lógica que Excel)."""
    return _parse_mud_excel_sheet(df_raw, source_name)


def _parse_mud_pdf(file, source_name: str = "") -> list[dict]:
    """Extrae tablas/texto de PDF y parsea propiedades conocidas."""
    out: list[dict] = []
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        return out
    name = source_name or getattr(file, "name", "") or ""
    row_record: dict = {"Date": _date_from_filename_or_today(name), "Source": name}
    try:
        with pdfplumber.open(file) as pdf:
            full_text_parts = []
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if page_text:
                    full_text_parts.append(page_text)
                tables = page.extract_tables() or []
                for table in tables:
                    for row in table or []:
                        if not row:
                            continue
                        for idx, cell in enumerate(row):
                            if cell is None:
                                continue
                            canonical = _normalize_mud_property_name(str(cell))
                            if not canonical:
                                continue
                            for other in row[idx + 1:]:
                                if other is None:
                                    continue
                                _mud_apply_canonical_value(row_record, canonical, other)
                                if canonical in row_record or canonical.startswith("Gel"):
                                    break
                        full_text = "\n".join(full_text_parts)
            dt_pdf = _extract_date_from_text(full_text)
            if dt_pdf is not None:
                row_record["Date"] = dt_pdf
            _parse_mud_text_block(full_text, row_record)
            _parse_mud_lines(full_text, row_record)
        if any(k for k in row_record if k not in ("Date", "Source")):
            out.append(row_record)
    except Exception:
        pass
    return out


def _fetch_mud_attachments_from_email(
    imap_server: str,
    imap_user: str,
    imap_pass: str,
    filename_contains: str | None = None,
    mark_read: bool = True,
) -> list[tuple[str, bytes]]:
    """
    Descarga adjuntos PDF/Excel/CSV de correos no leídos por IMAP.
    filename_contains: filtro opcional (ej. "Daily Full Report" o "LA-358").
    mark_read: si True, marca los correos como leídos tras descargar.
    Retorna lista de (nombre_archivo, contenido_bytes).
    """
    results: list[tuple[str, bytes]] = []
    try:
        import imaplib
        import email as email_module
    except ImportError:
        return results
    try:
        with imaplib.IMAP4_SSL(imap_server, timeout=30) as mail:
            mail.login(imap_user, imap_pass)
            mail.select("inbox")
            status, messages = mail.search(None, "(UNSEEN)")
            if status != "OK":
                return results
            for num in (messages[0] or b"").split():
                if not num:
                    continue
                status, data = mail.fetch(num, "(RFC822)")
                if status != "OK":
                    continue
                msg = email_module.message_from_bytes(data[0][1])
                for part in msg.walk():
                    if part.get_content_disposition() != "attachment":
                        continue
                    filename = part.get_filename()
                    if not filename:
                        continue
                    filename = str(filename).strip()
                    ext = (filename or "").lower()
                    if not (
                        ext.endswith(".pdf")
                        or ext.endswith(".xlsx")
                        or ext.endswith(".xls")
                        or ext.endswith(".csv")
                    ):
                        continue
                    if filename_contains and filename_contains.strip():
                        if filename_contains.strip().lower() not in filename.lower():
                            continue
                    payload = part.get_payload(decode=True)
                    if payload:
                        results.append((filename, bytes(payload)))
                if mark_read and results:
                    try:
                        mail.store(num, "+FLAGS", "\\Seen")
                    except Exception:
                        pass
    except Exception:
        raise
    return results


def _build_mud_bitacora(parsed_rows: list[dict]) -> pd.DataFrame:
    """Construye DataFrame bitácora con columnas canónicas."""
    if not parsed_rows:
        return pd.DataFrame()
    all_keys = set()
    for r in parsed_rows:
        all_keys.update(r.keys())
    cols = [c for c in MUD_CANONICAL_ORDER if c in all_keys]
    for c in sorted(all_keys):
        if c not in cols:
            cols.append(c)
    rows = []
    for r in parsed_rows:
        row = {}
        for k in cols:
            row[k] = r.get(k)
        rows.append(row)
    df = pd.DataFrame(rows)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    if "DateTime" in df.columns:
        df["DateTime"] = df["DateTime"].fillna("")
    return df


def _mud_numeric_property_columns(bitacora: pd.DataFrame) -> list[str]:
    preferred = [
        "FL Temp", "Density @ °C", "FV", "FV Temp", "PV", "PV Temp", "YP",
        "Gel_10s", "Gel_10min", "Gel_30min", "tau0",
        "L600", "L300", "L200", "L100", "L6", "L3",
        "HTHP", "HTHP @ °C", "Corr Solid", "NAP", "Water", "NAP Ratio", "Water Ratio",
        "Sand", "Cake (HTHP)", "Chlorides", "Calcium", "CaCl2", "Water Phase Salinity",
        "Excess Lime", "Electrical_Stability", "LGS (%)", "HGS (%)", "LGS (kg/m³)", "HGS (kg/m³)",
        "ASG", "n (HB)", "K (HB)", "Viscometer Sag Shoe Test", "(VSST)",
        "Marsh", "Temperature", "VA", "Filtrado", "Enjarre", "LGS", "HGS", "Solids", "Oil", "RAA",
        "AgNO3", "Salinity", "Alkalinity", "Excess_Cal",
    ]
    cols = []
    for c in preferred:
        if c in bitacora.columns and pd.api.types.is_numeric_dtype(bitacora[c]):
            cols.append(c)
    for c in bitacora.columns:
        if c in cols or c == "Date" or c in MUD_ANALYTIC_EXCLUDE:
            continue
        if pd.api.types.is_numeric_dtype(bitacora[c]):
            cols.append(c)
    return cols


def _mud_build_view_df(bitacora: pd.DataFrame) -> pd.DataFrame:
    if bitacora is None or bitacora.empty:
        return pd.DataFrame()
    view = bitacora.copy()
    if "DateTime" not in view.columns and "Date" in view.columns:
        view["DateTime"] = pd.to_datetime(view["Date"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        dt_series = pd.to_datetime(view.get("Date"), errors="coerce")
        mask = view["DateTime"].astype(str).str.strip().eq("")
        if mask.any():
            view.loc[mask, "DateTime"] = dt_series.loc[mask].dt.strftime("%Y-%m-%dT%H:%M:%S")
    if "Time" not in view.columns and "Date" in view.columns:
        view["Time"] = pd.to_datetime(view["Date"], errors="coerce").dt.strftime("%H:%M")
    if "FV @ °C" not in view.columns and "FV" in view.columns:
        if "FV Temp" in view.columns:
            view["FV @ °C"] = view.apply(lambda r: f"{_mud_num_to_text(r['FV'])} @ {_mud_num_to_text(r['FV Temp'])}" if pd.notna(r.get("FV")) and pd.notna(r.get("FV Temp")) else (_mud_num_to_text(r['FV']) if pd.notna(r.get("FV")) else ""), axis=1)
        else:
            view["FV @ °C"] = view["FV"].map(_mud_num_to_text)
    if "PV @ °C" not in view.columns and "PV" in view.columns:
        if "PV Temp" in view.columns:
            view["PV @ °C"] = view.apply(lambda r: f"{_mud_num_to_text(r['PV'])} @ {_mud_num_to_text(r['PV Temp'])}" if pd.notna(r.get("PV")) and pd.notna(r.get("PV Temp")) else (_mud_num_to_text(r['PV']) if pd.notna(r.get("PV")) else ""), axis=1)
        else:
            view["PV @ °C"] = view["PV"].map(_mud_num_to_text)
    if "Properties" not in view.columns:
        view["Properties"] = np.arange(1, len(view) + 1)
    if "Additional Properties" not in view.columns:
        view["Additional Properties"] = view["Properties"]
    export_cols = [c for c, _, _ in MUD_EXPORT_HEADER_SPECS if c in view.columns]
    for c in export_cols:
        if c in ("DateTime", "Time"):
            view[c] = view[c].fillna("")
    return view[export_cols]


def _export_mud_bitacora_excel(view_df: pd.DataFrame) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Table 1.1"

    headers = [(c, h1, h2) for c, h1, h2 in MUD_EXPORT_HEADER_SPECS if c in view_df.columns]
    last_col = len(headers)
    if last_col == 0:
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return buf.getvalue()

    title_date = ""
    if "Date" in view_df.columns:
        dt0 = pd.to_datetime(view_df["Date"], errors="coerce")
        if hasattr(dt0, "notna") and dt0.notna().any():
            title_date = dt0.dropna().min().strftime("%Y-%m-%d")
    elif "DateTime" in view_df.columns:
        dt0 = pd.to_datetime(view_df["DateTime"], errors="coerce")
        if hasattr(dt0, "notna") and dt0.notna().any():
            title_date = dt0.dropna().min().strftime("%Y-%m-%d")
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=last_col)
    ws.cell(1, 1).value = f"Daily Fluid Properties Daily Report\nReport: {title_date}" if title_date else "Daily Fluid Properties Daily Report"
    ws.cell(1, 1).alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ws.cell(1, 1).font = Font(size=14, bold=True)
    ws.row_dimensions[1].height = 34

    fill_header = PatternFill("solid", fgColor="D9D9D9")
    fill_sub = PatternFill("solid", fgColor="EDEDED")
    thin_gray = Side(style="thin", color="BFBFBF")
    border = Border(top=thin_gray, bottom=thin_gray)

    for idx, (_, h1, h2) in enumerate(headers, start=1):
        c2 = ws.cell(2, idx, h1)
        c3 = ws.cell(3, idx, h2)
        for cell in (c2, c3):
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.font = Font(bold=True)
            cell.border = border
        c2.fill = fill_header
        c3.fill = fill_sub

    widths = {
        "Depth (MD)": 11, "Depth (TVD)": 11, "Properties": 10, "Fluid set": 16, "Source": 14,
        "Time": 10, "DateTime": 22, "FL Temp": 10, "Density @ °C": 14,
        "FV @ °C": 12, "PV @ °C": 12, "YP": 10, "Gel_10s": 10, "Gel_10min": 11,
        "Gel_30min": 11, "tau0": 10, "L600": 9, "L300": 9, "L200": 9, "L100": 9, "L6": 9, "L3": 9,
        "HTHP": 10, "HTHP @ °C": 9, "Corr Solid": 11, "NAP": 9, "Water": 9, "NAP Ratio": 10,
        "Water Ratio": 12, "Sand": 9, "Cake (HTHP)": 12, "Chlorides": 12, "Calcium": 12,
        "CaCl2": 11, "Water Phase Salinity": 16, "NaCL (Sol/Insol)": 16, "Excess Lime": 12,
        "Electrical_Stability": 13, "LGS (%)": 10, "HGS (%)": 10, "LGS (kg/m³)": 12, "HGS (kg/m³)": 12,
        "ASG": 9, "Additional Properties": 16, "n (HB)": 12, "K (HB)": 12,
        "Viscometer Sag Shoe Test": 20, "(VSST)": 10,
    }
    num_format = "0.00"
    int_format = "0"
    row_start = 4
    for r_idx, (_, row) in enumerate(view_df.iterrows(), start=row_start):
        for c_idx, (col_name, _, _) in enumerate(headers, start=1):
            val = row.get(col_name)
            cell = ws.cell(r_idx, c_idx, val)
            if pd.isna(val):
                cell.value = None
            elif col_name == "DateTime" and str(val).strip():
                cell.number_format = "@"
            elif isinstance(val, (int, np.integer)):
                cell.number_format = int_format
            elif isinstance(val, (float, np.floating)) and np.isfinite(float(val)):
                cell.number_format = num_format if abs(float(val) - round(float(val))) > 1e-9 else int_format
            cell.alignment = Alignment(horizontal="center", vertical="center")
        ws.row_dimensions[r_idx].height = 21

    for idx, (col_name, _, _) in enumerate(headers, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = widths.get(col_name, 12)
    ws.freeze_panes = "A4"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


def _send_mud_bitacora_email(
    attachment_bytes: bytes,
    to_email: str,
    subject: str,
    body: str,
    filename: str = "mud_bitacora.xlsx",
    smtp_server: str = MUD_SMTP_SERVER,
    smtp_port: int = MUD_SMTP_PORT,
    smtp_user: str = MUD_SMTP_USER,
    smtp_pass: str = MUD_SMTP_PASS,
    from_email: str = MUD_SMTP_FROM,
) -> tuple[bool, str]:
    """Envía la bitácora Excel por correo como adjunto."""
    if not smtp_user or not smtp_pass:
        return False, "Faltan credenciales SMTP. Configura MUD_SMTP_USER y MUD_SMTP_PASS en secrets."
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = to_email
        msg.set_content(body)
        msg.add_attachment(
            attachment_bytes,
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=filename,
        )
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return True, f"Bitácora enviada correctamente a {to_email}."
    except Exception as e:
        return False, str(e)


def render_mud_report() -> None:
    _ms = st.session_state.get("mud_data_source")
    if _ms == "Correo electrónico":
        st.session_state["mud_data_source"] = MUD_SRC_EMAIL
    elif _ms == "Subir archivos":
        st.session_state["mud_data_source"] = MUD_SRC_FILES

    st.markdown(
        f"""
        <div style="margin-bottom: 0.5rem;">
            <span style="font-size: 1.5rem; font-weight: 600;">{'Mud Report'}</span>
            <span style="display: inline-flex; align-items: center; gap: 0.35rem; margin-left: 0.75rem; flex-wrap: wrap;">
                <span style="background: linear-gradient(135deg, #b91c1c 0%, #ea580c 50%, #f59e0b 100%); color: #fff; font-size: 0.7rem; font-weight: 700; padding: 0.22rem 0.6rem; border-radius: 999px; letter-spacing: 0.03em; box-shadow: 0 1px 3px rgba(234,88,12,0.4);">🔥 Rogii</span>
                <span style="background: linear-gradient(135deg, #0f766e 0%, #14b8a6 100%); color: #fff; font-size: 0.7rem; font-weight: 600; padding: 0.2rem 0.55rem; border-radius: 999px;">{'Bitácora'}</span>
                <span style="background: linear-gradient(135deg, #1e3a5f 0%, #2563eb 100%); color: #fff; font-size: 0.7rem; font-weight: 600; padding: 0.2rem 0.55rem; border-radius: 999px;">{'PDF / Excel / CSV'}</span>
                <span style="background: linear-gradient(135deg, #7c2d12 0%, #ea580c 100%); color: #fff; font-size: 0.7rem; font-weight: 600; padding: 0.2rem 0.55rem; border-radius: 999px;">{'Correo'}</span>
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption('Carga reportes de lodo en PDF, Excel o CSV (subiendo archivos o desde correo). Se genera una bitácora unificada por día.')

    mud_source = st.radio(
        'Fuente de datos',
        [MUD_SRC_FILES, MUD_SRC_EMAIL],
        horizontal=True,
        key="mud_data_source",
        format_func=lambda x: 'Subir archivos' if x == MUD_SRC_FILES else 'Correo electrónico',
    )

    parsed: list[dict] = []

    # Chips de contexto (Rogii + fuente + Auto 60s si aplica)
    mud_chip_items = [
        ("🔥 Rogii", "#b91c1c", "#ea580c"),
        ('Correo electrónico', "#1e3a5f", "#2563eb")
        if mud_source == MUD_SRC_EMAIL
        else ('📁 Subir archivos', "#0f766e", "#14b8a6"),
    ]
    if mud_source == MUD_SRC_EMAIL and st.session_state.get("mud_auto_refresh", False):
        mud_chip_items.append(("Auto 60s 🔥", "#7c2d12", "#ea580c"))
    mud_cols = st.columns(len(mud_chip_items))
    for i, (label, c1, c2) in enumerate(mud_chip_items):
        with mud_cols[i]:
            st.markdown(
                f'<span style="display:inline-flex;align-items:center;gap:0.25rem;'
                f"background:linear-gradient(135deg,{c1},{c2});color:#fff;font-size:0.75rem;font-weight:600;"
                f'padding:0.25rem 0.6rem;border-radius:999px;box-shadow:0 1px 2px rgba(0,0,0,0.2);">{label}</span>',
                unsafe_allow_html=True,
            )
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    if mud_source == MUD_SRC_EMAIL:
        with st.expander('Configuración de correo (IMAP)', expanded=True):
            st.caption('Credenciales cargadas desde `.streamlit/secrets.toml` (IMAP_SERVER, IMAP_USER, IMAP_PASS). Puedes editarlas aquí solo para esta sesión.')
            col_imap1, col_imap2 = st.columns(2)
            with col_imap1:
                imap_server = st.text_input(
                    'Servidor IMAP',
                    value=MUD_IMAP_SERVER,
                    key="mud_imap_server",
                    help='Ej: imap.gmail.com',
                )
                imap_user = st.text_input(
                    'Usuario (correo)',
                    value=MUD_IMAP_USER,
                    key="mud_imap_user",
                )
            with col_imap2:
                imap_pass = st.text_input(
                    'Contraseña (App Password en Gmail)',
                    value=MUD_IMAP_PASS,
                    type="password",
                    key="mud_imap_pass",
                    help='En Gmail usa una contraseña de aplicación, no la de la cuenta.',
                )
                filename_filter = st.text_input(
                    'Filtrar por nombre de archivo (opcional)',
                    value=MUD_IMAP_FILTER,
                    placeholder='Ej: "Daily Full Report" o "LA-358"',
                    key="mud_imap_filter",
                )
            mark_read = st.checkbox(
                'Marcar correos como leídos al descargar',
                value=True,
                key="mud_imap_mark_read",
            )

        st.markdown('**Revisión automática**')
        mud_auto_refresh = st.checkbox(
            'Revisar correo automáticamente cada 60 s',
            value=st.session_state.get("mud_auto_refresh", False),
            key="mud_auto_refresh",
            help='Cada X segundos se consulta el correo y se actualiza la bitácora. Desmarca para detener.',
        )
        if mud_auto_refresh:
            mud_refresh_interval = st.number_input(
                'Intervalo (segundos)',
                min_value=30,
                max_value=300,
                value=60,
                step=15,
                key="mud_auto_refresh_interval",
                help='Cada cuántos segundos se revisa el correo (30–300 s).',
            )

        run_fetch = st.button(
            '🔥 Rogii – Revisar correo y cargar reportes',
            type="primary",
            key="mud_fetch_email_btn",
            help='Consulta IMAP y descarga adjuntos PDF/Excel/CSV de correos no leídos.',
        ) or (
            mud_auto_refresh
            and st.session_state.pop("mud_auto_rerun_trigger", False)
        )

        if run_fetch:
            if not imap_server or not imap_user or not imap_pass:
                st.error('Completa servidor IMAP, usuario y contraseña (o configúralos en .env).')
            else:
                with st.spinner('Conectando al correo y descargando adjuntos...'):
                    try:
                        attachments = _fetch_mud_attachments_from_email(
                            imap_server.strip(),
                            imap_user.strip(),
                            imap_pass.strip(),
                            filename_contains=filename_filter.strip() or None,
                            mark_read=mark_read,
                        )
                    except Exception as e:
                        st.error(f"{'No se pudo conectar o descargar:'} {e}")
                        attachments = []
                if not attachments:
                    st.info('No se encontraron adjuntos PDF/Excel/CSV en correos no leídos (o no coinciden con el filtro).')
                else:
                    st.success('Se descargaron **{n}** adjunto(s). Procesando...'.format(n=len(attachments)))
                    for name, data in attachments:
                        try:
                            if name.lower().endswith(".pdf"):
                                buf = io.BytesIO(data)
                                parsed.extend(_parse_mud_pdf(buf, source_name=name))
                            elif name.lower().endswith((".xlsx", ".xls")):
                                xl = pd.ExcelFile(io.BytesIO(data))
                                for sh in xl.sheet_names[:5]:
                                    df_raw = pd.read_excel(xl, sheet_name=sh, header=None)
                                    parsed.extend(_parse_mud_excel_sheet(df_raw, name))
                            else:
                                df_raw = pd.read_csv(io.BytesIO(data), sep=None, engine="python", low_memory=False)
                                parsed.extend(_parse_mud_csv(df_raw, name))
                        except Exception as e:
                            st.warning(f"No se pudo procesar **{name}**: {e}")
                    if parsed:
                        bitacora_new = _build_mud_bitacora(parsed)
                        existing = st.session_state.get("mud_bitacora")
                        if existing is not None and not existing.empty:
                            bitacora_combined = pd.concat([existing, bitacora_new], ignore_index=True)
                            bitacora_combined["Date"] = pd.to_datetime(bitacora_combined["Date"], errors="coerce")
                            bitacora_combined = bitacora_combined.dropna(subset=["Date"]).sort_values("Date").drop_duplicates().reset_index(drop=True)
                            st.session_state["mud_bitacora"] = bitacora_combined
                        else:
                            st.session_state["mud_bitacora"] = bitacora_new
                        st.success(f"Bitácora actualizada con **{len(parsed)}** registro(s) desde correo.")
                        st.rerun()
                    else:
                        st.warning("No se detectaron propiedades de lodo en los adjuntos.")

    else:
        uploaded = st.file_uploader(
            'Subir reportes de lodo (PDF, Excel, CSV)',
            type=["pdf", "xlsx", "xls", "csv"],
            accept_multiple_files=True,
            key="mud_upload",
        )

        if uploaded:
            for f in uploaded:
                name = getattr(f, "name", "") or ""
                try:
                    if name.lower().endswith(".pdf"):
                        parsed.extend(_parse_mud_pdf(f, source_name=name))
                    elif name.lower().endswith((".xlsx", ".xls")):
                        xl = pd.ExcelFile(f)
                        for sh in xl.sheet_names[:5]:
                            df_raw = pd.read_excel(xl, sheet_name=sh, header=None)
                            parsed.extend(_parse_mud_excel_sheet(df_raw, name))
                    else:
                        df_raw = pd.read_csv(f, sep=None, engine="python", low_memory=False)
                        parsed.extend(_parse_mud_csv(df_raw, name))
                except Exception as e:
                    st.warning(f"No se pudo procesar **{name}**: {e}")

            if parsed:
                bitacora = _build_mud_bitacora(parsed)
                st.session_state["mud_bitacora"] = bitacora
            else:
                st.warning("No se detectaron propiedades de lodo en los archivos. Revisa que contengan columnas o celdas como Density, MW, PV, YP, Gels, etc.")
                if "mud_bitacora" in st.session_state:
                    del st.session_state["mud_bitacora"]

    bitacora = st.session_state.get("mud_bitacora")
    if bitacora is None or bitacora.empty:
        st.info("Sube uno o más reportes (PDF, Excel o CSV) para generar la bitácora.")
        return
    bitacora_view = _mud_build_view_df(bitacora)

    # Chips pro Rogii sobre la bitácora
    n_reg = len(bitacora)
    bitacora_chips = [
        ("🔥 Rogii", "#b91c1c", "#ea580c"),
        (f"{n_reg:,} registros", "#0f766e", "#14b8a6"),
        ("Bitácora", "#1e3a5f", "#2563eb"),
    ]
    if bitacora["Date"].notna().any():
        d_min = bitacora["Date"].min()
        d_max = bitacora["Date"].max()
        if hasattr(d_min, "strftime"):
            bitacora_chips.append((f"{d_min.strftime('%d/%b')} – {d_max.strftime('%d/%b')}", "#334155", "#64748b"))
    bitacora_cols = st.columns(len(bitacora_chips))
    for i, (label, c1, c2) in enumerate(bitacora_chips):
        with bitacora_cols[i]:
            st.markdown(
                f'<span style="display:inline-flex;align-items:center;gap:0.25rem;'
                f"background:linear-gradient(135deg,{c1},{c2});color:#fff;font-size:0.75rem;font-weight:600;"
                f'padding:0.28rem 0.65rem;border-radius:999px;box-shadow:0 1px 3px rgba(0,0,0,0.15);">{label}</span>',
                unsafe_allow_html=True,
            )
    st.success(f"Bitácora: **{n_reg:,}** registros por fecha.")
    tab_bitacora, tab_graficas, tab_stats = st.tabs(["Bitácora", "Gráficas y evolución", "Estadísticas"])

    with tab_bitacora:
        st.subheader("Bitácora de propiedades de fluidos")
        st.dataframe(bitacora_view, use_container_width=True, hide_index=True)

        buf_csv = io.BytesIO()
        bitacora_view.to_csv(buf_csv, index=False, encoding="utf-8-sig")
        buf_csv.seek(0)
        xlsx_bytes = _export_mud_bitacora_excel(bitacora_view)

        default_base = _default_mud_bitacora_basename(bitacora)
        st.markdown("### 📎 Nombre del archivo de salida")
        output_base = st.text_input(
            "Nombre base para CSV, Excel y adjunto de correo:",
            value=default_base,
            key="mud_output_basename",
            help="Puedes editarlo antes de descargar o enviar. Se añade .csv o .xlsx según el formato.",
        )
        output_base = _sanitize_filename(
            output_base.replace(".csv", "").replace(".xlsx", "").replace(".xls", "")
        )
        csv_name = f"{output_base}.csv"
        xlsx_name = f"{output_base}.xlsx"
        st.caption(f"Descarga: **{csv_name}** · **{xlsx_name}** · Correo adjunta: **{xlsx_name}**")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.download_button(
                "Exportar bitácora (CSV)",
                data=buf_csv.getvalue(),
                file_name=csv_name,
                mime="text/csv",
                key="mud_export_csv",
            )
        with col2:
            st.download_button(
                "Exportar bitácora (Excel)",
                data=xlsx_bytes,
                file_name=xlsx_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="mud_export_xlsx",
            )
        with col3:
            if st.button("Enviar bitácora por correo", key="mud_send_email_btn", type="secondary"):
                date_label = ""
                try:
                    if "Date" in bitacora.columns and bitacora["Date"].notna().any():
                        dmax = pd.to_datetime(bitacora["Date"], errors="coerce").dropna().max()
                        if pd.notna(dmax):
                            date_label = dmax.strftime("%Y-%m-%d")
                except Exception:
                    date_label = ""
                subject = f"Mud bitácora {date_label}".strip()
                body = (
                    "Hola,\n\n"
                    "Adjunto la bitácora de propiedades de fluidos generada desde la app.\n\n"
                    "Saludos."
                )
                ok, msg = _send_mud_bitacora_email(
                    attachment_bytes=xlsx_bytes,
                    to_email=MUD_SMTP_TO,
                    subject=subject,
                    body=body,
                    filename=xlsx_name,
                )
                if ok:
                    st.success(msg)
                else:
                    st.error(f"No se pudo enviar la bitácora por correo: {msg}")

        with st.expander("Configuración de envío por correo", expanded=False):
            st.caption("Estos valores se leen desde `.streamlit/secrets.toml` (SMTP_* / SMTP_TO).")
            e1, e2 = st.columns(2)
            with e1:
                st.text_input("SMTP server", value=MUD_SMTP_SERVER, disabled=True, key="mud_smtp_server_view")
                st.text_input("SMTP user", value=MUD_SMTP_USER, disabled=True, key="mud_smtp_user_view")
                st.text_input("From", value=MUD_SMTP_FROM, disabled=True, key="mud_smtp_from_view")
            with e2:
                st.text_input("SMTP port", value=str(MUD_SMTP_PORT), disabled=True, key="mud_smtp_port_view")
                st.text_input("To", value=MUD_SMTP_TO, disabled=True, key="mud_smtp_to_view")
                st.text_input("SMTP password", value=("********" if MUD_SMTP_PASS else ""), type="password", disabled=True, key="mud_smtp_pass_view")

    with tab_graficas:
        st.subheader("Evolución de propiedades por día")
        props = _mud_numeric_property_columns(bitacora)
        if not props:
            st.info("No hay columnas numéricas para graficar.")
        else:
            st.caption("Selecciona las propiedades a graficar")
            default_first = min(4, len(props))
            n_cols = 4
            n_rows = (len(props) + n_cols - 1) // n_cols
            checkbox_state = {}
            for row in range(n_rows):
                cols = st.columns(n_cols)
                for col_idx in range(n_cols):
                    i = row * n_cols + col_idx
                    if i >= len(props):
                        break
                    p = props[i]
                    default = i < default_first
                    checkbox_state[p] = st.checkbox(
                        p,
                        value=st.session_state.get(f"mud_cb_{p}", default),
                        key=f"mud_cb_{p}",
                        label_visibility="visible",
                    )
            selected = [p for p in props if checkbox_state.get(p, False)]
            if selected:
                df_plot = bitacora[["Date"] + selected].copy()
                df_plot = df_plot.set_index("Date").sort_index()
                df_plot = df_plot.reset_index()
                fig = go.Figure()
                for p in selected:
                    fig.add_trace(
                        go.Scatter(
                            x=df_plot["Date"],
                            y=df_plot[p],
                            mode="lines+markers",
                            name=p,
                            line=dict(width=2),
                            marker=dict(size=8),
                        )
                    )
                fig.update_layout(
                    title="Evolución de propiedades de lodo",
                    xaxis_title="Fecha",
                    yaxis_title="Valor",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    hovermode="x unified",
                )
                fig = prettify_auto(fig, h=480)
                st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
                # Chips por propiedad
                chip_items = []
                for p in selected:
                    s = bitacora[p].dropna()
                    if len(s):
                        chip_items.append((p, "blue", f"min {format_num(s.min())} · max {format_num(s.max())} · n={len(s):,}"))
                if chip_items:
                    nchips = len(chip_items)
                    cols_chip = st.columns(min(nchips, 4))
                    for i, (label, color, sub) in enumerate(chip_items):
                        with cols_chip[i % len(cols_chip)]:
                            st.caption(f"**{label}** — {sub}")

            st.markdown("---")
            st.subheader("Gráfica por propiedad (selección única)")
            single_prop = st.selectbox("Propiedad", ["(ninguna)"] + props, key="mud_single_prop")
            if single_prop and single_prop != "(ninguna)":
                s = bitacora[single_prop].dropna()
                if len(s):
                    fig1 = px.line(bitacora, x="Date", y=single_prop, title=f"Evolución – {single_prop}", markers=True)
                    fig1.update_traces(line=dict(width=2), marker=dict(size=10))
                    apply_line_area_fill(fig1, line_color="#2563EB", fill_alpha=0.22)
                    fig1 = prettify(fig1, h=420)
                    st.plotly_chart(fig1, use_container_width=True, config=PLOTLY_CONFIG)
                    _render_chips_row([(single_prop, "blue"), (f"min {format_num(s.min())}", "gray"), (f"max {format_num(s.max())}", "gray"), (f"promedio {format_num(s.mean())}", "green")])

            st.markdown("---")
            st.subheader("🔥 Gráficas adicionales pro")
            st.caption("Heatmap de correlación, gráfico de control y perfil radar para análisis de fluidos.")

            # 1) Heatmap de correlación entre propiedades de lodo
            if len(props) >= 2:
                st.markdown("**Correlación entre propiedades**")
                corr_mud = bitacora[props].corr()
                if not corr_mud.isna().all().all():
                    _mud_hm_stats = heatmap_numeric_stats(bitacora, props)
                    _mud_chips = stats_df_to_heatmap_chips(_mud_hm_stats, max_chips=10)
                    if _mud_chips:
                        st.caption("**Chips — min–max y media por propiedad (base del heatmap)**")
                        _render_chips_row(_mud_chips)
                    with st.expander("Min / media / max por propiedad (lodo)", expanded=False):
                        st.dataframe(_mud_hm_stats, use_container_width=True, hide_index=True)
                    corr_pct = (corr_mud * 100).round(0)
                    text_arr = np.where(
                        np.isnan(corr_pct.values),
                        "",
                        (np.nan_to_num(corr_pct.values, nan=0.0).astype(int)).astype(str) + "%",
                    )
                    fig_corr_mud = px.imshow(
                        corr_mud,
                        color_continuous_scale="RdBu",
                        zmin=-1,
                        zmax=1,
                        title="Mud Report – Correlación entre propiedades",
                    )
                    fig_corr_mud.update_traces(
                        text=text_arr,
                        texttemplate="%{text}",
                        textfont=dict(size=11),
                        xgap=1,
                        ygap=1,
                    )
                    fig_corr_mud.update_layout(coloraxis_colorbar=dict(title="Corr (-1 a 1)"))
                    fig_corr_mud = prettify_heatmap_auto(fig_corr_mud, h=420)
                    st.plotly_chart(fig_corr_mud, use_container_width=True, config=PLOTLY_CONFIG)
                    _sp_mud = build_minmax_mean_spine_figure(
                        _mud_hm_stats,
                        title="Perfil min · media · max — propiedades de lodo (normalizado)",
                    )
                    if _sp_mud is not None:
                        st.caption("**Curvas pro:** rango observado por variable (● = media en el rango min–max).")
                        st.plotly_chart(_sp_mud, use_container_width=True, config=PLOTLY_CONFIG)
                    st.caption("Rojo = correlación positiva, azul = negativa. Valores en % de fuerza lineal.")
                else:
                    st.info("No hay suficientes datos para calcular correlaciones.")
            else:
                st.caption("Se necesitan al menos 2 propiedades numéricas para el heatmap de correlación.")

            # 2) Gráfico de control (propiedad vs fecha, media ± 2σ)
            st.markdown("**Gráfico de control (media ± 2σ)**")
            ctrl_prop = st.selectbox(
                "Propiedad para control",
                props,
                key="mud_ctrl_prop",
                help="Puntos fuera de la banda se marcan en naranja.",
            )
            if ctrl_prop:
                df_ctrl = bitacora[["Date", ctrl_prop]].dropna()
                if len(df_ctrl) >= 2:
                    mean_val = float(df_ctrl[ctrl_prop].mean())
                    std_val = float(df_ctrl[ctrl_prop].std()) or 1e-6
                    upper = mean_val + 2 * std_val
                    lower = mean_val - 2 * std_val
                    df_ctrl = df_ctrl.copy()
                    df_ctrl["_out"] = (df_ctrl[ctrl_prop] > upper) | (df_ctrl[ctrl_prop] < lower)
                    fig_ctrl = go.Figure()
                    in_spec = df_ctrl[~df_ctrl["_out"]]
                    out_spec = df_ctrl[df_ctrl["_out"]]
                    if not in_spec.empty:
                        fig_ctrl.add_trace(
                            go.Scatter(
                                x=in_spec["Date"],
                                y=in_spec[ctrl_prop],
                                mode="markers",
                                name="Dentro de límites",
                                marker=dict(size=8, color="#2563EB", opacity=0.85),
                            )
                        )
                    if not out_spec.empty:
                        fig_ctrl.add_trace(
                            go.Scatter(
                                x=out_spec["Date"],
                                y=out_spec[ctrl_prop],
                                mode="markers",
                                name="Fuera de límites (±2σ)",
                                marker=dict(size=10, color="#EA580C", symbol="diamond"),
                            )
                        )
                    fig_ctrl.add_hline(y=mean_val, line_dash="dash", line_color="#10B981", annotation_text="Media")
                    fig_ctrl.add_hline(y=upper, line_dash="dot", line_color="#EF4444", annotation_text="UCL")
                    fig_ctrl.add_hline(y=lower, line_dash="dot", line_color="#EF4444", annotation_text="LCL")
                    fig_ctrl.update_layout(
                        title=f"Control chart – {ctrl_prop}",
                        xaxis_title="Fecha",
                        yaxis_title=ctrl_prop,
                        height=400,
                        showlegend=True,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    )
                    fig_ctrl = prettify_auto(fig_ctrl, h=400)
                    st.plotly_chart(fig_ctrl, use_container_width=True, config=PLOTLY_CONFIG)
                    st.caption(f"Media = {format_num(mean_val)}, LCL = {format_num(lower)}, UCL = {format_num(upper)}. Puntos fuera de banda = {len(out_spec)}.")
                else:
                    st.info("Se necesitan al menos 2 puntos para el gráfico de control.")

            # 3) Radar / perfil del lodo (último registro o por fecha)
            st.markdown("**Perfil radar (comparación normalizada)**")
            radar_props = [p for p in props if bitacora[p].notna().any()]
            if len(radar_props) >= 3:
                dates_opt = bitacora["Date"].dropna().unique()
                if len(dates_opt) > 0:
                    dates_sorted = sorted(dates_opt, reverse=True)
                    default_idx = 0
                    radar_date = st.selectbox(
                        "Fecha del perfil",
                        options=dates_sorted,
                        format_func=lambda x: x.strftime("%Y-%m-%d %H:%M") if hasattr(x, "strftime") else str(x),
                        index=default_idx,
                        key="mud_radar_date",
                        help="Perfil normalizado 0–100% para esa fecha.",
                    )
                    row = bitacora[bitacora["Date"] == radar_date].iloc[-1]
                    r_vals = []
                    for p in radar_props:
                        v = row.get(p)
                        if pd.isna(v):
                            r_vals.append(0.0)
                        else:
                            s_col = bitacora[p].dropna()
                            if len(s_col) and s_col.max() != s_col.min():
                                norm = (float(v) - float(s_col.min())) / (float(s_col.max()) - float(s_col.min()))
                            else:
                                norm = 0.5
                            r_vals.append(round(norm * 100, 1))
                    fig_radar = go.Figure()
                    fig_radar.add_trace(
                        go.Scatterpolar(
                            r=r_vals + [r_vals[0]],
                            theta=radar_props + [radar_props[0]],
                            fill="toself",
                            name="Perfil normalizado",
                            line=dict(color="#EA580C", width=2),
                            fillcolor="rgba(234,88,12,0.25)",
                        )
                    )
                    fig_radar.update_layout(
                        polar=dict(radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(size=10))),
                        title=f"Perfil de lodo – {radar_date.strftime('%Y-%m-%d') if hasattr(radar_date, 'strftime') else radar_date}",
                        height=460,
                        showlegend=False,
                    )
                    fig_radar = prettify_auto(fig_radar, h=460)
                    st.plotly_chart(fig_radar, use_container_width=True, config=PLOTLY_CONFIG)
                    st.caption("Cada eje = propiedad normalizada 0–100% respecto al min/max del histórico. Útil para comparar perfiles por fecha.")
                else:
                    st.info("No hay fechas válidas para el radar.")
            else:
                st.caption("Se necesitan al menos 3 propiedades numéricas para el perfil radar.")

    with tab_stats:
        st.subheader("Estadísticas por propiedad")
        props = _mud_numeric_property_columns(bitacora)
        if not props:
            st.info("No hay columnas numéricas.")
        else:
            stats_rows = []
            for p in props:
                s = bitacora[p].dropna()
                if len(s):
                    stats_rows.append({
                        "Propiedad": p,
                        "N": len(s),
                        "Min": s.min(),
                        "Max": s.max(),
                        "Media": s.mean(),
                        "Mediana": s.median(),
                        "Desv. est.": s.std(),
                    })
            if stats_rows:
                stats_df = pd.DataFrame(stats_rows)
                st.dataframe(stats_df, use_container_width=True, hide_index=True)
                st.markdown("---")
                st.subheader("Distribución (histograma)")
                prop_hist = st.selectbox("Propiedad para histograma", props, key="mud_hist_prop")
                if prop_hist:
                    vals = bitacora[prop_hist].dropna()
                    fig_hist = build_hist_with_trend(vals, title=f"Distribución – {prop_hist}", x_label=prop_hist, nbins=25)
                    st.plotly_chart(prettify_hist(fig_hist), use_container_width=True, config=PLOTLY_CONFIG)
                    st.caption(f"**Resumen:** {series_summary(vals)}.")

    # Auto-refresh correo cada N segundos (solo si fuente = Correo y hay bitácora)
    if (
        st.session_state.get("mud_data_source") == MUD_SRC_EMAIL
        and st.session_state.get("mud_auto_refresh")
    ):
        interval = int(st.session_state.get("mud_auto_refresh_interval", 60))
        interval = max(30, min(300, interval))
        countdown_placeholder = st.empty()
        for i in range(interval, 0, -1):
            countdown_placeholder.info('🔥 **Rogii** – Próxima revisión de correo en **{i}** s… (desmarca «Revisar correo automáticamente» para detener)'.format(i=i))
            time.sleep(1)
        countdown_placeholder.empty()
        st.session_state["mud_auto_rerun_trigger"] = True
        st.rerun()