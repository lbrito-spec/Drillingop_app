# TNPIV29_app_fixed_v2 (con Viajes + Avance de profundidad)
# tnpiv3_app.py
# ------------------------------------------------------------
# Requisitos:
#   pip install streamlit pandas plotly reportlab python-pptx pillow kaleido
#
# Ejecutar:
#   streamlit run tnpiv3_app.py
# ------------------------------------------------------------

# --- FIX: alias seguro para evitar NameError (compatibilidad) ---
tipo_tiempo = None
# --- FIX: alias seguro para operaciones_seleccionadas ---
operaciones_seleccionadas = None
try:
    operaciones_seleccionadas = st.session_state.get('operacion_sel', None)
except Exception:
    pass

try:
    tipo_tiempo = st.session_state.get('tipo_time_general', None)
except Exception:
    pass

import os
import re
import base64
import json
from io import BytesIO
from datetime import datetime
import uuid

import streamlit as st
import pandas as pd

def _calc_eff(prog: float, real: float) -> float:
    """Eficiencia (%): 100 si real <= programado; si real>prog => (prog/real)*100."""
    try:
        prog = float(prog)
        real = float(real)
    except Exception:
        return 0.0
    if real <= 0 or prog <= 0:
        return 0.0
    return 100.0 if real <= prog else (prog / real) * 100.0


# --- FIX: asegurar RowID por registro (para edición en Detalle) ---
def _ensure_rowid(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    if "RowID" not in df.columns:
        df.insert(0, "RowID", "")
    missing = df["RowID"].isna() | (df["RowID"].astype(str).str.strip() == "")
    if missing.any():
        df.loc[missing, "RowID"] = [str(uuid.uuid4()) for _ in range(int(missing.sum()))]
    return df


def _normalize_time_cause_columns(df_in: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas de causas TNPI/TNP.

    - Asegura que existan Categoria/Detalle para TNPI y TNP.
    - Evita NaN/None en tablas y gráficas (usa "-").
    - Backward compatible: si hay registros antiguos con TNP guardado en
      Categoria_TNPI/Detalle_TNPI, los copia a Categoria_TNP/Detalle_TNP.
    """
    if df_in is None or df_in.empty:
        return df_in

    df = df_in.copy()

    for col in [
        "Categoria_TNPI",
        "Detalle_TNPI",
        "Categoria_TNP",
        "Detalle_TNP",
    ]:
        if col not in df.columns:
            df[col] = "-"

    # Clean null-like values
    for col in ["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP"]:
        df[col] = df[col].replace({None: "-"}).fillna("-")

    # Backfill TNP causes from TNPI columns if older data was stored there
    m_tnp = df.get("Tipo", "") == "TNP"
    if m_tnp.any():
        m_missing_cat = m_tnp & (df["Categoria_TNP"].isin(["-", "", "nan"]))
        m_missing_det = m_tnp & (df["Detalle_TNP"].isin(["-", "", "nan"]))
        if "Categoria_TNPI" in df.columns:
            df.loc[m_missing_cat, "Categoria_TNP"] = df.loc[m_missing_cat, "Categoria_TNPI"].replace({"nan": "-"})
        if "Detalle_TNPI" in df.columns:
            df.loc[m_missing_det, "Detalle_TNP"] = df.loc[m_missing_det, "Detalle_TNPI"].replace({"nan": "-"})

        # Final safety: don't leave NaNs
        df.loc[m_tnp, ["Categoria_TNP", "Detalle_TNP"]] = df.loc[m_tnp, ["Categoria_TNP", "Detalle_TNP"]].replace({None: "-"}).fillna("-")

    return df

import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit.components.v1 as components

# ------------------------------
# CSV robust loader (para CSVs con separador desconocido)
# ------------------------------
def robust_read_csv(src, encodings=("utf-8-sig","utf-8","latin-1"), seps=(",", ";", "\t", "|")) -> pd.DataFrame:
    """
    Lee CSV desde ruta o stream (UploadedFile) intentando:
      1) sep=None (Sniffer) con engine='python'
      2) una lista de separadores comunes
    También prueba encodings típicos (utf-8-sig/utf-8/latin-1).
    """
    # Obtener bytes si es stream (p.ej. st.file_uploader)
    data_bytes = None
    if hasattr(src, "getvalue"):
        try:
            data_bytes = src.getvalue()
        except Exception:
            data_bytes = None
    if data_bytes is None and hasattr(src, "read") and not isinstance(src, (str, bytes, os.PathLike)):
        try:
            # Ojo: read() consume el stream; por eso guardamos bytes
            data_bytes = src.read()
        except Exception:
            data_bytes = None

    def _try_read(buf, enc, sep):
        return pd.read_csv(buf, sep=sep, engine="python", encoding=enc)

    # Caso path en disco
    if isinstance(src, (str, os.PathLike)):
        last_err = None
        for enc in encodings:
            try:
                return pd.read_csv(src, sep=None, engine="python", encoding=enc)
            except Exception as e:
                last_err = e
            for s in seps:
                try:
                    return pd.read_csv(src, sep=s, engine="python", encoding=enc)
                except Exception as e:
                    last_err = e
        raise last_err if last_err else ValueError("No se pudo leer el CSV")

    # Caso stream / bytes
    if data_bytes is None:
        raise ValueError("No se pudo leer el archivo (stream vacío).")

    from io import BytesIO
    last_err = None
    for enc in encodings:
        try:
            return _try_read(BytesIO(data_bytes), enc, None)
        except Exception as e:
            last_err = e
        for s in seps:
            try:
                return _try_read(BytesIO(data_bytes), enc, s)
            except Exception as e:
                last_err = e
    raise last_err if last_err else ValueError("No se pudo leer el CSV")


from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader

from pptx import Presentation
from pptx.util import Inches, Pt

from PIL import Image

# ------------------------------
# PLOTLY EXPORT (kaleido)
# ------------------------------
PLOTLY_IMG_OK = True
try:
    import plotly.io as pio
    import plotly.graph_objects as go
except Exception:
    PLOTLY_IMG_OK = False

# ------------------------------
# CONFIG STREAMLIT
# ------------------------------

def _semaforo_from_eff(eff):
    """Devuelve un semáforo (emoji) a partir de eficiencia en % (0-100)."""
    try:
        if eff is None:
            return "⚪"
        if isinstance(eff, str) and eff.strip()=="":
            return "⚪"
        val = float(eff)
    except Exception:
        return "⚪"
    if val >= 85:
        return "🟢"
    if val >= 75:
        return "🟡"
    return "🔴"

# Backward-compat alias used in some blocks
def _semaforo_text(eff):
    return _semaforo_from_eff(eff)

def semaforo_dot(eff):
    """Compat: devuelve bolita semáforo según eficiencia (%)."""
    return _semaforo_from_eff(eff)


# --- Helpers: coalesce duplicate columns (avoid losing data when columns are repeated) ---
def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    # If df has duplicate column names, coalesce them row-wise (first non-null/non-empty) into one.
    if df is None or df.empty or not df.columns.duplicated().any():
        return df
    out = df.copy()
    dup_names = [c for c in out.columns[out.columns.duplicated()].unique()]
    for name in dup_names:
        cols = [c for c in out.columns if c == name]
        base = out[cols[0]].copy()
        for c in cols[1:]:
            s = out[c]
            mask = base.isna() | (base.astype(str).str.strip() == '') | (base.astype(str).str.lower() == 'nan')
            base = base.where(~mask, s)
        out[name] = base
        # drop extra duplicate columns, keep the first occurrence only
        keep = []
        seen_first = False
        for c in out.columns:
            if c != name:
                keep.append(c)
            else:
                if not seen_first:
                    keep.append(c)
                    seen_first = True
        out = out.loc[:, keep]
    return out


def add_semaforo_column(df, eff_col="Eficiencia_pct"):
    """Agrega columna 'Semáforo' sin alterar estilos (solo texto)."""
    if df is None:
        return df
    df = _coalesce_duplicate_columns(df)
    if df is None or df.empty:
        return df
    if eff_col not in df.columns:
        return df
    _df = df.copy()
    _df["Semáforo"] = _df[eff_col].apply(_semaforo_from_eff)
    return _df

st.set_page_config(page_title="Dashboard Operativo DrillSpot", layout="wide")

# --- Modo visual (forzar claro/oscuro independiente del theme de Streamlit) ---
# Esto controla los "cards" (HTML/iframes) y algunos estilos pro. No afecta cálculos.
if "ui_mode" not in st.session_state:
    # Si ya existe un turno (p.ej. BHA), úsalo como default. Si no, Diurno.
    st.session_state["ui_mode"] = st.session_state.get("turno", "Diurno")

with st.sidebar:
    st.radio("Modo visual", ["Diurno", "Nocturno"], key="ui_mode", horizontal=True)

# ------------------------------
# RUTAS (PC LOCAL)  ✅ AJUSTA ESTO
# ------------------------------
LOGO_PATH = r"C:\Users\l.brito_rogii\Downloads\DrillingOP_APP\ROGII_DINAMIC.gif"
TNPI_CSV_PATH = r"C:\Users\l.brito_rogii\Downloads\DrillingOP_APP\Detalles causas de TNPI.csv"

# ------------------------------
# ESTILO GLOBAL (HEADER PRO + UTILIDADES)
# ------------------------------
st.markdown(
    """
    <style>
      /* Quita margen arriba del main */
      .block-container { padding-top: 1.1rem; }

      /* Header card */
      .ds-header {
        border-radius: 22px;
        padding: 18px 20px;
        background: radial-gradient(1200px 240px at 20% -20%, rgba(40,180,99,0.22), transparent 60%),
                    radial-gradient(1200px 240px at 80% 0%, rgba(46,134,193,0.22), transparent 55%),
                    linear-gradient(180deg, rgba(18,18,20,0.95), rgba(8,8,10,0.96));
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 18px 50px rgba(0,0,0,0.40);
        display:flex;
        gap: 16px;
        align-items:center;
      }
      .ds-logo-wrap{
        width:64px;height:64px;border-radius:18px;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        display:flex;align-items:center;justify-content:center;
        box-shadow: inset 0 0 0 1px rgba(255,255,255,0.02);
        overflow:hidden;
      }
      .ds-logo {
  width: 90px;
  height: auto;
  max-height: 70px;
  margin-right: 16px;
}

.ds-logo.no-float {
  animation: none !important;
}

      @keyframes dsFloat{
        0%{ transform: translateY(0px) scale(1.00); }
        50%{ transform: translateY(-3px) scale(1.03); }
        100%{ transform: translateY(0px) scale(1.00); }
      }
      .ds-title{
        font-size: 34px;
        font-weight: 900;
        line-height: 1.05;
        margin: 0;
        color: rgba(255,255,255,0.95);
        letter-spacing: 0.2px;
      }
      .ds-sub{
        margin-top: 6px;
        color: rgba(255,255,255,0.72);
        font-size: 14px;
        font-weight: 600;
      }

      /* Estado del día (pill) + glow dinámico por eficiencia */
      .ds-header { position: relative; overflow: hidden; }
      .ds-header::after{
        content:"";
        position:absolute; inset:-2px;
        background: radial-gradient(700px 260px at 12% 0%, var(--ds-glow, rgba(46,134,193,0.18)), transparent 60%),
                    radial-gradient(900px 260px at 88% 10%, var(--ds-glow2, rgba(40,180,99,0.18)), transparent 55%);
        pointer-events:none;
      }
      .ds-header[data-status="ok"]{ --ds-glow: rgba(40,180,99,0.22); --ds-glow2: rgba(46,134,193,0.18); }
      .ds-header[data-status="warn"]{ --ds-glow: rgba(241,196,15,0.22); --ds-glow2: rgba(46,134,193,0.14); }
      .ds-header[data-status="crit"]{ --ds-glow: rgba(231,76,60,0.28); --ds-glow2: rgba(241,196,15,0.12); }

      .ds-status{
        display:inline-flex; align-items:center; gap:8px;
        padding: 6px 10px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.10);
        background: rgba(255,255,255,0.06);
        color: rgba(255,255,255,0.88);
        font-weight: 800;
        font-size: 12px;
        letter-spacing: 0.2px;
      }
      .ds-status b{ font-weight: 950; }
      .ds-status .chip{
        width:10px;height:10px;border-radius:999px;
        border: 2px solid rgba(255,255,255,0.10);
        box-shadow: 0 8px 16px rgba(0,0,0,0.35);
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------
# HELPERS: base64 (PNG/GIF) para HTML
# ------------------------------
def file_to_base64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()


def mime_from_path(path: str) -> str:
    ext = os.path.splitext(path.lower())[1]
    if ext == ".gif":
        return "image/gif"
    if ext in [".jpg", ".jpeg"]:
        return "image/jpeg"
    return "image/png"


logo_b64 = ""
logo_mime = "image/png"
if LOGO_PATH and os.path.exists(LOGO_PATH):
    try:
        logo_b64 = file_to_base64(LOGO_PATH)
        logo_mime = mime_from_path(LOGO_PATH)
    except Exception:
        logo_b64 = ""
        logo_mime = "image/png"

# ------------------------------
# CONSTANTES
# ------------------------------
EXPORT_COLORWAY = ["#2E86C1", "#28B463", "#E74C3C", "#F1C40F", "#8E44AD", "#16A085"]
EQUIPO_TIPO = ["3000HP / AE", "2000HP"]
MODO_REPORTE_OPTS = ["Perforación", "Cambio de etapa"]
TIPO_AGUJERO = ["Entubado", "Descubierto"]
SECCIONES_DEFAULT = ['36"', '26"', '18 1/2"', '13 3/8"', '12 1/4"', '8 1/2"', '6 1/8"']
TURNOS = ["Diurno", "Nocturno"]

ACTIVIDADES = [
    "Perforación",
    "Circula",
    "Rebaja cemento",
    "Prueba hermeticidad TR",
    "Instala UAP",
    "Desplaza",
    "Mantenimiento",
    "Succiona contrapozos",
    "Instala brida en cabezal",
    "Cambio de bombas",
    "Verifica parámetros",
    "Comandos fuera de la conexión",
    "Repaso fuera de la conexión",
    "Fallas",
    "Arma/Desarma BHA",
    "Conexión perforando",
    "Mete/levanta TR 30",
    "Mete/levanta TR 20",
    "Mete/levanta TR 16",
    "Mete/levanta TR 13 3/8",
    "Mete/levanta LN / TR (Lingadas)",
    "Mete/levanta LN / TR (TxT)",

    # Viajes (TRIPS)
    "Viaje metiendo con Pistolas",
    "Viaje sacando con pistolas",
    "Viaje metiendo con pescante",
    "Viaje levantando con pescante (asumiendo que se realizó la operación de pesca)",
    "Viaje inspeccionando roscas",
    "Viaje procedimiento quemado roscas nuevas",
    "Viaje de TLC",
    "Viaje metiendo con cuchara",
    "Viaje levantando con cuchara",
    "Viaje levantando/Metiendo TP de suelo natural",
    "Viaje levantando núcleo",
    "Viaje metiendo retenedor/PBR",
    "Viaje metiendo/levantando aplicando contrapresión (MPD)",
    "Viaje metiendo/levantando Alineados a MPD sin aplicar contrapresión",
    "Viaje levantando con tubería llena",
    "Viaje metiendo y sacando con conexión a top Drive (rotación y bombeo)",
    "Viaje metiendo y sacando con conexión a top Drive (rotación y bombeo, MPD)",
    "Viaje con conexión reductores de fricción / removedores de recortes (cada dos lingadas)",
    "Viaje con conexión usando llaves de fuerza",
    "Viaje con Calibración interna de TP",
    "Viaje Tramos dobles",
    "Viaje levantando empacador",
    "Viaje metiendo / levantando Aparejo doble",
    "Viaje metiendo / levantando aparejo de producción",
    "Viaje metiendo TP lingadas",
    "Viaje metiendo TP TxT",
    "Viaje levantando TP lingadas",
    "Viaje levantando TP TxT",
    "Viaje metiendo / levantando TP de 3 1/2\" - 2 7/8\" por lingadas",
    "Viaje metiendo / levantando TP de 3 1/2\" - 2 7/8\" TxT",
]

# Catálogo de objetivos para Viajes (m/h y min por conexión)
# Nota: estos valores vienen de la tabla de objetivos (velocidad y tiempo de conexión)
VIAJE_CATALOG = {

    "Mete/levanta TR 30": {"vel_mh": 48.0, "tconn_min": 8.0},
    "Mete/levanta TR 20": {"vel_mh": 75.0, "tconn_min": 5.5},
    "Mete/levanta TR 16": {"vel_mh": 112.0, "tconn_min": 5.0},
    "Mete/levanta TR 13 3/8": {"vel_mh": 120.0, "tconn_min": 4.5},
    "Mete/levanta LN / TR (Lingadas)": {"vel_mh": 242.0, "tconn_min": 4.0},
    "Mete/levanta LN / TR (TxT)": {"vel_mh": 140.0, "tconn_min": 4.0},
    "Viaje metiendo con Pistolas": {"vel_mh": 476.0, "tconn_min": 2.0},
    "Viaje sacando con pistolas": {"vel_mh": 476.0, "tconn_min": 2.0},
    "Viaje metiendo con pescante": {"vel_mh": 336.0, "tconn_min": 2.0},
    "Viaje levantando con pescante (asumiendo que se realizó la operación de pesca)": {"vel_mh": 306.0, "tconn_min": 2.5},
    "Viaje inspeccionando roscas": {"vel_mh": 336.0, "tconn_min": 4.0},
    "Viaje procedimiento quemado roscas nuevas": {"vel_mh": 252.0, "tconn_min": 5.5},
    "Viaje de TLC": {"vel_mh": 308.0, "tconn_min": 3.5},
    "Viaje metiendo con cuchara": {"vel_mh": 224.0, "tconn_min": 2.3},
    "Viaje levantando con cuchara": {"vel_mh": 224.0, "tconn_min": 2.0},
    "Viaje levantando/Metiendo TP de suelo natural": {"vel_mh": 252.0, "tconn_min": 5.0},
    "Viaje levantando núcleo": {"vel_mh": 364.0, "tconn_min": 2.5},
    "Viaje metiendo retenedor/PBR": {"vel_mh": 364.0, "tconn_min": 2.0},
    "Viaje metiendo/levantando aplicando contrapresión (MPD)": {"vel_mh": 252.0, "tconn_min": 4.0},
    "Viaje metiendo/levantando Alineados a MPD sin aplicar contrapresión": {"vel_mh": 430.0, "tconn_min": 2.0},
    "Viaje levantando con tubería llena": {"vel_mh": 476.0, "tconn_min": 2.5},
    "Viaje metiendo y sacando con conexión a top Drive (rotación y bombeo)": {"vel_mh": 252.0, "tconn_min": 5.0},
    "Viaje metiendo y sacando con conexión a top Drive (rotación y bombeo, MPD)": {"vel_mh": 196.0, "tconn_min": 7.0},
    "Viaje con conexión reductores de fricción / removedores de recortes (cada dos lingadas)": {"vel_mh": 210.0, "tconn_min": 7.0},
    "Viaje con conexión usando llaves de fuerza": {"vel_mh": 430.0, "tconn_min": 2.9},
    "Viaje con Calibración interna de TP": {"vel_mh": 470.0, "tconn_min": 2.3},
    "Viaje Tramos dobles": {"vel_mh": 250.0, "tconn_min": 2.9},
    "Viaje levantando empacador": {"vel_mh": 364.0, "tconn_min": 2.0},
    "Viaje metiendo / levantando Aparejo doble": {"vel_mh": 75.0, "tconn_min": 3.8},
    "Viaje metiendo / levantando aparejo de producción": {"vel_mh": 124.0, "tconn_min": 3.8},
    "Viaje metiendo TP lingadas": {"vel_mh": 640.0, "tconn_min": 1.5},
    "Viaje metiendo TP TxT": {"vel_mh": 192.0, "tconn_min": 2.0},
    "Viaje levantando TP lingadas": {"vel_mh": 732.0, "tconn_min": 1.5},
    "Viaje levantando TP TxT": {"vel_mh": 219.0, "tconn_min": 2.0},
    "Viaje metiendo / levantando TP de 3 1/2\" - 2 7/8\" por lingadas": {"vel_mh": 458.0, "tconn_min": 2.9},
    "Viaje metiendo / levantando TP de 3 1/2\" - 2 7/8\" TxT": {"vel_mh": 156.0, "tconn_min": 2.9},
}

# Conexiones
CONN_COMPONENTS = [
    "Preconexión",
    "Conexión",
    "Postconexión",
    "Repaso",
    "Survey",
    "Comandos RSS",
    "Bache",
    "Presión reducida",
]
CONN_COLOR_MAP = {
    "Repaso": "#7F8C8D",
    "Preconexión": "#F9E79F",
    "Conexión": "#00A8E8",
    "Postconexión": "#D5DBDB",
    "Bache": "#48C9B0",
    "Survey": "#5B2C6F",
    "Comandos RSS": "#E67E22",
    "Presión reducida": "#85C1E9",
}
CONN_ORDER = [
    "Repaso", "Preconexión", "Survey", "Conexión",
    "Postconexión", "Bache", "Comandos RSS", "Presión reducida"
]

CONN_TYPE_OPTS = ["Fondo a fondo", "Fondo a fondo con MPD"]
ANGLE_BUCKETS = ["<30°", "30° - 60°", ">60°"]

CONN_STDS = {
    ("Fondo a fondo", "<30°"): {"Preconexión": 5, "Conexión": 5, "Postconexión": 5, "TOTAL": 15},
    ("Fondo a fondo", "30° - 60°"): {"Preconexión": 12, "Conexión": 5, "Postconexión": 5, "TOTAL": 22},
    ("Fondo a fondo", ">60°"): {"Preconexión": 25, "Conexión": 5, "Postconexión": 5, "TOTAL": 35},
    ("Fondo a fondo con MPD", "<30°"): {"Preconexión": 8, "Conexión": 7, "Postconexión": 5, "TOTAL": 20},
    ("Fondo a fondo con MPD", "30° - 60°"): {"Preconexión": 12, "Conexión": 7, "Postconexión": 8, "TOTAL": 27},
    ("Fondo a fondo con MPD", ">60°"): {"Preconexión": 25, "Conexión": 7, "Postconexión": 8, "TOTAL": 40},
}

# BHA estándares -> (objetivo arma, objetivo desarma)
BHA_TYPES = {
    1:  ("Sarta lisa y/o Empacada y/o Péndulo", 4.0, 3.0),
    2:  ("Motor - Fondo/ MLPWD", 6.5, 5.0),
    3:  ("Rotatorio / MLPWD", 6.0, 4.5),
    4:  ("Rotatorio - MLPWD - Ampliador", 7.0, 5.5),
    5:  ("Rotatorio - MWD/LWD/PWD - Densidad Neutron (fuente radioactiva)/Sónico", 8.5, 6.5),
    6:  ("Sarta de limpieza, coronas, molinos, empacador de prueba y pescante", 3.5, 2.5),
    7:  ("Cucharas (Armado/Desarmado)", 4.5, 2.5),
    8:  ("Motor o Rotatorio - MWD/LWD/PWD - Densidad Neutrón/Sónico - 1 o más ampliador", 10.5, 7.5),
    9:  ("Sartas de Jetteo para aguas profundas (Casing / liner Drilling)", 3.0, 3.5),
    10: ("Equipo de Producción/Disparos/Toma de Registros y Operaciones Terminación", 10.0, 10.0),
}

# ------------------------------
# ACRÓNIMOS y casing
# ------------------------------
ACRONYMS = {"TR", "TP", "TNPI", "TNP", "BHA", "VCP", "WITS", "MPD", "AE", "RT", "BOP", "ROP", "RSS", "CRT", "GWD", "MLPWD", "MWD", "LWD", "PWD"}

def smart_case(text: str) -> str:
    if text is None:
        return ""
    t = str(text).strip()
    if t == "":
        return ""
    base = t[:1].upper() + t[1:].lower()
    out = base
    for a in sorted(ACRONYMS, key=len, reverse=True):
        out = re.sub(rf"\b{re.escape(a.lower())}\b", a, out, flags=re.IGNORECASE)
    out = out.replace("Tnpi", "TNPI").replace("Tnp", "TNP").replace("Tp", "TP").replace("Rop", "ROP")
    return out

def clamp_0_100(x: float) -> float:
    try:
        return max(0.0, min(float(x), 100.0))
    except Exception:
        return 0.0

def safe_pct(num: float, den: float) -> float:
    return (num / den * 100.0) if den and den > 0 else 0.0

def semaforo_color(v_0_100: float) -> str:
    v = clamp_0_100(v_0_100)
    if v >= 85:
        return "#2ECC71"
    if v >= 75:
        return "#F1C40F"
    return "#E74C3C"

def status_from_eff(eff: float) -> tuple[str, str, str]:
    """returns (status_key, label, color_hex)"""
    e = clamp_0_100(eff)
    if e >= 85:
        return ("ok", "OK", "#2ECC71")
    if e >= 75:
        return ("warn", "ATENCIÓN", "#F1C40F")
    return ("crit", "CRÍTICO", "#E74C3C")

# ------------------------------
# TNPI catálogo
# ------------------------------
@st.cache_data(show_spinner=False)
def load_tnpi_catalog(csv_path: str) -> pd.DataFrame:
    """Carga catálogo de causas.
    Soporta CSVs con:
      - 2 columnas (Categoria_TNPI, Detalle_TNPI)
      - 4 columnas (Categoria_TNPI, Detalle_TNPI, Categoria_TNP, Detalle_TNP)
    Si no vienen columnas de TNP, las crea vacías (o replica TNPI como fallback).
    """
    cols_4 = ["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP"]
    cols_2 = ["Categoria_TNPI", "Detalle_TNPI"]

    if csv_path and os.path.exists(csv_path):
        try:
            df = robust_read_csv(csv_path)
        except Exception:
            df = pd.read_csv(csv_path, encoding="utf-8", errors="ignore")

        # Normaliza número de columnas
        if df.shape[1] >= 4:
            df = df.iloc[:, :4].copy()
            df.columns = cols_4
        elif df.shape[1] == 2:
            df = df.iloc[:, :2].copy()
            df.columns = cols_2
            # crea columnas TNP (fallback a TNPI para no romper UI)
            df["Categoria_TNP"] = df["Categoria_TNPI"]
            df["Detalle_TNP"] = df["Detalle_TNPI"]
        elif df.shape[1] == 3:
            df = df.iloc[:, :3].copy()
            df.columns = ["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP"]
            df["Detalle_TNP"] = ""
        else:
            # vacío / inesperado
            df = pd.DataFrame(columns=cols_4)

        for c in cols_4:
            if c not in df.columns:
                df[c] = ""
            df[c] = df[c].fillna("").astype(str).str.strip()

        return df

    # fallback mínimo
    return pd.DataFrame(columns=cols_4)

def style_for_export(fig):
    if not PLOTLY_IMG_OK:
        return fig
    f = go.Figure(fig.to_dict())
    f.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(color="black", size=14),
        margin=dict(l=40, r=40, t=70, b=40),
        legend=dict(bgcolor="rgba(255,255,255,0.85)", borderwidth=0),
        title=dict(x=0.02),
        colorway=EXPORT_COLORWAY,
    )
    return f

def plotly_to_png_bytes(fig) -> bytes | None:
    if not PLOTLY_IMG_OK:
        return None
    try:
        fig_export = style_for_export(fig)
        png = pio.to_image(fig_export, format="png", width=1400, height=800, scale=2)
        im = Image.open(BytesIO(png)).convert("RGBA")
        bg = Image.new("RGBA", im.size, (255, 255, 255, 255))
        bg.paste(im, (0, 0), im)
        out = bg.convert("RGB")
        b = BytesIO()
        out.save(b, format="PNG", optimize=True)
        return b.getvalue()
    except Exception:
        return None

def build_pdf(meta: dict, kpis: dict, charts: dict) -> bytes:
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    def write_text(txt, y, size=10, bold=False):
        c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        c.drawString(0.75 * inch, y, txt)
        return y - 0.22 * inch

    def write_chart(fig, y, title):
        img_bytes = plotly_to_png_bytes(fig)
        if img_bytes is None:
            return y
        y = write_text(title, y, bold=True)
        img_h = 3.1 * inch
        img_w = width - 1.5 * inch
        y_img = y - img_h

        if y_img < 0.75 * inch:
            c.showPage()
            y = height - 0.75 * inch
            y = write_text(title, y, bold=True)
            y_img = y - img_h

        img_reader = ImageReader(BytesIO(img_bytes))
        c.drawImage(
            img_reader,
            0.75 * inch,
            y_img,
            width=img_w,
            height=img_h,
            preserveAspectRatio=True,
            mask=None,
        )
        return y_img - 0.25 * inch

    y = height - 0.75 * inch
    y = write_text("Reporte DrillSpot / ROGII", y, size=14, bold=True)
    y = write_text(f"Equipo: {meta.get('equipo','')}", y)
    y = write_text(f"Pozo: {meta.get('pozo','')}", y)
    y = write_text(f"Etapa: {meta.get('etapa','')}", y)
    y = write_text(f"Fecha: {meta.get('fecha','')}", y)
    y -= 0.1 * inch

    y = write_text("KPIs", y, bold=True)
    for k, v in kpis.items():
        y = write_text(f"- {k}: {v}", y, size=9)
        if y < 1.0 * inch:
            c.showPage()
            y = height - 0.75 * inch

    if charts:
        c.showPage()
        y = height - 0.75 * inch
        y = write_text("Gráficas", y, size=14, bold=True)
        for name, fig in charts.items():
            y = write_chart(fig, y, name)
            if y < 1.0 * inch:
                c.showPage()
                y = height - 0.75 * inch

    c.save()
    buffer.seek(0)
    return buffer.getvalue()

def build_pptx(meta: dict, kpis: dict, charts: dict) -> bytes:
    prs = Presentation()
    prs.slide_width = Inches(13.33)
    prs.slide_height = Inches(7.5)

    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = "Reporte DrillSpot / ROGII"
    slide.placeholders[1].text = (
        f"Equipo: {meta.get('equipo','')} | Pozo: {meta.get('pozo','')} | "
        f"Etapa: {meta.get('etapa','')} | Fecha: {meta.get('fecha','')}"
    )

    slide = prs.slides.add_slide(prs.slide_layouts[5])
    slide.shapes.title.text = "KPIs"
    box = slide.shapes.add_textbox(Inches(0.8), Inches(1.4), Inches(11.7), Inches(5.2))
    tf = box.text_frame
    tf.clear()
    tf.word_wrap = True
    for i, (k, v) in enumerate(kpis.items()):
        p = tf.add_paragraph() if i > 0 else tf.paragraphs[0]
        p.text = f"{k}: {v}"
        p.font.size = Pt(18)

    for title, fig in (charts or {}).items():
        slide = prs.slides.add_slide(prs.slide_layouts[5])
        slide.shapes.title.text = title
        img_bytes = plotly_to_png_bytes(fig)
        if img_bytes is None:
            slide.shapes.add_textbox(Inches(0.8), Inches(1.6), Inches(11.5), Inches(1.0)).text_frame.text = (
                "No se pudo embebir imagen (instala kaleido)."
            )
        else:
            slide.shapes.add_picture(BytesIO(img_bytes), Inches(0.8), Inches(1.4), width=Inches(11.6))

    buf = BytesIO()
    prs.save(buf)
    buf.seek(0)
    return buf.getvalue()

# ------------------------------
# Gauge principal
# ------------------------------
def build_gauge(title: str, value_0_100: float):
    if not PLOTLY_IMG_OK:
        return None
    v = clamp_0_100(value_0_100)
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=v,
            number={"suffix": "%", "font": {"size": 70}},
            title={"text": title, "font": {"size": 26}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 1},
                "bar": {"thickness": 0.3},
                "steps": [
                    {"range": [0, 75], "color": "#E74C3C"},
                    {"range": [75, 85], "color": "#F1C40F"},
                    {"range": [85, 100], "color": "#2ECC71"},
                ],
            },
        )
    )
    fig.update_layout(
        height=360,
        margin=dict(l=20, r=20, t=60, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"),
    )
    return fig

# ------------------------------
# HTML PRO: CSS embebido
# ------------------------------

def _is_light_theme() -> bool:
    """Determina si debemos renderizar en modo claro.

    Prioridad:
    1) st.session_state['ui_mode'] (Diurno/Nocturno) — controla el look de los cards pro.
    2) theme.base de Streamlit.
    """
    try:
        ui_mode = st.session_state.get("ui_mode")
        if ui_mode in ("Diurno", "Nocturno"):
            return ui_mode == "Diurno"
    except Exception:
        pass

    try:
        base = st.get_option("theme.base")
        return str(base).lower() == "light"
    except Exception:
        return False


def _pro_iframe_css(light: bool = False) -> str:
    """CSS base for 'pro' embedded tables/cards (used in HTML iframes)."""
    if light:
        bg = "#ffffff"
        card = "#ffffff"
        border = "#e5e7eb"
        text = "#111827"
        muted = "#6b7280"
        row_hover = "#f3f4f6"
        header = "#f9fafb"
        shadow = "0 8px 18px rgba(0,0,0,.10)"
        track = "#e5e7eb"
    else:
        bg = "#0b0f14"
        card = "#0f1620"
        border = "#223043"
        text = "#e6edf3"
        muted = "#9aa7b2"
        row_hover = "#132033"
        header = "#0c121b"
        shadow = "0 8px 22px rgba(0,0,0,.35)"
        track = "#223043"

    return f"""
    <style>
      :root {{
        --bg: {bg};
        --card: {card};
        --border: {border};
        --text: {text};
        --muted: {muted};
        --row-hover: {row_hover};
        --header: {header};
        --shadow: {shadow};
        --track: {track};
      }}
      html, body {{
        margin:0; padding:0;
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial;
        background: transparent;
        color: var(--text);
      }}
      .wrap {{
        background: var(--bg);
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 18px 18px 14px 18px;
        box-shadow: var(--shadow);
      }}
      .title {{
        font-size: 28px;
        font-weight: 800;
        letter-spacing: .2px;
        margin: 4px 0 14px;
      }}
      .sub {{
        color: var(--muted);
        margin-top: -10px;
        margin-bottom: 14px;
        font-size: 14px;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        border-spacing: 0;
        overflow: hidden;
        border-radius: 14px;
      }}
      thead th {{
        text-align: left;
        font-size: 14px;
        color: var(--muted);
        font-weight: 700;
        padding: 12px 14px;
        background: var(--header);
        border-bottom: 1px solid var(--border);
      }}
      tbody td {{
        padding: 12px 14px;
        border-bottom: 1px solid var(--border);
        font-size: 16px;
      }}
      /* Numeric columns alignment (used by the activity indicator table) */
      .ds-name {{
        text-align: left;
      }}
      .ds-num {{
        text-align: right;
        font-variant-numeric: tabular-nums;
      }}
      tbody tr:hover td {{
        background: var(--row-hover);
      }}
      .kpi {{
        font-weight: 800;
        font-size: 26px;
      }}
      .pill {{
        display:inline-flex;
        align-items:center;
        gap:10px;
      }}
      .dot {{
        width: 14px; height: 14px;
        border-radius: 50%;
        display:inline-block;
      }}
      .dot.red {{ background: #ef4444; box-shadow: 0 0 0 4px rgba(239,68,68,.18); }}
      .dot.ylw {{ background: #f59e0b; box-shadow: 0 0 0 4px rgba(245,158,11,.18); }}
      .dot.grn {{ background: #22c55e; box-shadow: 0 0 0 4px rgba(34,197,94,.18); }}
      .bar {{
        width: 290px;
        height: 14px;
        border-radius: 999px;
        background: var(--track);
        overflow: hidden;
      }}
      .bar > span {{
        display:block;
        height: 100%;
        border-radius: 999px;
      }}
    </style>
    """
def kpi_table_html(rows: list[dict]) -> str:
    def dot(color, pulse=False, tooltip=""):
        cls = "dot pulse" if pulse else "dot"
        tt = f' title="{tooltip}"' if tooltip else ""
        return f'<span class="{cls}" style="background:{color};"{tt}></span>'

    tr = ""
    for r in rows:
        eff = clamp_0_100(r.get("eff", 0))
        color = semaforo_color(eff)
        pulse = eff < 75
        tooltip = "Eficiencia < 75% (revisar TNPI / causas)" if pulse else ""
        tr += f"""
        <tr>
          <td class="ds-name">{r.get('kpi','')}</td>
          <td class="ds-num">{r.get('real','')}</td>
          <td class="ds-num">{r.get('tnpi','')}</td>
          <td class="ds-num">{r.get('tnp','')}</td>
          <td class="ds-num">{eff:.0f} {dot(color, pulse=pulse, tooltip=tooltip)}</td>
        </tr>
        """

    return f"""
    {_pro_iframe_css(light=_is_light_theme())}
    <div class="ds-card">
      <div style="font-size:26px;font-weight:900;color:rgba(255,255,255,0.95);margin:2px 0 10px 0;">
        Indicador de desempeño
      </div>
      <table class="ds-t">
        <thead>
          <tr>
            <th>KPI</th>
            <th style="text-align:right;">Real</th>
            <th style="text-align:right;">TNPI</th>
            <th style="text-align:right;">TNP</th>
            <th style="text-align:right;">Eficiencia (%)</th>
          </tr>
        </thead>
        <tbody>{tr}</tbody>
      </table>
      <div style="margin-top:10px;color:rgba(255,255,255,0.70);font-size:13px;font-weight:700;display:flex;gap:18px;align-items:center;">
        <span><span class="dot" style="background:#E74C3C;"></span> &nbsp;&lt; 75%</span>
        <span><span class="dot" style="background:#F1C40F;"></span> &nbsp;75–85%</span>
        <span><span class="dot" style="background:#2ECC71;"></span> &nbsp;&ge; 85%</span>
      </div>
    </div>
    """

def indicators_table_html(title: str, rows: list[dict], kind: str = "actividad") -> str:
    def dot(color, pulse=False, tooltip=""):
        cls = "dot pulse" if pulse else "dot"
        tt = f' title="{tooltip}"' if tooltip else ""
        return f'<span class="{cls}" style="background:{color};"{tt}></span>'

    th_name = "Actividad" if kind == "actividad" else "Conexión"
    th_real = "Real (h)" if kind == "actividad" else "Real (min)"
    th_tnpi = "TNPI (h)" if kind == "actividad" else "TNPI (min)"
    th_tnp = "TNP (h)" if kind == "actividad" else "TNP (min)"

    tr = ""
    for r in rows:
        eff = clamp_0_100(r.get("eff", 0))
        color = semaforo_color(eff)
        pulse = eff < 75
        tooltip = "Eficiencia < 75% (revisar TNPI / causas)" if pulse else ""
        width = max(0, min(int(round(eff)), 100))

        tr += f"""
        <tr>
          <td class="ds-name">{r.get("name","")}</td>
          <td class="ds-num">{r.get("real","")}</td>
          <td class="ds-num">{r.get("tnpi","")}</td>
          <td class="ds-num">{r.get("tnp","")}</td>
          <td class="ds-num">
            <div class="barwrap">
              <div class="bar"><span style="width:{width}%; background:{color};"></span></div>
              <div class="pct">{eff:.0f}%</div>
            </div>
          </td>
          <td>{dot(color, pulse=pulse, tooltip=tooltip)}</td>
        </tr>
        """

    return f"""
    {_pro_iframe_css(light=_is_light_theme())}
    <div class="ds-card">
      <div style="font-size:34px;font-weight:950;color:rgba(255,255,255,0.95);margin:4px 0 12px 0;">
        {title}
      </div>
      <table class="ds-t">
        <thead>
          <tr>
            <th>{th_name}</th>
            <th style="text-align:right;">{th_real}</th>
            <th style="text-align:right;">{th_tnpi}</th>
            <th style="text-align:right;">{th_tnp}</th>
            <th style="text-align:right;">Eficiencia (%)</th>
            <th>Semáforo</th>
          </tr>
        </thead>
        <tbody>{tr}</tbody>
      </table>
    </div>
    """

# =====================================================================
# SESSION STATE INIT (ANTES del header preview!)
# =====================================================================
# --- FIX: inicializar depth_rows para Viajes ---
if "depth_rows" not in st.session_state:
    st.session_state.depth_rows = pd.DataFrame(
        columns=["Etapa", "PT_programada_m", "PT_actual_m"]
    )

if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame(
        columns=["RowID",
        "Equipo", "Pozo", "Etapa", "Fecha", "Equipo_Tipo", "Modo_Reporte",
            "Seccion", "Corrida", "Tipo_Agujero", "Operacion", "Actividad", "Turno",
            "Tipo", "Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP",
            "Horas_Prog", "Horas_Reales",
            "ROP_Prog_mh", "ROP_Real_mh",
            "Comentario", "Origen",
        ]
    )

if "df_conn" not in st.session_state:
    st.session_state.df_conn = pd.DataFrame(
        columns=[
            "Equipo", "Pozo", "Etapa", "Fecha", "Equipo_Tipo", "Seccion", "Corrida",
            "Tipo_Agujero", "Turno", "Conn_No", "Profundidad_m",
            "Conn_Tipo", "Angulo_Bucket",
            "Componente", "Minutos_Reales", "Minutos_Estandar", "Minutos_TNPI",
            "Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP", "Comentario",
        ]
    )

if "df_bha" not in st.session_state:
    st.session_state.df_bha = pd.DataFrame(
        columns=[
            "Equipo", "Pozo", "Etapa", "Fecha", "Turno",
            "Barrena", "BHA_Tipo", "BHA_Componentes", "Accion",
            "Estandar_h", "Real_h", "TNPI_h", "TNP_h", "Eficiencia_pct"
        ]
    )

if "drill_day" not in st.session_state:
    st.session_state.drill_day = {
        # Datos globales (para compatibilidad)
        "metros_prog_total": 0.0,
        "metros_real_dia": 0.0,
        "metros_real_noche": 0.0,
        "rop_prog_total": 0.0,
        "rop_real_dia": 0.0,
        "rop_real_noche": 0.0,
        "tnpi_metros_h": 0.0,
        "pt_programada_m": 0.0,
        "prof_actual_m": 0.0,

        # Nuevo: Datos por etapa
        "por_etapa": {
            # Ejemplo:
            # "36'": {"pt_prog": 1000, "prof_actual": 500, "metros_prog": 200, ...},
            # "26'": {"pt_prog": 800, "prof_actual": 300, "metros_prog": 150, ...},
        }
    }

if "custom_actividades" not in st.session_state:
    st.session_state.custom_actividades = []


# --- FIX: desglose de TNPI para BHA (múltiples causas) ---
if "bha_tnpi_breakdown" not in st.session_state:
    st.session_state.bha_tnpi_breakdown = []  # lista de dicts: {Categoria_TNPI, Detalle_TNPI, Horas_TNPI_h, Comentario}

if "bha_tnpi_breakdown_draft" not in st.session_state:
    st.session_state.bha_tnpi_breakdown_draft = []  # borrador antes de "Guardar cambios"
if "bha_tnpi_breakdown_saved" not in st.session_state:
    st.session_state.bha_tnpi_breakdown_saved = False

# --- FIX: desglose de TNPI para actividades generales (múltiples causas) ---
if "act_tnpi_breakdown" not in st.session_state:
    st.session_state.act_tnpi_breakdown = []
if "act_tnpi_breakdown_draft" not in st.session_state:
    st.session_state.act_tnpi_breakdown_draft = []
if "act_tnpi_breakdown_saved" not in st.session_state:
    st.session_state.act_tnpi_breakdown_saved = False

# FUNCIÓN PARA OBTENER/ACTUALIZAR DATOS POR ETAPA (PONER JUSTO DESPUÉS)
def get_etapa_data(etapa_nombre):
    """Obtiene o crea los datos de una etapa específica"""
    if "por_etapa" not in st.session_state.drill_day:
        st.session_state.drill_day["por_etapa"] = {}

    if etapa_nombre not in st.session_state.drill_day["por_etapa"]:
        # Crear estructura inicial para la etapa
        st.session_state.drill_day["por_etapa"][etapa_nombre] = {
            "pt_programada_m": 0.0,
            "prof_actual_m": 0.0,

            # Metros / ROP diarios (último valor capturado)
            "metros_prog_total": 0.0,
            "metros_real_dia": 0.0,
            "metros_real_noche": 0.0,
            "rop_prog_total": 0.0,
            "rop_real_dia": 0.0,
            "rop_real_noche": 0.0,

            # Metas por etapa
            "rop_prog_etapa": 0.0,

            # Históricos por fecha (para acumulados / promedios por etapa)
            "metros_real_dia_by_date": {},
            "metros_real_noche_by_date": {},
            "rop_real_dia_by_date": {},
            "rop_real_noche_by_date": {},

            "tnpi_metros_h": 0.0,
        }

    return st.session_state.drill_day["por_etapa"][etapa_nombre]

# =====================================================================
# HEADER PRO (preview eficiencia para glow/estado)
# =====================================================================
_df_prev = st.session_state.df
_total_prev = float(_df_prev["Horas_Reales"].sum()) if not _df_prev.empty else 0.0
_tp_prev = float(_df_prev[_df_prev["Tipo"] == "TP"]["Horas_Reales"].sum()) if not _df_prev.empty else 0.0
_eff_prev = clamp_0_100(safe_pct(_tp_prev, _total_prev)) if _total_prev > 0 else 0.0
_status_key, _status_label, _status_color = status_from_eff(_eff_prev)

if logo_b64:
    st.markdown(
        f"""
        <div class="ds-header" data-status="{_status_key}">
          <div class="ds-logo-wrap">
            <img class="ds-logo" src="data:{logo_mime};base64,{logo_b64}" />
          </div>
          <div style="flex:1; position:relative; z-index:1;">
            <div class="ds-title">Dashboard Diario Operativo – DrillSpot / ROGII</div>
            <div class="ds-sub">Operational Report</div>
          </div>
          <div style="display:flex; flex-direction:column; gap:8px; align-items:flex-end; position:relative; z-index:1;">
            <div class="ds-status">
              <span class="chip" style="background:{_status_color};"></span>
              Estado del día: <b>{_status_label}</b>
            </div>
            <div class="ds-status" title="Eficiencia del día (TP / Real total)">
              <span class="chip" style="background:rgba(255,255,255,0.20);"></span>
              Eficiencia: <b>{_eff_prev:.0f}%</b>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    st.markdown(
        f"""
        <div class="ds-header" data-status="{_status_key}">
          <div style="flex:1; position:relative; z-index:1;">
            <div class="ds-title">Dashboard Diario Operativo – DrillSpot / ROGII</div>
            <div class="ds-sub">Operational Report</div>
          </div>
          <div style="display:flex; flex-direction:column; gap:8px; align-items:flex-end; position:relative; z-index:1;">
            <div class="ds-status">
              <span class="chip" style="background:{_status_color};"></span>
              Estado del día: <b>{_status_label}</b>
            </div>
            <div class="ds-status" title="Eficiencia del día (TP / Real total)">
              <span class="chip" style="background:rgba(255,255,255,0.20);"></span>
              Eficiencia: <b>{_eff_prev:.0f}%</b>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )



# ------------------------------
# Toggle global (defínelo ANTES de usarlo en gráficos previos al sidebar)
# ------------------------------
show_charts = bool(st.session_state.get("show_charts", True))

# --- ROP Programado vs Real ---
# (Movido a la pestaña dedicada "ROP" para evitar duplicidad/confusión)

st.divider()


# ------------------------------
# MODO DE REPORTE (DEFAULT SEGURO)
# ------------------------------
# Se usa antes del sidebar (por el bloque Avance de profundidad).
modo_reporte = st.session_state.get("modo_reporte", MODO_REPORTE_OPTS[0])


# =====================================================================
# GUARDAR / CARGAR JORNADA (JSON local)
# =====================================================================
def _default_jornada_path(equipo: str, pozo: str, fecha_str: str) -> str:
    safe = lambda s: re.sub(r"[^A-Za-z0-9_-]+", "_", str(s)).strip("_")
    script_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
    return os.path.join(script_dir, f"jornada_{safe(equipo)}_{safe(pozo)}_{safe(fecha_str)}.json")

def save_jornada_json(path_out: str) -> None:
    # Meta/contexto del sidebar (para reconstrucción confiable al cargar)
    meta = {
        "equipo": st.session_state.get("equipo_val", ""),
        "pozo": st.session_state.get("pozo_val", ""),
        "fecha": str(st.session_state.get("fecha_val", "")),
        "equipo_tipo": st.session_state.get("equipo_tipo_val", ""),
        "etapa_manual": bool(st.session_state.get("etapa_manual_chk", False)),
        "etapa": st.session_state.get("etapa_sel", ""),
        "etapa_manual_val": st.session_state.get("etapa_manual_val", ""),
        "modo_reporte": st.session_state.get("modo_reporte", ""),
        "show_charts": bool(st.session_state.get("show_charts", True)),
    }

    # También guardamos el meta dentro de drill_day para que quede autocontenido
    st.session_state.drill_day["meta"] = meta

    payload = {
        "version": "1.1",
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "meta": meta,
        "df": st.session_state.df.to_dict(orient="records"),
        "df_conn": st.session_state.df_conn.to_dict(orient="records"),
        "df_bha": st.session_state.df_bha.to_dict(orient="records"),
        "drill_day": st.session_state.drill_day,
        "custom_actividades": st.session_state.get("custom_actividades", []),
    }
    with open(path_out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def load_jornada_json(path_in: str) -> bool:
    if not path_in or not os.path.exists(path_in):
        return False
    with open(path_in, "r", encoding="utf-8") as f:
        payload = json.load(f)

    # Tablas
    st.session_state.df = pd.DataFrame(payload.get("df", []), columns=st.session_state.df.columns)
    st.session_state.df_conn = pd.DataFrame(payload.get("df_conn", []), columns=st.session_state.df_conn.columns)
    st.session_state.df_bha = pd.DataFrame(payload.get("df_bha", []), columns=st.session_state.df_bha.columns)

    # drill_day + meta
    st.session_state.drill_day = payload.get("drill_day", st.session_state.drill_day) or st.session_state.drill_day
    meta = payload.get("meta") or st.session_state.drill_day.get("meta") or {}

    # Actividades personalizadas
    st.session_state.custom_actividades = payload.get("custom_actividades", []) or []
    # Restauración segura del sidebar: NO modificar keys de widgets después de instanciados.
    # Guardamos valores para aplicarlos al inicio del script (antes de render del sidebar) y forzamos rerun.
    if meta:
        pending = {
            'equipo_val': meta.get('equipo', ''),
            'pozo_val': meta.get('pozo', ''),
        }

        # fecha viene como string "YYYY-MM-DD" o "YYYY/MM/DD"
        _fecha_raw = str(meta.get('fecha', ''))
        _fecha = None
        for fmt in ('%Y-%m-%d', '%Y/%m/%d'):
            try:
                _fecha = datetime.strptime(_fecha_raw, fmt).date()
                break
            except Exception:
                pass
        if _fecha is not None:
            pending['fecha_val'] = _fecha

        pending['equipo_tipo_val'] = meta.get('equipo_tipo', '')
        pending['etapa_manual_chk'] = bool(meta.get('etapa_manual', False))
        pending['etapa_sel'] = meta.get('etapa', meta.get('etapa_manual_val', ''))
        pending['etapa_manual_val'] = meta.get('etapa_manual_val', meta.get('etapa', ''))

        if 'modo_reporte' in meta:
            pending['modo_reporte'] = meta.get('modo_reporte', st.session_state.get('modo_reporte', ''))
        if 'show_charts' in meta:
            pending['show_charts'] = bool(meta.get('show_charts', True))

        st.session_state['_pending_sidebar_restore'] = pending
        # Mantener meta también dentro de drill_day
        st.session_state.drill_day['meta'] = meta

    return True

# =====================================================================
# SIDEBAR (con modo presentación)
# =====================================================================
# --- RESTORE SEGURO: aplicar valores cargados ANTES de instanciar widgets ---
_pending = st.session_state.pop('_pending_sidebar_restore', None)
if isinstance(_pending, dict) and _pending:
    for _k, _v in _pending.items():
        try:
            st.session_state[_k] = _v
        except Exception:
            pass

st.sidebar.title("Panel de Control")
presentacion = st.sidebar.toggle("Modo presentación (ocultar sidebar)", value=False)

if presentacion:
    st.markdown(
        """
        <style>
          [data-testid="stSidebar"] { display: none; }
          .block-container { padding-top: 1.0rem; }
        </style>
        """,
        unsafe_allow_html=True
    )

with st.sidebar.container(border=True):
    st.sidebar.markdown("### Reporte")
    equipo = st.sidebar.text_input("Equipo", value=st.session_state.get("equipo_val","PM 2402"), key="equipo_val")
    equipo_tipo = st.sidebar.selectbox("Tipo de equipo", options=EQUIPO_TIPO, index=EQUIPO_TIPO.index(st.session_state.get("equipo_tipo_val", EQUIPO_TIPO[0])) if st.session_state.get("equipo_tipo_val", EQUIPO_TIPO[0]) in EQUIPO_TIPO else 0, key="equipo_tipo_val")
    pozo = st.sidebar.text_input("Pozo", value=st.session_state.get("pozo_val","OME 1 EXP"), key="pozo_val")
    # Etapa (sección) - lista + opción manual (para casos especiales)
    etapa_manual = st.sidebar.checkbox("Etapa manual", value=bool(st.session_state.get("etapa_manual_chk", False)), key="etapa_manual_chk", help="Actívalo si necesitas escribir una etapa que no esté en la lista.")
    if etapa_manual:
        etapa = st.sidebar.text_input("Etapa (manual)", value=st.session_state.get("etapa_manual_val", SECCIONES_DEFAULT[2]), key="etapa_manual_input")
        st.session_state["etapa_manual_val"] = etapa
    else:
        _default_etapa = st.session_state.get("etapa_sel", SECCIONES_DEFAULT[2])
        _idx = SECCIONES_DEFAULT.index(_default_etapa) if _default_etapa in SECCIONES_DEFAULT else 2
        etapa = st.sidebar.selectbox("Etapa", SECCIONES_DEFAULT, index=_idx, key="etapa_select")
        st.session_state["etapa_sel"] = etapa
    fecha = st.sidebar.date_input("Fecha", value=st.session_state.get("fecha_val", datetime.today().date()), key="fecha_val")
    
# --- Sync contexto actual a drill_day/meta (para que el JSON siempre quede completo) ---
_meta_now = {
    "equipo": st.session_state.get("equipo_val", ""),
    "pozo": st.session_state.get("pozo_val", ""),
    "fecha": str(fecha),
    "equipo_tipo": st.session_state.get("equipo_tipo_val", ""),
    "etapa_manual": bool(etapa_manual),
    "etapa": etapa,
    "etapa_manual_val": st.session_state.get("etapa_manual_val", ""),
    "modo_reporte": st.session_state.get("modo_reporte", ""),
    "show_charts": bool(st.session_state.get("show_charts", True)),
}
st.session_state.drill_day["meta"] = _meta_now

with st.sidebar.container(border=True):
    st.sidebar.markdown("### Jornada (guardar / cargar)")

    # Nombre sugerido del archivo (solo nombre, sin ruta)
    _fname_full = _default_jornada_path(equipo, pozo, str(fecha))
    _fname = os.path.basename(_fname_full)

    # Construir payload completo (incluye meta, df, conexiones, BHA, drill_day, etc.)
    def _build_jornada_payload() -> dict:
        meta = {
            "equipo": st.session_state.get("equipo_val", ""),
            "pozo": st.session_state.get("pozo_val", ""),
            "fecha": str(st.session_state.get("fecha_val", "")),
            "equipo_tipo": st.session_state.get("equipo_tipo_val", ""),
            "etapa_manual": bool(st.session_state.get("etapa_manual_chk", False)),
            "etapa": st.session_state.get("etapa_sel", ""),
            "etapa_manual_val": st.session_state.get("etapa_manual_val", ""),
            "modo_reporte": st.session_state.get("modo_reporte", ""),
            "show_charts": bool(st.session_state.get("show_charts", True)),
        }
        # Autocontenible
        st.session_state.drill_day["meta"] = meta

        return {
            "version": "1.2",
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "meta": meta,
            "df": st.session_state.df.to_dict(orient="records"),
            "df_conn": st.session_state.df_conn.to_dict(orient="records"),
            "df_bha": st.session_state.df_bha.to_dict(orient="records"),
            "drill_day": st.session_state.drill_day,
            "custom_actividades": st.session_state.get("custom_actividades", []),
        }

    _payload = _build_jornada_payload()
    _payload_str = json.dumps(_payload, ensure_ascii=False, indent=2)

    # --- Guardar jornada (DESCARGA) ---
    st.sidebar.download_button(
        label="Guardar jornada (.json)",
        data=_payload_str,
        file_name=_fname,
        mime="application/json",
        use_container_width=True,
    )

    st.sidebar.divider()

    # --- Cargar jornada (SUBIR ARCHIVO) ---
    up_jornada = st.sidebar.file_uploader(
        "Cargar jornada (.json)",
        type=["json"],
        accept_multiple_files=False,
        key="jornada_uploader",
        help="Carga un .json previamente guardado para continuar donde se dejó (incluye etapa, estadísticas, etc.).",
    )

    if st.sidebar.button("Aplicar jornada", use_container_width=True, disabled=(up_jornada is None)):
        try:
            payload = json.loads(up_jornada.getvalue().decode("utf-8"))
        except Exception as e:
            st.sidebar.error(f"No se pudo leer el JSON: {e}")
            payload = None

        if isinstance(payload, dict):
            # Reusar loader existente, pero desde dict
            # Guardamos temporalmente en memoria y aplicamos como si fuera load_jornada_json
            try:
                # Tablas
                st.session_state.df = pd.DataFrame(payload.get("df", []), columns=st.session_state.df.columns)
                st.session_state.df_conn = pd.DataFrame(payload.get("df_conn", []), columns=st.session_state.df_conn.columns)
                st.session_state.df_bha = pd.DataFrame(payload.get("df_bha", []), columns=st.session_state.df_bha.columns)

                # drill_day + meta
                st.session_state.drill_day = payload.get("drill_day", st.session_state.drill_day) or st.session_state.drill_day
                meta = payload.get("meta") or st.session_state.drill_day.get("meta") or {}

                # Actividades personalizadas
                st.session_state.custom_actividades = payload.get("custom_actividades", []) or []

                # Restauración segura (aplicar al inicio del script)
                if meta:
                    pending = {
                        "equipo_val": meta.get("equipo", ""),
                        "pozo_val": meta.get("pozo", ""),
                    }
                    _fecha_raw = str(meta.get("fecha", ""))
                    _fecha = None
                    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
                        try:
                            _fecha = datetime.strptime(_fecha_raw, fmt).date()
                            break
                        except Exception:
                            pass
                    if _fecha is not None:
                        pending["fecha_val"] = _fecha

                    pending["equipo_tipo_val"] = meta.get("equipo_tipo", "")
                    pending["etapa_manual_chk"] = bool(meta.get("etapa_manual", False))
                    pending["etapa_sel"] = meta.get("etapa", meta.get("etapa_manual_val", ""))
                    pending["etapa_manual_val"] = meta.get("etapa_manual_val", meta.get("etapa", ""))

                    if "modo_reporte" in meta:
                        pending["modo_reporte"] = meta.get("modo_reporte", st.session_state.get("modo_reporte", ""))
                    if "show_charts" in meta:
                        pending["show_charts"] = bool(meta.get("show_charts", True))

                    st.session_state["_pending_sidebar_restore"] = pending
                    st.session_state.drill_day["meta"] = meta

                st.sidebar.success("Jornada cargada ✅")
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"No se pudo aplicar la jornada: {e}")

with st.sidebar.container(border=True):
    st.sidebar.markdown("### Modo")
    modo_reporte = st.sidebar.radio(
        "Tipo",
        MODO_REPORTE_OPTS,
        index=MODO_REPORTE_OPTS.index(modo_reporte) if modo_reporte in MODO_REPORTE_OPTS else 0
    )
    st.session_state["modo_reporte"] = modo_reporte

# Toggle liviano para evitar render pesado
show_charts = st.sidebar.toggle(
    "Mostrar gráficas (mejor rendimiento)",
    value=bool(st.session_state.get("show_charts", True)),
    key="show_charts",
)

with st.sidebar.container(border=True):
    st.sidebar.markdown("### Catálogo TNPI (CSV)")
    script_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
    candidate_local = os.path.join(script_dir, "Detalles causas de TNPI.csv")
    csv_path_use = TNPI_CSV_PATH if (TNPI_CSV_PATH and os.path.exists(TNPI_CSV_PATH)) else (candidate_local if os.path.exists(candidate_local) else "")
    up = st.sidebar.file_uploader("Cargar CSV", type=["csv"], accept_multiple_files=False)

    if up is not None:
        try:
            df_tnpi_cat = robust_read_csv(up)
        except Exception as e:
            df_tnpi_cat = None
            st.sidebar.error(f"Error leyendo CSV TNPI: {e}")

        if df_tnpi_cat is not None and not df_tnpi_cat.empty:
            det_col = None
            cat_col = None
            for c in df_tnpi_cat.columns:
                cl = str(c).lower()
                if det_col is None and ("detalle" in cl or "causa" in cl):
                    det_col = c
                if cat_col is None and ("categoria" in cl or "categoría" in cl):
                    cat_col = c

            # Fallbacks comunes
            if det_col is None:
                for cand in ["Detalle de causa de TNPI", "Detalle", "Causa", "Detalle_TNPI"]:
                    if cand in df_tnpi_cat.columns:
                        det_col = cand
                        break
            if cat_col is None:
                for cand in ["Categoria", "Categoría", "Categoria_TNPI"]:
                    if cand in df_tnpi_cat.columns:
                        cat_col = cand
                        break

            if det_col is None or cat_col is None:
                st.sidebar.error("No pude identificar columnas de Categoria/Detalle en el CSV TNPI.")
            else:
                df_tnpi_cat = df_tnpi_cat[[cat_col, det_col]].copy()
                df_tnpi_cat.columns = ["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP"]
                df_tnpi_cat["Categoria_TNPI"] = df_tnpi_cat["Categoria_TNPI"].apply(smart_case)
                df_tnpi_cat["Detalle_TNPI"] = df_tnpi_cat["Detalle_TNPI"].apply(smart_case)
                df_tnpi_cat = df_tnpi_cat.dropna().drop_duplicates().reset_index(drop=True)
                st.sidebar.success("CSV TNPI cargado")
    else:
        df_tnpi_cat = load_tnpi_catalog(csv_path_use)
        if not csv_path_use:
            st.sidebar.warning("No se encontró el CSV. Usando catálogo mínimo.")


# ------------------------------
# Catálogo de causas TNP (Tiempo No Productivo) - similar a TNPI
# ------------------------------
def load_tnp_catalog(path_csv: str) -> pd.DataFrame:
    """Carga catálogo TNP desde CSV. Soporta utf-8 / latin-1."""
    if not path_csv or not os.path.exists(path_csv):
        return pd.DataFrame({"Categoria_TNP": ["-"], "Detalle_TNP": ["-"]})
    try:
        df0 = robust_read_csv(path_csv)
    except Exception:
        df0 = None
    if df0 is None or df0.empty:
        return pd.DataFrame({"Categoria_TNP": ["-"], "Detalle_TNP": ["-"]})

    det_col = None
    cat_col = None
    for c in df0.columns:
        cl = str(c).lower()
        if det_col is None and ("detalle" in cl or "causa" in cl):
            det_col = c
        if cat_col is None and ("categoria" in cl or "categoría" in cl):
            cat_col = c
    # Fallback por nombres esperados del archivo que nos compartiste
    if cat_col is None and "Categoria" in df0.columns:
        cat_col = "Categoria"
    if det_col is None and "Detalle de causa de TNP" in df0.columns:
        det_col = "Detalle de causa de TNP"

    if cat_col is None or det_col is None:
        # Intento: tomar primeras 2 cols
        cols = list(df0.columns)[:2]
        if len(cols) >= 2:
            cat_col, det_col = cols[1], cols[0]

    df0 = df0[[cat_col, det_col]].copy()
    df0.columns = ["Categoria_TNP", "Detalle_TNP"]
    df0["Categoria_TNP"] = df0["Categoria_TNP"].astype(str).apply(smart_case)
    df0["Detalle_TNP"] = df0["Detalle_TNP"].astype(str).apply(smart_case)
    df0 = df0.dropna().drop_duplicates().reset_index(drop=True)
    return df0

# Default: buscamos el CSV TNP junto al script o en rutas conocidas
TNP_CSV_PATH = ""
candidate_tnp_local = os.path.join(os.getcwd(), "Detalles causas de TNP2.csv")
candidate_tnp_alt = os.path.join(os.path.dirname(__file__) if "__file__" in globals() else os.getcwd(), "Detalles causas de TNP2.csv")
if os.path.exists(candidate_tnp_local):
    TNP_CSV_PATH = candidate_tnp_local
elif os.path.exists(candidate_tnp_alt):
    TNP_CSV_PATH = candidate_tnp_alt

st.sidebar.markdown("---")
st.sidebar.subheader("Catálogo TNP (causas)")
up_tnp = st.sidebar.file_uploader("Cargar CSV TNP", type=["csv"], accept_multiple_files=False, key="up_tnp_cat")

if up_tnp is not None:
    try:
        # DrillSpot exports a veces vienen con delimitador/encoding irregular
        df_tnp_cat = robust_read_csv(up_tnp)
    except Exception as e:
        df_tnp_cat = None
        st.sidebar.error(f"Error leyendo CSV TNP: {e}")

    if df_tnp_cat is not None and not df_tnp_cat.empty:
        det_col = None
        cat_col = None
        for c in df_tnp_cat.columns:
            cl = str(c).lower()
            if det_col is None and ("detalle" in cl or "causa" in cl):
                det_col = c
            if cat_col is None and ("categoria" in cl or "categoría" in cl):
                cat_col = c

        # Fallbacks comunes
        if cat_col is None:
            for cand in ["Categoria", "Categoría", "Categoria_TNP"]:
                if cand in df_tnp_cat.columns:
                    cat_col = cand
                    break
        if det_col is None:
            for cand in ["Detalle de causa de TNP", "Detalle", "Causa", "Detalle_TNP"]:
                if cand in df_tnp_cat.columns:
                    det_col = cand
                    break

        if det_col is None or cat_col is None:
            st.sidebar.error("No pude identificar columnas de Categoria/Detalle en el CSV TNP.")
        else:
            df_tnp_cat = df_tnp_cat[[cat_col, det_col]].copy()
            df_tnp_cat.columns = ["Categoria_TNP", "Detalle_TNP"]
            df_tnp_cat["Categoria_TNP"] = df_tnp_cat["Categoria_TNP"].astype(str).apply(smart_case)
            df_tnp_cat["Detalle_TNP"] = df_tnp_cat["Detalle_TNP"].astype(str).apply(smart_case)
            df_tnp_cat = df_tnp_cat.dropna().drop_duplicates().reset_index(drop=True)
            st.sidebar.success("CSV TNP cargado")
else:
    df_tnp_cat = load_tnp_catalog(TNP_CSV_PATH)
    if not TNP_CSV_PATH:
        st.sidebar.warning("No se encontró CSV TNP. Usando catálogo mínimo.")

tnp_cat_list = sorted(df_tnp_cat["Categoria_TNP"].dropna().unique().tolist())

cat_list = sorted(df_tnpi_cat["Categoria_TNPI"].dropna().unique().tolist()) or ["Proceso"]

# Inputs perforación (metros/ROP) + PT/Prof actual
if modo_reporte == "Perforación":
    with st.sidebar.container(border=True):
        st.sidebar.markdown("### Profundidad (avance) - Por Etapa")
        
        # Obtener datos específicos de esta etapa
        etapa_data = get_etapa_data(etapa)
        
        etapa_data["pt_programada_m"] = st.sidebar.number_input(
            f"PT programada (m) - {etapa}",
            0.0, step=1.0, 
            value=float(etapa_data["pt_programada_m"])
        )
        
        etapa_data["rop_prog_etapa"] = st.sidebar.number_input(
            f"ROP programada (m/h) - {etapa}",
            0.0, step=0.1,
            value=float(etapa_data.get("rop_prog_etapa", 0.0))
        )

        etapa_data["prof_actual_m"] = st.sidebar.number_input(
            f"Profundidad actual (m) - {etapa}",
            0.0, step=1.0, 
            value=float(etapa_data["prof_actual_m"])
        )
        
        # Mantener compatibilidad con datos globales (opcional)
        st.session_state.drill_day["pt_programada_m"] = etapa_data["pt_programada_m"]
        st.session_state.drill_day["prof_actual_m"] = etapa_data["prof_actual_m"]

    # (Metros perforados (día) movido a la pestaña ROP)
# CONTEXTO ACTUAL (PONER DESPUÉS DE LOS INPUTS DE PROFUNDIDAD)
with st.sidebar.container(border=True):
    st.sidebar.markdown("### Contexto Actual")
    
    # Mostrar claramente qué etapa estamos trabajando
    st.sidebar.markdown(f"""
        <div style='background: rgba(40, 180, 99, 0.1); padding: 8px; border-radius: 8px; border-left: 3px solid #28B463; margin-bottom: 10px;'>
            <div style='font-size: 12px; color: #28B463;'>Etapa actual:</div>
            <div style='font-size: 16px; color: white; font-weight: bold;'>{etapa}</div>
        </div>
    """, unsafe_allow_html=True)
    
    # Indicador de qué datos se están capturando
    if modo_reporte == "Perforación":
        # Contar actividades en esta etapa
                # FIX: usar siempre los DataFrames del session_state (df aún no está definido aquí)
        _df_loc = st.session_state.df
        _dfc_loc = st.session_state.df_conn
        actividades_etapa = len(_df_loc[_df_loc["Etapa"] == etapa]) if not _df_loc.empty else 0
        conexiones_etapa = len(_dfc_loc[_dfc_loc["Etapa"] == etapa]) if not _dfc_loc.empty else 0
        
        st.sidebar.markdown(f"""
            <div style='font-size: 12px; color: rgba(255,255,255,0.7);'>
                📊 <b>Actividades:</b> {actividades_etapa}<br>
                🔗 <b>Conexiones:</b> {conexiones_etapa}
            </div>
        """, unsafe_allow_html=True)

    st.sidebar.markdown("### Captura actividad (general)")
    corrida = st.sidebar.text_input("Corrida (Run)", "Run 1")
    tipo_agujero = st.sidebar.radio("Tipo de agujero", TIPO_AGUJERO, horizontal=True)
    turno = st.sidebar.radio("Turno", TURNOS, horizontal=True)

    operacion = "Perforación" if modo_reporte == "Perforación" else st.sidebar.selectbox(
        "Operación", ["Superficie", "TR", "Otra"], index=0
    )

    # --- Actividad (catálogo + personalizadas + otra) ---
    actividades_opts = ACTIVIDADES + sorted(st.session_state.get("custom_actividades", []))
    actividad_sel = st.sidebar.selectbox("Actividad", actividades_opts + ["Otra (especificar)"])

    actividad = actividad_sel
    if actividad_sel == "Otra (especificar)":
        actividad = st.sidebar.text_input("Especifica actividad", "", key="actividad_otro").strip()

    # Tipo de tiempo (SIEMPRE visible)
    tipo = st.sidebar.radio("Tipo de tiempo", ["TP", "TNPI", "TNP"], horizontal=True, key="tipo_time_general")

    # -------------------------------------------------
    # Helper: Viajes (calcular estándar sugerido)
    # Estándar (h) = distancia(m)/velocidad(m/h) + conexiones * tconn(min)/60
    # -------------------------------------------------
    if actividad in VIAJE_CATALOG:
        with st.sidebar.expander("Viaje – calculadora estándar (TNPI)", expanded=False):
            v = float(VIAJE_CATALOG[actividad].get("vel_mh", 0.0) or 0.0)
            tc = float(VIAJE_CATALOG[actividad].get("tconn_min", 0.0) or 0.0)

            etapa_viajes_sel = st.selectbox("Etapa base para viaje", options=st.session_state.depth_rows["Etapa"].tolist(), index=0)
            _drow = st.session_state.depth_rows[st.session_state.depth_rows["Etapa"] == etapa_viajes_sel].iloc[0]
            pt_prog_v = float(_drow["PT_programada_m"] or 0.0)
            pt_act_v = float(_drow["PT_actual_m"] or 0.0)

            dist = st.number_input("Distancia (m)", min_value=0.0, value=max(pt_prog_v - pt_act_v, 0.0), step=10.0, key="dist_viaje")
            conexiones_etapa = len(st.session_state.df_conn[st.session_state.df_conn["Etapa"] == etapa_viajes_sel]) if "df_conn" in st.session_state else 0
            nconn = st.number_input("Conexiones (#)", min_value=0, value=int(conexiones_etapa), step=1, key="nconn_viaje")

            if v > 0:
                est = dist / v + (nconn * tc / 60.0)
                st.caption(f"Estándar sugerido: {est:.2f} h (v={v:.0f} m/h, tconn={tc:.1f} min)")
            else:
                st.caption("Configura vel_mh > 0 en VIAJE_CATALOG para cálculo automático.")

    # Detalles TNPI/TNP (SIEMPRE disponibles cuando aplique)
    categoria_tnpi = "-"
    detalle_tnpi = "-"
    categoria_tnp = "-"
    detalle_tnp = "-"

    if tipo == "TNPI":
        # Usa el catálogo TNPI cargado (df_tnpi_cat) y su lista de categorías (cat_list)
        categoria_tnpi = st.sidebar.selectbox(
            "Categoría TNPI",
            options=cat_list if "cat_list" in globals() else ["-"],
            key="cat_tnpi_general",
        )
        det_all = (
            df_tnpi_cat[df_tnpi_cat["Categoria_TNPI"] == categoria_tnpi]["Detalle_TNPI"].tolist()
            if "df_tnpi_cat" in globals()
            else ["-"]
        )
        q = (st.sidebar.text_input("Buscar detalle TNPI", "", key="q_tnpi_general") or "").strip().lower()
        det_filtered = [d for d in det_all if q in str(d).lower()] if q else det_all
        detalle_tnpi = st.sidebar.selectbox(
            "Detalle TNPI",
            options=det_filtered if det_filtered else det_all,
            key="det_tnpi_general",
        )

    elif tipo == "TNP":
        categoria_tnp = st.sidebar.selectbox(
            "Categoría TNP",
            options=tnp_cat_list if "tnp_cat_list" in globals() else ["-"],
            key="cat_tnp_general",
        )
        det_all_tnp = (
            df_tnp_cat[df_tnp_cat["Categoria_TNP"] == categoria_tnp]["Detalle_TNP"].tolist()
            if "df_tnp_cat" in globals()
            else ["-"]
        )
        q2 = (st.sidebar.text_input("Buscar detalle TNP", "", key="q_tnp_general") or "").strip().lower()
        det_filtered_tnp = [d for d in det_all_tnp if q2 in str(d).lower()] if q2 else det_all_tnp
        detalle_tnp = st.sidebar.selectbox(
            "Detalle TNP",
            options=det_filtered_tnp if det_filtered_tnp else det_all_tnp,
            key="det_tnp_general",
        )

    horas_prog = st.sidebar.number_input("Horas estándar / programadas (h)", 0.0, step=0.25, key="hp_general")
    horas_real = st.sidebar.number_input("Horas reales (h)", 0.0, step=0.25, key="hr_general")
    rop_prog = 0.0
    rop_real = 0.0
    # ROP por actividad (opcional) se centraliza en la pestaña "ROP" para evitar confusión.

    

    # --- Auto TNPI por exceso (solo cuando capturas como TP y hay estándar) ---

    tnpi_exceso_h = 0.0

    if tipo == "TP" and float(horas_prog) > 0:
        tnpi_exceso_h = max(0.0, float(horas_real) - float(horas_prog))

    if tnpi_exceso_h > 0:
        st.sidebar.markdown(f"**TNPI por exceso detectado:** {tnpi_exceso_h:.2f} h")
        st.sidebar.caption(f"(Real {float(horas_real):.2f} h − Estándar {float(horas_prog):.2f} h)")

        # --- Exceso (Real > Estándar) ---
        st.sidebar.markdown("**Asignación del exceso (Real > Estándar)**")

        exceso_tipo = st.sidebar.radio(
            "¿Cómo registrar el exceso?",
            options=["TNPI", "TNP"],
            horizontal=True,
            key="exceso_tipo_general_choice",
        )
        st.session_state["exceso_tipo_general"] = exceso_tipo

        # Helpers de catálogo
        _has_cat = "df_tnpi_cat" in globals() and isinstance(df_tnpi_cat, pd.DataFrame)

        # --- Asignación simple (una sola causa) ---
        if exceso_tipo == "TNPI":
            st.sidebar.markdown("**Asignación simple (TNPI)**")
            act_cat_simple = st.sidebar.selectbox(
                "Categoría TNPI (exceso)",
                options=cat_list if "cat_list" in globals() else ["-"],
                index=0,
                key="act_cat_simple",
            )

            if _has_cat and "Categoria_TNPI" in df_tnpi_cat.columns and "Detalle_TNPI" in df_tnpi_cat.columns:
                _det_opts_act = (
                    df_tnpi_cat[df_tnpi_cat["Categoria_TNPI"] == act_cat_simple]["Detalle_TNPI"]
                    .dropna()
                    .unique()
                    .tolist()
                )
            else:
                _det_opts_act = ["-"]

            act_det_simple = st.sidebar.selectbox(
                "Detalle TNPI (exceso)",
                options=_det_opts_act if len(_det_opts_act) else ["-"],
                index=0,
                key="act_det_simple",
            )

            # placeholders para no romper lógica abajo
            act_cat_simple_tnp = "-"
            act_det_simple_tnp = "-"

        else:
            st.sidebar.markdown("**Asignación simple (TNP)**")

            # Lista categorías TNP (fallback: TNPI si no existe catálogo TNP)
            if _has_cat and "Categoria_TNP" in df_tnpi_cat.columns:
                _cat_tnp = df_tnpi_cat["Categoria_TNP"].dropna().unique().tolist()
                _cat_tnp = [c for c in _cat_tnp if str(c).strip() not in ("", "-", "nan", "None")]
            else:
                _cat_tnp = []

            if not _cat_tnp and "cat_list" in globals():
                _cat_tnp = cat_list

            act_cat_simple_tnp = st.sidebar.selectbox(
                "Categoría TNP (exceso)",
                options=_cat_tnp if _cat_tnp else ["-"],
                index=0,
                key="act_cat_simple_tnp",
            )

            if _has_cat and "Categoria_TNP" in df_tnpi_cat.columns and "Detalle_TNP" in df_tnpi_cat.columns:
                _det_opts_tnp = (
                    df_tnpi_cat[df_tnpi_cat["Categoria_TNP"] == act_cat_simple_tnp]["Detalle_TNP"]
                    .dropna()
                    .unique()
                    .tolist()
                )
            else:
                _det_opts_tnp = ["-"]

            act_det_simple_tnp = st.sidebar.selectbox(
                "Detalle TNP (exceso)",
                options=_det_opts_tnp if len(_det_opts_tnp) else ["-"],
                index=0,
                key="act_det_simple_tnp",
            )

            # placeholders para no romper lógica TNPI abajo
            act_cat_simple = "-"
            act_det_simple = "-"
# --- Desglose opcional (múltiples causas) ---
        with st.sidebar.expander("Detalle TNPI (opcional) — desglose por múltiples causas", expanded=False):
            st.caption("Agrega varias causas y presiona **Guardar**. La suma debe ser igual al TNPI por exceso detectado.")

            if (not st.session_state.get("act_tnpi_breakdown_draft")) and (not st.session_state.get("act_tnpi_breakdown")):
                st.session_state.act_tnpi_breakdown_draft = [{
                    "Categoria_TNPI": (cat_list[0] if "cat_list" in globals() and isinstance(cat_list, list) and len(cat_list) else "-"),
                    "Detalle_TNPI": "-",
                    "Horas_TNPI_h": float(tnpi_exceso_h),
                    "Comentario": "",
                }]

            _cat_new_act = st.selectbox(
                "Categoría TNPI (nuevo renglón)",
                options=cat_list if "cat_list" in globals() else ["-"],
                index=0,
                key="act_bd_cat_new",
            )

            if "df_tnpi_cat" in globals() and "Categoria_TNPI" in df_tnpi_cat.columns and "Detalle_TNPI" in df_tnpi_cat.columns:
                _det_new_opts_act = (
                    df_tnpi_cat[df_tnpi_cat["Categoria_TNPI"] == _cat_new_act]["Detalle_TNPI"]
                    .dropna()
                    .unique()
                    .tolist()
                )
            else:
                _det_new_opts_act = ["-"]

            _det_new_act = st.selectbox(
                "Detalle TNPI (nuevo renglón)",
                options=_det_new_opts_act if len(_det_new_opts_act) else ["-"],
                index=0,
                key="act_bd_det_new",
            )

            _h_new_act = st.number_input(
                "Horas TNPI (h) (nuevo renglón)",
                min_value=0.0,
                value=0.0,
                step=0.25,
                format="%.2f",
                key="act_bd_h_new",
            )

            _com_new_act = st.text_input("Comentario (nuevo renglón)", "", key="act_bd_com_new")

            # --- Botones del desglose ---
            if st.button("Agregar", use_container_width=True, key="act_bd_add"):
                if _h_new_act <= 0:
                    st.warning("Las horas deben ser > 0 para agregar la causa.")
                else:
                    st.session_state.act_tnpi_breakdown_draft.append({
                        "Categoria_TNPI": _cat_new_act,
                        "Detalle_TNPI": _det_new_act,
                        "Horas_TNPI_h": float(_h_new_act),
                        "Comentario": _com_new_act,
                    })
                    st.rerun()

            if st.button("Limpiar", use_container_width=True, key="act_bd_clear"):
                st.session_state.act_tnpi_breakdown_draft = []
                st.session_state.act_tnpi_breakdown_saved = False
                st.rerun()

            # Tabla borrador
            bd_act = pd.DataFrame(
                st.session_state.get("act_tnpi_breakdown_draft", []),
                columns=["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP", "Horas_TNPI_h", "Comentario"],
            )
            bd_act["Horas_TNPI_h"] = pd.to_numeric(bd_act["Horas_TNPI_h"], errors="coerce").fillna(0.0)

            st.markdown("**Causas en borrador**")
            if bd_act.empty:
                st.info("Aún no hay causas en el borrador.")
            else:
                st.dataframe(bd_act, use_container_width=True, hide_index=True)

            sum_bd_act = float(bd_act["Horas_TNPI_h"].sum()) if not bd_act.empty else 0.0
            st.caption(
                f"Suma borrador: **{sum_bd_act:.2f} h**  |  TNPI por exceso: **{float(tnpi_exceso_h):.2f} h**"
            )

            if st.button("Guardar", use_container_width=True, key="act_bd_save"):
                if bd_act.empty:
                        st.error("No hay renglones en el borrador para guardar.")
                else:
                    bd2 = bd_act[bd_act["Horas_TNPI_h"] > 0].copy()
                    sum2 = float(bd2["Horas_TNPI_h"].sum())
                    if abs(sum2 - float(tnpi_exceso_h)) > 1e-6:
                        st.error(
                            f"La suma del desglose ({sum2:.2f} h) debe ser igual al TNPI por exceso ({float(tnpi_exceso_h):.2f} h)."
                        )
                    else:
                        st.session_state.act_tnpi_breakdown = bd2.to_dict(orient="records")
                        st.session_state.act_tnpi_breakdown_saved = True
                        st.success(
                            "Desglose guardado. Al agregar la actividad se registrarán varias causas TNPI."
                        )
    comentario = st.sidebar.text_input("Comentario", "", key="com_general")
    disable_general_add = (actividad in ["Conexión perforando", "Arma/Desarma BHA"]) or (actividad_sel == "Otra (especificar)" and not actividad)
    if st.sidebar.button("Agregar actividad", use_container_width=True, disabled=disable_general_add):
        if actividad_sel == "Otra (especificar)" and not actividad:
            st.warning("Especifica el nombre de la actividad antes de agregarla.")
            st.stop()

        # Memorizar actividad nueva en esta sesión (y se persiste si guardas la jornada)
        if actividad_sel == "Otra (especificar)" and actividad:
            base_lower = {a.strip().lower() for a in ACTIVIDADES}
            custom_lower = {a.strip().lower() for a in st.session_state.get("custom_actividades", [])}
            if actividad.lower() not in base_lower and actividad.lower() not in custom_lower:
                st.session_state.custom_actividades.append(actividad)

        etapa_use = ((etapa_viajes_sel or etapa) if "etapa_viajes_sel" in globals() else etapa)

        # --- Lógica general ---
        # 1) Si capturas como TP y el Real supera el Estándar, se divide en:
        #    - TP = Estándar
        #    - TNPI = exceso
        # 2) Para ese TNPI por exceso: puedes asignarlo a 1 causa (simple) o desglosarlo (detalle opcional).

        add_rows = []

        # Caso: TP con exceso -> split TP + TNPI
        if tipo == "TP" and float(horas_prog) > 0 and float(horas_real) > float(horas_prog):
            exceso_h = max(0.0, float(horas_real) - float(horas_prog))
            tipo_exceso = st.session_state.get("exceso_tipo_general", "TNPI")  # Obtener el tipo seleccionado

            base = {
                "Equipo": equipo,
                "Pozo": pozo,
                "Etapa": etapa_use,
                "Fecha": str(fecha),
                "Equipo_Tipo": st.session_state.get("equipo_tipo_val", ""),
                "Modo_Reporte": modo_reporte,
                "Seccion": etapa,
                "Corrida": corrida,
                "Tipo_Agujero": tipo_agujero,
                "Operacion": operacion,
                "Actividad": actividad,
                "Turno": turno,
                "ROP_Prog_mh": float(rop_prog),
                "ROP_Real_mh": float(rop_real),
                "Comentario": comentario,
                "Origen": "Manual",
            }

            # TP (hasta el estándar)
            add_rows.append({
                        **base,
                        "Tipo": "TP",
                        "Categoria_TNPI": "-",
                        "Detalle_TNPI": "-",
                        "Categoria_TNP": "-",
                        "Detalle_TNP": "-",
                        "Horas_Prog": float(horas_prog),
                        "Horas_Reales": float(horas_prog),
                    })

                        # TNPI/TNP por exceso: desglose guardado o asignación simple
            exceso_tipo = st.session_state.get("exceso_tipo_general", "TNPI")
            exceso_tipo = "TNP" if str(exceso_tipo).upper() == "TNP" else "TNPI"

            bd_saved = pd.DataFrame()
            if exceso_tipo == "TNP":
                # Registrar exceso como TNP (usa los selectores de Categoría/Detalle TNP)
                add_rows.append({
                    **base,
                    "Tipo": "TNP",
                    "Categoria_TNPI": "-",
                    "Detalle_TNPI": "-",
                    "Categoria_TNP": (act_cat_simple_tnp if "act_cat_simple_tnp" in locals() else categoria_tnp),
                    "Detalle_TNP": (act_det_simple_tnp if "act_det_simple_tnp" in locals() else detalle_tnp),
                    "Horas_Prog": 0.0,
                    "Horas_Reales": float(tnpi_exceso_h),
                    "Comentario": f"Exceso (Real {horas_real:.2f}h > Estándar {horas_prog:.2f}h) registrado como TNP.",
                    "Origen": "EXCESO",
                })
            else:
                # Registrar exceso como TNPI. Si hay desglose guardado, lo aplicamos.
                act_tnpi_breakdown = st.session_state.get("act_tnpi_breakdown", None)
                if act_tnpi_breakdown is not None and len(act_tnpi_breakdown) > 0:
                    try:
                        bd_saved = pd.DataFrame(act_tnpi_breakdown)
                        # Normaliza columnas esperadas
                        if "Categoria_TNPI" not in bd_saved.columns and "Categoria" in bd_saved.columns:
                            bd_saved["Categoria_TNPI"] = bd_saved["Categoria"]
                        if "Detalle_TNPI" not in bd_saved.columns and "Detalle" in bd_saved.columns:
                            bd_saved["Detalle_TNPI"] = bd_saved["Detalle"]
                        if "Horas_Reales" not in bd_saved.columns and "Horas" in bd_saved.columns:
                            bd_saved["Horas_Reales"] = bd_saved["Horas"]
                        bd_saved = bd_saved[["Categoria_TNPI", "Detalle_TNPI", "Horas_Reales"]].copy()
                        bd_saved["Horas_Reales"] = pd.to_numeric(bd_saved["Horas_Reales"], errors="coerce").fillna(0.0)
                    except Exception:
                        bd_saved = pd.DataFrame()
            
                if bd_saved.empty:
                    add_rows.append({
                        **base,
                        "Tipo": "TNPI",
                        "Categoria_TNPI": categoria_tnpi,
                        "Detalle_TNPI": detalle_tnpi,
                        "Categoria_TNP": "-",
                        "Detalle_TNP": "-",
                        "Horas_Prog": 0.0,
                        "Horas_Reales": float(tnpi_exceso_h),
                        "Comentario": f"Exceso (Real {horas_real:.2f}h > Estándar {horas_prog:.2f}h) registrado como TNPI.",
                        "Origen": "EXCESO",
                    })
            # Si existe desglose TNPI guardado, lo aplicamos como múltiples filas TNPI
            if (not bd_saved.empty) and (exceso_tipo == "TNPI"):
                sum_bd = float(bd_saved["Horas_Reales"].sum()) if "Horas_Reales" in bd_saved.columns else 0.0
                if sum_bd > 0 and abs(sum_bd - float(tnpi_exceso_h)) > 1e-6:
                    bd_saved["Horas_Reales"] = bd_saved["Horas_Reales"] * (float(tnpi_exceso_h) / sum_bd)
                for _, r in bd_saved.iterrows():
                    add_rows.append({
                        **base,
                        "Tipo": "TNPI",
                        "Categoria_TNPI": str(r.get("Categoria_TNPI", "-")),
                        "Detalle_TNPI": str(r.get("Detalle_TNPI", "-")),
                        "Categoria_TNP": "-",
                        "Detalle_TNP": "-",
                        "Horas_Prog": 0.0,
                        "Horas_Reales": float(r.get("Horas_Reales", 0.0)),
                        "Comentario": f"Exceso (Real {horas_real:.2f}h > Estándar {horas_prog:.2f}h) registrado como TNPI (desglose).",
                        "Origen": "EXCESO",
                    })

        else:
            # Sin split: respeta el tipo seleccionado (TP/TNPI/TNP)
            add_rows.append({
                "Equipo": equipo,
                "Pozo": pozo,
                "Etapa": etapa_use,
                "Fecha": str(fecha),
                "Equipo_Tipo": st.session_state.get("equipo_tipo_val", ""),
                "Modo_Reporte": modo_reporte,
                "Seccion": etapa,
                "Corrida": corrida,
                "Tipo_Agujero": tipo_agujero,
                "Operacion": operacion,
                "Actividad": actividad,
                "Turno": turno,
                "Tipo": tipo,
                "Categoria_TNPI": categoria_tnpi if tipo == "TNPI" else "-",
                "Detalle_TNPI": detalle_tnpi if tipo == "TNPI" else "-",
                "Categoria_TNP": categoria_tnp if tipo == "TNP" else "-",
                "Detalle_TNP": detalle_tnp if tipo == "TNP" else "-",
                "Horas_Prog": float(horas_prog),
                "Horas_Reales": float(horas_real),
                "ROP_Prog_mh": float(rop_prog),
                "ROP_Real_mh": float(rop_real),
                "Comentario": comentario,
                "Origen": "Manual",
            })

        st.session_state.df = pd.concat([st.session_state.df, pd.DataFrame(add_rows)], ignore_index=True)
        
    st.session_state.df = _ensure_rowid(st.session_state.df)
    st.session_state.df = _normalize_time_cause_columns(st.session_state.df)


# Eliminar columnas duplicadas (puede ocurrir por compatibilidad / merges)
if hasattr(st.session_state, "df") and isinstance(st.session_state.df, pd.DataFrame):
    if st.session_state.df.columns.duplicated().any():
        st.session_state.df = _coalesce_duplicate_columns(st.session_state.df)

st.sidebar.success("Actividad agregada")


# =====================================================================
# CAPTURA ESPECIAL: CONEXIÓN PERFORANDO (MEJORADO - CON ETAPA ESPECÍFICA)
# =====================================================================
if modo_reporte == "Perforación" and actividad == "Conexión perforando":
    with st.sidebar.expander("Conexión perforando (captura)", expanded=True):
        # Asegurar que se use la etapa seleccionada en el sidebar principal
        etapa_conn = st.selectbox(
            "Etapa para conexión", 
            options=SECCIONES_DEFAULT,
            index=SECCIONES_DEFAULT.index(etapa) if etapa in SECCIONES_DEFAULT else 0,
            key="etapa_conn"
        )
        corrida_c = st.text_input("Corrida (Run) – conexiones", "Run 1", key="run_conn")
        tipo_agujero_c = st.radio("Tipo de agujero – conexiones", TIPO_AGUJERO, horizontal=True, key="hole_conn")
        turno_c = st.radio("Turno – conexiones", TURNOS, horizontal=True, key="turno_conn")
        profundidad_m = st.number_input("Profundidad (m)", 0.0, step=1.0, key="prof_conn")
        
        conn_tipo = st.selectbox("Tipo de conexión", CONN_TYPE_OPTS, key="conn_tipo")
        ang_bucket = st.selectbox("Rango de ángulo", ANGLE_BUCKETS, key="ang_bucket")
        
        st.markdown("**Componentes (min reales)**")
        mins_real = {}
        for comp in CONN_COMPONENTS:
            mins_real[comp] = st.number_input(comp, min_value=0.0, step=0.1, value=0.0, key=f"min_{comp}")
        
        tipo_tiempo_conn = st.radio("Tipo de tiempo (Conexión)", options=["TP", "TNP"], horizontal=True, key="tipo_tiempo_conn")

        # Catálogo TNP (mismo archivo que TNPI)
        cat_list_tnp = sorted([c for c in df_tnpi_cat.get("Categoria_TNP", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if c.strip() != ""])
        if not cat_list_tnp:
            cat_list_tnp = ["-"]

        if tipo_tiempo_conn == "TP":
            st.markdown("**Exceso (Real > Estándar)**")
            exceso_policy_conn = st.radio(
                "¿Cómo registrar el exceso?",
                options=["TNPI", "TNP"],
                horizontal=True,
                key="conn_exceso_policy",
                help="Si Real supera el estándar, el exceso puede registrarse como TNPI (no productivo/improductivo) o como TNP (no planeado).",
            )

            if exceso_policy_conn == "TNPI":
                st.markdown("**Causa TNPI (solo para el exceso)**")
                cat_tnpi_conn = st.selectbox("Categoría TNPI (exceso)", options=cat_list, key="conn_cat_tnpi")
                det_all = df_tnpi_cat[df_tnpi_cat["Categoria_TNPI"] == cat_tnpi_conn]["Detalle_TNPI"].tolist()
                q2 = (st.text_input("Buscar detalle TNPI (exceso)", value="", key="q_conn_tnpi") or "").strip().lower()
                det_filtered = [d for d in det_all if q2 in str(d).lower()] if q2 else det_all
                det_tnpi_conn = st.selectbox(
                    "Detalle TNPI (exceso)",
                    options=det_filtered if det_filtered else det_all,
                    key="det_conn_tnpi",
                )
                cat_tnp_conn, det_tnp_conn = "-", "-"
            else:
                st.markdown("**Causa TNP (solo para el exceso)**")
                cat_tnp_conn = st.selectbox("Categoría TNP (exceso)", options=cat_list_tnp, key="conn_cat_tnp")
                det_all = df_tnpi_cat[df_tnpi_cat.get("Categoria_TNP", "") == cat_tnp_conn].get("Detalle_TNP", pd.Series(dtype=str)).tolist()
                q2 = (st.text_input("Buscar detalle TNP (exceso)", value="", key="q_conn_tnp") or "").strip().lower()
                det_filtered = [d for d in det_all if q2 in str(d).lower()] if q2 else det_all
                det_tnp_conn = st.selectbox(
                    "Detalle TNP (exceso)",
                    options=det_filtered if det_filtered else det_all if det_all else ["-"],
                    key="det_conn_tnp",
                )
                cat_tnpi_conn, det_tnpi_conn = "-", "-"
        else:
            # Toda la conexión se registra como TNP (no hay TNPI automático aquí)
            exceso_policy_conn = "TNP"
            st.markdown("**Causa TNP (toda la conexión)**")
            cat_tnp_conn = st.selectbox("Categoría TNP", options=cat_list_tnp, key="conn_cat_tnp_full")
            det_all = df_tnpi_cat[df_tnpi_cat.get("Categoria_TNP", "") == cat_tnp_conn].get("Detalle_TNP", pd.Series(dtype=str)).tolist()
            q2 = (st.text_input("Buscar detalle TNP", value="", key="q_conn_tnp_full") or "").strip().lower()
            det_filtered = [d for d in det_all if q2 in str(d).lower()] if q2 else det_all
            det_tnp_conn = st.selectbox(
                "Detalle TNP",
                options=det_filtered if det_filtered else det_all if det_all else ["-"],
                key="det_conn_tnp_full",
            )
            cat_tnpi_conn, det_tnpi_conn = "-", "-"

        conn_comment = st.text_input("Comentario conexión", "", key="conn_comment")

        if st.button("Agregar conexión", use_container_width=True):
            conn_no = int(st.session_state.df_conn["Conn_No"].max()) + 1 if not st.session_state.df_conn.empty else 1

            
            std_map = CONN_STDS.get((conn_tipo, ang_bucket), {})
            std_pre = float(std_map.get("Preconexión", 0))
            std_conn = float(std_map.get("Conexión", 0))
            std_post = float(std_map.get("Postconexión", 0))
            
            rows = []
            for comp in CONN_COMPONENTS:
                real = float(mins_real.get(comp, 0.0))
                if comp == "Preconexión":
                    std_use = std_pre
                elif comp == "Conexión":
                    std_use = std_conn
                elif comp == "Postconexión":
                    std_use = std_post
                else:
                    std_use = 0.0
                
                tnpi_exceso = 0.0
                minutos_tnp = 0.0
                if tipo_tiempo_conn == "TP":
                    exceso = max(0.0, float(real) - float(std_use))
                    if exceso_policy_conn == "TNP":
                        # El exceso se registra como TNP (no TNPI)
                        tnpi_exceso = 0.0
                        minutos_tnp = float(exceso)
                        cat_tnpi_use, det_tnpi_use = "-", "-"
                        cat_tnp_use, det_tnp_use = cat_tnp_conn, det_tnp_conn
                    else:
                        # El exceso se registra como TNPI
                        tnpi_exceso = float(exceso)
                        minutos_tnp = 0.0
                        cat_tnpi_use, det_tnpi_use = cat_tnpi_conn, det_tnpi_conn
                        cat_tnp_use, det_tnp_use = "-", "-"
                else:
                    # Toda la conexión como TNP
                    tnpi_exceso = 0.0
                    minutos_tnp = float(real)
                    cat_tnpi_use, det_tnpi_use = "-", "-"
                    cat_tnp_use, det_tnp_use = cat_tnp_conn, det_tnp_conn
                rows.append(
                    {
                        "Equipo": equipo,
                        "Pozo": pozo,
                        "Etapa": etapa_conn,  # Usar la etapa específica para conexiones
                        "Fecha": str(fecha),
                        "Equipo_Tipo": st.session_state.get("equipo_tipo_val", ""),
                        "Seccion": etapa_conn,  # También en Seccion
                        "Corrida": corrida_c,
                        "Tipo_Agujero": tipo_agujero_c,
                        "Turno": turno_c,
                        "Conn_No": conn_no,
                        "Profundidad_m": float(profundidad_m),
                        "Conn_Tipo": conn_tipo,
                        "Angulo_Bucket": ang_bucket,
                        "Componente": comp,
                        "Minutos_Reales": real,
                        "Minutos_Estandar": float(std_use),
                        "Minutos_TNPI": float(tnpi_exceso),
                        "Minutos_TNP": float(minutos_tnp),
                        "Categoria_TNPI": (cat_tnpi_use if float(tnpi_exceso) > 0 else "-"),
                        "Detalle_TNPI": (det_tnpi_use if float(tnpi_exceso) > 0 else "-"),
                        "Categoria_TNP": (cat_tnp_use if float(minutos_tnp) > 0 else "-"),
                        "Detalle_TNP": (det_tnp_use if float(minutos_tnp) > 0 else "-"),
                        "Comentario": conn_comment,
                    }
                )
            
            df_new = pd.DataFrame(rows)
            st.session_state.df_conn = pd.concat([st.session_state.df_conn, df_new], ignore_index=True)
            st.session_state["_toast_conn"] = True
            

            total_real_min = float(df_new["Minutos_Reales"].sum())
            std_total_line = float(std_map.get("TOTAL", std_pre + std_conn + std_post))
            exceso_total_min = max(0.0, total_real_min - std_total_line)

            # Parte base (hasta el estándar) siempre conserva el tipo seleccionado (TP o TNP).
            base_min = min(total_real_min, std_total_line)

            # ¿Cómo registrar el exceso?
            # - Si el usuario eligió registrar el exceso como TNPI -> lo mandamos a Minutos_TNPI
            # - Si eligió TNP -> lo mandamos a Minutos_TNP y guardamos categoría/detalle TNP
            if tipo_tiempo_conn == "TP":
                if exceso_policy_conn == "TNPI":
                    tnpi_min = exceso_total_min
                    tnp_min = 0.0
                else:  # "TNP"
                    tnpi_min = 0.0
                    tnp_min = exceso_total_min
            else:
                # Si la conexión completa se está registrando como TNP, no hay desglose de exceso.
                tnpi_min = 0.0
                tnp_min = float(total_real_min)

            base = dict(
                Equipo=equipo,
                Pozo=pozo,
                Etapa=etapa_conn,
                Fecha=str(fecha),
                Equipo_Tipo=st.session_state.get("equipo_tipo_val", ""),
                Modo_Reporte="Perforación",
                Seccion=etapa_conn,
                Corrida=corrida_c,
                Tipo_Agujero=tipo_agujero_c,
                Operacion="Perforación",
                Actividad=f"Conexión perforando ({ang_bucket})",
                Turno=turno,
                Tipo=tipo_tiempo_conn,
                Categoria_TNPI="-",
                Detalle_TNPI="-",
                Categoria_TNP=(cat_tnp_conn if tipo_tiempo_conn == "TNP" else "-"),
                Detalle_TNP=(det_tnp_conn if tipo_tiempo_conn == "TNP" else "-"),
                Horas_Prog=float(std_total_line / 60.0) if std_total_line else 0.0,
                Horas_Reales=float(base_min / 60.0),
                ROP_Prog_mh=0.0,
                ROP_Real_mh=0.0,
                Comentario=st.session_state.get("comentario_conn", "") or "",
                Origen="Manual",
                Eficiencia_pct=float(_calc_eff(std_total_line / 60.0, base_min / 60.0)),
                Semáforo=_semaforo_text(float(_calc_eff(std_total_line / 60.0, base_min / 60.0))),
            )

            add_rows = [base]

            # Fila de exceso (solo si la base era TP y hubo exceso)
            if tipo_tiempo_conn == "TP" and exceso_total_min > 0:
                if exceso_policy_conn == "TNPI" and tnpi_min > 0:
                    add_rows.append({**base,
                        "Tipo": "TNPI",
                        "Categoria_TNPI": (cat_tnpi_use or "-"),
                        "Detalle_TNPI": (det_tnpi_use or "-"),
                        "Categoria_TNP": "-",
                        "Detalle_TNP": "-",
                        "Horas_Prog": 0.0,
                        "Horas_Reales": float(tnpi_min / 60.0),
                        "Eficiencia_pct": 0.0,
                        "Semáforo": _semaforo_text(0.0),
                    })
                if exceso_policy_conn == "TNP" and tnp_min > 0:
                    add_rows.append({**base,
                        "Tipo": "TNP",
                        "Categoria_TNPI": "-",
                        "Detalle_TNPI": "-",
                        "Categoria_TNP": (cat_tnp_conn or "-"),
                        "Detalle_TNP": (det_tnp_conn or "-"),
                        "Horas_Prog": 0.0,
                        "Horas_Reales": float(tnp_min / 60.0),
                        "Eficiencia_pct": 0.0,
                        "Semáforo": _semaforo_text(0.0),
                    })
            st.session_state.df = pd.concat([st.session_state.df, pd.DataFrame(add_rows)], ignore_index=True)
            
        st.session_state.df = _ensure_rowid(st.session_state.df)

# =====================================================================
# CAPTURA ESPECIAL: ARMA/DESARMA BHA
# =====================================================================
if actividad == "Arma/Desarma BHA":
    with st.sidebar.expander("Arma/Desarma BHA (captura)", expanded=True):
        bha_tipo_tiempo = st.session_state.get("tipo_time_general", "TP")
        st.session_state["bha_tipo_tiempo"] = bha_tipo_tiempo

        bha_turno = st.radio("Turno (BHA)", TURNOS, horizontal=True, key="bha_turno")
        barrena = st.text_input("Barrena (BNA)", "", key="bha_barrena")
        bha_tipo = st.selectbox("Tipo (1–10)", options=list(BHA_TYPES.keys()), index=0, key="bha_tipo")

        desc, std_arma, std_desarma = BHA_TYPES[int(bha_tipo)]
        accion = st.radio("Acción", ["Arma", "Desarma"], horizontal=True, key="bha_accion")

        std_default = float(std_arma if accion == "Arma" else std_desarma)
        override = st.checkbox("Editar estándar manualmente", value=False, key="bha_override")
        if override:
            estandar_h = st.number_input("Estándar (h)", min_value=0.0, step=0.25, value=float(std_default), key="bha_std_manual")
        else:
            estandar_h = float(std_default)
            st.caption(f"Estándar automático: **{estandar_h:.2f} h**")

        label_real_bha = "Real (h)"
        if st.session_state.get("bha_tipo_tiempo", "TP") == "TNP":
            label_real_bha = "TNP (h) — tiempo real"
        elif st.session_state.get("bha_tipo_tiempo", "TP") == "TNPI":
            label_real_bha = "TNPI (h) — tiempo real"
        elif st.session_state.get("bha_tipo_tiempo", "TP") == "TP":
            label_real_bha = "TP (h) — tiempo real"

        real_h = st.number_input(label_real_bha, min_value=0.0, step=0.25, value=0.0, key="bha_real_h")

        tnpi_h = max(0.0, float(real_h) - float(estandar_h))
        tnp_h = 0.0
        if (st.session_state.get("tipo_time_bha") or st.session_state.get("tipo_time_general") or "TP") == "TNP":
            tnp_h = float(real_h)
            tnpi_h = 0.0

            tnp_h = float(real_h)
            tnpi_h = 0.0
        tp_h_local = max(0.0, float(real_h) - float(tnpi_h))
        eff_bha = clamp_0_100(safe_pct(tp_h_local, float(real_h))) if real_h > 0 else 0.0

        st.caption(f"Componentes: **{desc}**")
        st.caption(f"TNPI por exceso: **{tnpi_h:.2f} h** | Eficiencia: **{eff_bha:.0f}%**")

        bha_cat = "-"
        bha_det = "-"

        # --- NUEVO: Seleccionar tipo de exceso para BHA ---

        if tnpi_h > 0 and bha_tipo_tiempo == "TP":

            st.markdown("**Exceso (BHA)**")

            

            bha_exceso_tipo = st.radio(

                "¿Cómo registrar el exceso?",

                options=["TNPI", "TNP"],

                horizontal=True,

                key="bha_exceso_tipo",

                help="El exceso de horas puede registrarse como TNPI (no productivo/improductivo) o como TNP (no planeado)."

            )

            

            # Actualizar variable para uso posterior
        # session_state already managed by widget key
            # Mostrar configuración según el tipo seleccionado

            if bha_exceso_tipo == "TNPI":

                st.markdown("**Asignación simple (una sola causa)**")

                bha_cat = st.selectbox(

                    "Categoría TNPI (BHA)",

                    options=cat_list if "cat_list" in globals() else ["-"],

                    index=0,

                    key="bha_cat_simple_1",

                )



                if "df_tnpi_cat" in globals() and "Categoria_TNPI" in df_tnpi_cat.columns and "Detalle_TNPI" in df_tnpi_cat.columns:

                    _det_opts = (

                        df_tnpi_cat[df_tnpi_cat["Categoria_TNPI"] == bha_cat]["Detalle_TNPI"]

                        .dropna()

                        .unique()

                        .tolist()

                    )

                else:

                    _det_opts = ["-"]



                bha_det = st.selectbox(

                    "Detalle TNPI (BHA)",

                    options=_det_opts if len(_det_opts) else ["-"],

                    index=0,

                    key="bha_det_simple_1",

                )

            else:  # TNP

                # Configuración para TNP

                st.markdown("**Asignación simple para TNP (una sola causa)**")

                bha_cat = st.selectbox(

                    "Categoría TNP (BHA)",

                    options=tnp_cat_list if "tnp_cat_list" in globals() else ["-"],

                    index=0,

                    key="bha_cat_simple_2",

                )

                

                if "df_tnp_cat" in globals() and "Categoria_TNP" in df_tnp_cat.columns and "Detalle_TNP" in df_tnp_cat.columns:

                    _det_opts = (

                        df_tnp_cat[df_tnp_cat["Categoria_TNP"] == bha_cat]["Detalle_TNP"]

                        .dropna()

                        .unique()

                        .tolist()

                    )

                else:

                    _det_opts = ["-"]

                

                bha_det = st.selectbox(

                    "Detalle TNP (BHA)",

                    options=_det_opts if len(_det_opts) else ["-"],

                    index=0,

                    key="bha_det_simple_2",

                )

            if "df_tnpi_cat" in globals() and "Categoria_TNPI" in df_tnpi_cat.columns and "Detalle_TNPI" in df_tnpi_cat.columns:
                _det_opts = (
                    df_tnpi_cat[df_tnpi_cat["Categoria_TNPI"] == bha_cat]["Detalle_TNPI"]
                    .dropna()
                    .unique()
                    .tolist()
                )
            else:
                _det_opts = ["-"]

            bha_det = st.selectbox(
                "Detalle TNPI (BHA)",
                options=_det_opts if len(_det_opts) else ["-"],
                index=0,
                key="bha_det_simple_3",
            )

            # --- Desglose opcional (múltiples causas) ---
            with st.expander("Detalle TNPI (opcional) — desglose por múltiples causas", expanded=False):
                st.caption("Agrega varias causas y presiona **Guardar cambios**. La suma debe ser igual al TNPI calculado.")

                # Inicializar borrador si está vacío y no hay nada guardado
                if (not st.session_state.get("bha_tnpi_breakdown_draft")) and (not st.session_state.get("bha_tnpi_breakdown")):
                    st.session_state.bha_tnpi_breakdown_draft = [{
                        "Categoria_TNPI": (cat_list[0] if "cat_list" in globals() and isinstance(cat_list, list) and len(cat_list) else "-"),
                        "Detalle_TNPI": "-",
                        "Horas_TNPI_h": float(tnpi_h),
                        "Comentario": "",
                    }]

                _cat_new = st.selectbox(
                    "Categoría TNPI (nuevo renglón)",
                    options=cat_list if "cat_list" in globals() else ["-"],
                    index=0,
                    key="bha_bd_cat_new",
                )

                if "df_tnpi_cat" in globals() and "Categoria_TNPI" in df_tnpi_cat.columns and "Detalle_TNPI" in df_tnpi_cat.columns:
                    _det_new_opts = (
                        df_tnpi_cat[df_tnpi_cat["Categoria_TNPI"] == _cat_new]["Detalle_TNPI"]
                        .dropna()
                        .unique()
                        .tolist()
                    )
                else:
                    _det_new_opts = ["-"]

                _det_new = st.selectbox(
                    "Detalle TNPI (nuevo renglón)",
                    options=_det_new_opts if len(_det_new_opts) else ["-"],
                    index=0,
                    key="bha_bd_det_new",
                )

                _h_new = st.number_input(
                    "Horas TNPI (h) (nuevo renglón)",
                    min_value=0.0,
                    value=0.0,
                    step=0.25,
                    format="%.2f",
                    key="bha_bd_h_new",
                )

                _com_new = st.text_input("Comentario (nuevo renglón)", "", key="bha_bd_com_new")

                if st.button("Agregar", use_container_width=True, key="bha_bd_add"):
                        if _h_new <= 0:
                            st.warning("Las horas deben ser > 0 para agregar la causa.")
                        else:
                            st.session_state.bha_tnpi_breakdown_draft.append({
                                "Categoria_TNPI": _cat_new,
                                "Detalle_TNPI": _det_new,
                                "Horas_TNPI_h": float(_h_new),
                                "Comentario": _com_new,
                            })
                            st.rerun()

                if st.button("Limpiar", use_container_width=True, key="bha_bd_clear"):
                    st.session_state.bha_tnpi_breakdown_draft = []
                    st.session_state.bha_tnpi_breakdown_saved = False
                    st.rerun()

                bd = pd.DataFrame(
                    st.session_state.get("bha_tnpi_breakdown_draft", []),
                    columns=["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP", "Horas_TNPI_h", "Comentario"]
                )
                bd["Horas_TNPI_h"] = pd.to_numeric(bd["Horas_TNPI_h"], errors="coerce").fillna(0.0)

                st.markdown("**Causas en borrador**")
                if bd.empty:
                    st.info("Aún no hay causas en el borrador.")
                else:
                    for i, r in bd.reset_index(drop=True).iterrows():
                        cols = st.columns([2, 3, 1, 3, 1])
                        cols[0].write(str(r.get("Categoria_TNPI", "-")))
                        cols[1].write(str(r.get("Detalle_TNPI", "-")))
                        cols[2].write(f'{float(r.get("Horas_TNPI_h", 0.0)):.2f}')
                        cols[3].write(str(r.get("Comentario", "")))
                        if cols[4].button("Eliminar", key=f"bha_bd_del_{i}"):
                            st.session_state.bha_tnpi_breakdown_draft.pop(i)
                            st.rerun()

                sum_bd = float(bd["Horas_TNPI_h"].sum()) if not bd.empty else 0.0
                st.caption(f"Suma borrador: **{sum_bd:.2f} h**  |  TNPI calculado: **{float(tnpi_h):.2f} h**")

                if st.button("Guardar", use_container_width=True, key="bha_bd_save"):
                    if bd.empty:
                        st.error("No hay renglones en el borrador para guardar.")
                    else:
                        bd2 = bd[bd["Horas_TNPI_h"] > 0].copy()
                        sum2 = float(bd2["Horas_TNPI_h"].sum())
                        if abs(sum2 - float(tnpi_h)) > 1e-6:
                            st.error(f"La suma del desglose ({sum2:.2f} h) debe ser igual al TNPI calculado ({float(tnpi_h):.2f} h).")
                        else:
                            st.session_state.bha_tnpi_breakdown = bd2.to_dict(orient="records")
                            st.session_state.bha_tnpi_breakdown_saved = True
                            st.success("Desglose guardado. Al agregar el BHA se registrarán varias causas TNPI.")

        bha_comment = st.text_input("Comentario BHA", "", key="bha_comment")

        if st.button("Agregar BHA", use_container_width=True):
            row_bha = {
                "Equipo": equipo,
                "Pozo": pozo,
                "Etapa": ((etapa_viajes_sel or etapa) if "etapa_viajes_sel" in globals() else etapa),
                "Fecha": str(fecha),
                "Turno": bha_turno,
                "Barrena": barrena,
                "BHA_Tipo": int(bha_tipo),
                "BHA_Componentes": desc,
                "Accion": accion,
                "Estandar_h": float(estandar_h),
                "Real_h": float(real_h),
                "TNPI_h": float(tnpi_h),
                "TNP_h": float(tnp_h),
                "Eficiencia_pct": float(_calc_eff(estandar_h, real_h)),
            }
            st.session_state.df_bha = pd.concat([st.session_state.df_bha, pd.DataFrame([row_bha])], ignore_index=True)

            base = dict(
                Equipo=equipo,
                Pozo=pozo,
                Etapa=etapa,
                Fecha=str(fecha),
                Equipo_Tipo=st.session_state.get("equipo_tipo_val", ""),
                Modo_Reporte=modo_reporte,
                Seccion=etapa,
                Corrida=corrida,
                Tipo_Agujero=tipo_agujero,
                Operacion=operacion,
                Actividad=f"Arma/Desarma BHA (Tipo {int(bha_tipo)})",
                Turno=bha_turno,
                ROP_Prog_mh=0.0,
                ROP_Real_mh=0.0,
                Comentario=bha_comment.strip(),
                Origen="BHA",
            )

            add = [
                {
                    **base,
                    "Tipo": "TP",
                    "Categoria_TNPI": "-",
                    "Detalle_TNPI": "-",
                    "Horas_Prog": float(estandar_h),
                    "Horas_Reales": float(max(0.0, float(real_h) - float(tnpi_h))),
                }
            ]
            if tnpi_h > 0 and bha_tipo_tiempo == "TP":
                # Si hay desglose guardado, úsalo. Si no, usa asignación simple (una sola causa).
                bd_saved = pd.DataFrame(
                    st.session_state.get("bha_tnpi_breakdown", []),
                    columns=["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP", "Horas_TNPI_h", "Comentario"]
                )
                if (not bd_saved.empty) and bool(st.session_state.get("bha_tnpi_breakdown_saved", False)):
                    bd_saved["Horas_TNPI_h"] = pd.to_numeric(bd_saved["Horas_TNPI_h"], errors="coerce").fillna(0.0)
                    bd_saved = bd_saved[bd_saved["Horas_TNPI_h"] > 0].copy()

                    sum_bd = float(bd_saved["Horas_TNPI_h"].sum())
                    if abs(sum_bd - float(tnpi_h)) > 1e-6:
                        st.error(f"La suma del desglose TNPI guardado ({sum_bd:.2f} h) debe ser igual al TNPI calculado ({float(tnpi_h):.2f} h).")
                        st.stop()

                    for _, r in bd_saved.iterrows():
                        add.append(
                            {
                                **base,
                                "Tipo": "TNPI",
                                "Categoria_TNPI": str(r.get("Categoria_TNPI", "-") or "-"),
                                "Detalle_TNPI": str(r.get("Detalle_TNPI", "-") or "-"),
                                "Horas_Prog": 0.0,
                                "Horas_Reales": float(r.get("Horas_TNPI_h", 0.0) or 0.0),
                                "Comentario": (base.get("Comentario","") + (f" | {r.get('Comentario')}" if r.get("Comentario") else "")).strip(" |"),
                            }
                        )
                else:
                    # Asignación simple (una sola causa)
                    add.append(
                        {
                            **base,
                            "Tipo": "TNPI",
                            "Categoria_TNPI": bha_cat,
                            "Detalle_TNPI": bha_det,
                            "Horas_Prog": 0.0,
                            "Horas_Reales": float(tnpi_h),
                        }
                    )

                # Limpia desglose para el siguiente BHA
                st.session_state.bha_tnpi_breakdown = []
                st.session_state.bha_tnpi_breakdown_draft = []
                st.session_state.bha_tnpi_breakdown_saved = False

            st.session_state.df = pd.concat([st.session_state.df, pd.DataFrame(add)], ignore_index=True)
            st.success("BHA agregado")

# =====================================================================
# MAIN DATA
# =====================================================================
df = st.session_state.df.copy()
df_conn = st.session_state.df_conn.copy()
df_bha = st.session_state.df_bha.copy()

# =====================================================================
# BHA: GRAFICA ESTÁNDAR VS REAL (cuando estás capturando Arma/Desarma)
# =====================================================================
# Nota: se muestra solo cuando en el sidebar eliges la actividad "Arma/Desarma BHA"


# =====================================================================
# KPIs base
# =====================================================================
total_prog = float(df["Horas_Prog"].sum()) if not df.empty else 0.0
total_real = float(df["Horas_Reales"].sum()) if not df.empty else 0.0
tp_h = float(df[df["Tipo"] == "TP"]["Horas_Reales"].sum()) if not df.empty else 0.0
tnpi_h = float(df[df["Tipo"] == "TNPI"]["Horas_Reales"].sum()) if not df.empty else 0.0
tnp_h = float(df[df["Tipo"] == "TNP"]["Horas_Reales"].sum()) if not df.empty else 0.0
eficiencia_dia = clamp_0_100(safe_pct(tp_h, total_real)) if total_real > 0 else 0.0

# =====================================================================
# METROS / ROP (IMPORTANTE: define variables SIEMPRE)
# =====================================================================
mr_total = 0.0
tnpi_m_h = 0.0
eff_m = 0.0
rr = 0.0
eff_rop = 0.0

if modo_reporte == "Perforación":
    mp = float(st.session_state.drill_day.get("metros_prog_total", 0.0))
    mr_d = float(st.session_state.drill_day.get("metros_real_dia", 0.0))
    mr_n = float(st.session_state.drill_day.get("metros_real_noche", 0.0))
    mr_total = mr_d + mr_n

    tnpi_m_h = float(st.session_state.drill_day.get("tnpi_metros_h", 0.0))
    eff_m = clamp_0_100(safe_pct(mr_total, mp)) if mp > 0 else 0.0

    rp = float(st.session_state.drill_day.get("rop_prog_total", 0.0))
    rr_d = float(st.session_state.drill_day.get("rop_real_dia", 0.0))
    rr_n = float(st.session_state.drill_day.get("rop_real_noche", 0.0))
    rr = (rr_d + rr_n) / (2 if (rr_d > 0 and rr_n > 0) else 1) if (rr_d > 0 or rr_n > 0) else 0.0
    eff_rop = clamp_0_100(safe_pct(rr, rp)) if rp > 0 else 0.0

# =====================================================================
# KPI CONEXIONES (IMPORTANTE: define variables SIEMPRE)
# =====================================================================
conn_real_min = 0.0
conn_std_min = 0.0
conn_tnpi_min = 0.0
eff_conn = 0.0

if modo_reporte == "Perforación" and not df_conn.empty:
    conn_real_min = float(df_conn.groupby(["Conn_No"])["Minutos_Reales"].sum().sum())
    per_conn2 = df_conn.groupby("Conn_No", as_index=False).first()[["Conn_No", "Conn_Tipo", "Angulo_Bucket"]]
    per_conn2["Std_Total"] = per_conn2.apply(
        lambda r: float(CONN_STDS.get((r["Conn_Tipo"], r["Angulo_Bucket"]), {}).get("TOTAL", 0.0)),
        axis=1
    )
    conn_std_min = float(per_conn2["Std_Total"].sum())

    conn_tp_min = min(conn_real_min, conn_std_min) if conn_std_min > 0 else conn_real_min
    conn_tnpi_min = max(0.0, conn_real_min - conn_std_min) if conn_std_min > 0 else 0.0
    eff_conn = clamp_0_100(safe_pct(conn_tp_min, conn_real_min)) if conn_real_min > 0 else 0.0


# =====================================================================
# DrillSpot KPI Export (XLSX) -> Viajes & Conexiones (por hora)
# =====================================================================
def _clean_drillspot_kpi_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Espera el formato típico del export 'KPI Report' de DrillSpot:
    columnas: Start Time, End Time, Start Bit Depth, End Bit Depth, KPI, Duration, ...
    Nota: la primera fila suele traer unidades ('date','m','name','min', etc). Se elimina.
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    df = df_raw.copy()

    # Normaliza nombres (por si vienen con espacios raros)
    df.columns = [str(c).strip() for c in df.columns]

    # Quita primera fila de unidades si aplica
    if "Start Time" in df.columns:
        first = str(df.iloc[0]["Start Time"]).strip().lower()
        if first in {"date", "datetime"}:
            df = df.iloc[1:].reset_index(drop=True)

    # Tipos
    for c in ["Start Time", "End Time"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    for c in ["Start Bit Depth", "End Bit Depth", "Duration"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "KPI" in df.columns:
        df["KPI"] = df["KPI"].astype(str)

    df = df.dropna(subset=["Start Time", "End Time", "KPI", "Duration"]).reset_index(drop=True)
    return df


def load_drillspot_kpi_xlsx(uploaded_file) -> pd.DataFrame:
    """Lee el XLSX exportado por DrillSpot y devuelve un DataFrame limpio."""
    if uploaded_file is None:
        return pd.DataFrame()
    try:
        xls = pd.ExcelFile(uploaded_file)
        sheet = "KPI Report" if "KPI Report" in xls.sheet_names else xls.sheet_names[0]
        df_raw = xls.parse(sheet)
        return _clean_drillspot_kpi_df(df_raw)
    except Exception:
        return pd.DataFrame()


def compute_viaje_conn_hourly_from_kpi(
    df_kpi: pd.DataFrame,
    direction: str,
) -> tuple[pd.DataFrame, dict]:
    """
    direction: 'Trip In' o 'Trip Out'
    Retorna:
      - hourly_df con columnas: hour (0-23), speed_mh (real), conn_min (real)
      - meta dict: distance_m_total, running_minutes_total, conn_events_total
    """
    if df_kpi is None or df_kpi.empty:
        hourly = pd.DataFrame({"hour": list(range(24)), "speed_mh": [0.0]*24, "conn_min": [0.0]*24})
        return hourly, {"distance_m_total": 0.0, "running_minutes_total": 0.0, "conn_events_total": 0}

    df = df_kpi.copy()

    # Filtra KPIs
    run_key = f"{direction}: Running"
    conn_key = f"{direction}: Connection"

    df_run = df[df["KPI"].str.contains(run_key, case=False, na=False)].copy()
    df_conn = df[df["KPI"].str.contains(conn_key, case=False, na=False)].copy()

    # Running -> velocidad (m/h) por hora (ponderado por tiempo)
    if not df_run.empty:
        df_run["hour"] = df_run["Start Time"].dt.hour.astype(int)
        df_run["dist_m"] = (df_run["End Bit Depth"] - df_run["Start Bit Depth"]).abs()
        df_run["dur_h"] = (df_run["Duration"] / 60.0).replace(0, np.nan)
        df_run["speed_mh"] = (df_run["dist_m"] / df_run["dur_h"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)

        g = df_run.groupby("hour", as_index=False).agg(
            dist_m=("dist_m", "sum"),
            dur_h=("dur_h", "sum"),
        )
        g["speed_mh"] = g.apply(lambda r: (r["dist_m"] / r["dur_h"]) if r["dur_h"] and r["dur_h"] > 0 else 0.0, axis=1)
        speed_map = dict(zip(g["hour"].astype(int), g["speed_mh"].astype(float)))
        dist_total = float(df_run["dist_m"].sum())
        run_min_total = float(df_run["Duration"].sum())
    else:
        speed_map = {}
        dist_total = 0.0
        run_min_total = 0.0

    # Connection -> minutos promedio por hora (promedio por evento)
    if not df_conn.empty:
        df_conn["hour"] = df_conn["Start Time"].dt.hour.astype(int)
        g2 = df_conn.groupby("hour", as_index=False).agg(
            conn_min=("Duration", "mean"),
            n=("Duration", "count"),
        )
        conn_map = dict(zip(g2["hour"].astype(int), g2["conn_min"].astype(float)))
        conn_events_total = int(df_conn.shape[0])
    else:
        conn_map = {}
        conn_events_total = 0

    rows = []
    for h in range(24):
        rows.append(
            {
                "hour": h,
                "speed_mh": float(speed_map.get(h, 0.0)),
                "conn_min": float(conn_map.get(h, 0.0)),
            }
        )
    hourly = pd.DataFrame(rows)
    return hourly, {
        "distance_m_total": dist_total,
        "running_minutes_total": run_min_total,
        "conn_events_total": conn_events_total,
    }


def default_trip_direction_from_activity(activity_name: str) -> str:
    """Heurística simple para mapear tu 'Viaje ...' a Trip In / Trip Out del export de KPIs."""
    a = (activity_name or "").lower()
    if any(k in a for k in ["metiendo", "bajando", "entrando"]):
        return "Trip In"
    if any(k in a for k in ["sacando", "levantando", "subiendo", "saliendo"]):
        return "Trip Out"
    # fallback
    return "Trip In"

# =====================================================================
# CACHE: generar figuras (reduce lentitud)
# =====================================================================
@st.cache_data(show_spinner=False)
def _make_figs(df_json: str, df_conn_json: str, modo_reporte: str):
    df_local = pd.read_json(df_json, orient="split") if df_json else pd.DataFrame()
    dfc_local = pd.read_json(df_conn_json, orient="split") if df_conn_json else pd.DataFrame()

    figs = {"tiempos": None, "act_pie": None, "act_bar": None, "conn_pie": None, "conn_stack": None}

    # tiempos
    if not df_local.empty and {"Tipo", "Horas_Reales"}.issubset(df_local.columns):
        figs["tiempos"] = px.pie(df_local, names="Tipo", values="Horas_Reales", hole=0.55, title="TP vs TNPI vs TNP")

    # actividades
    if not df_local.empty and {"Actividad", "Horas_Reales"}.issubset(df_local.columns):
        df_act = df_local.groupby("Actividad", as_index=False)["Horas_Reales"].sum().sort_values("Horas_Reales", ascending=False)
        figs["act_pie"] = px.pie(df_act, names="Actividad", values="Horas_Reales", hole=0.35, title="Horas por actividad")

        palette = px.colors.qualitative.Set3 + px.colors.qualitative.Pastel + px.colors.qualitative.Bold
        act_names = df_act["Actividad"].tolist()
        act_color_map = {a: palette[i % len(palette)] for i, a in enumerate(act_names)}

        figs["act_bar"] = px.bar(
            df_act, x="Actividad", y="Horas_Reales", color="Actividad",
            title="Distribución de actividades (24 h)",
            color_discrete_map=act_color_map,
            text="Horas_Reales",
        )
        figs["act_bar"].update_layout(showlegend=False)

    # conexiones
    if modo_reporte == "Perforación" and not dfc_local.empty and {"Componente", "Minutos_Reales"}.issubset(dfc_local.columns):
        df_conn_sum = dfc_local.groupby("Componente", as_index=False)["Minutos_Reales"].sum()
        df_conn_sum["Componente"] = pd.Categorical(df_conn_sum["Componente"], categories=CONN_ORDER, ordered=True)
        df_conn_sum = df_conn_sum.sort_values("Componente")

        figs["conn_pie"] = px.pie(
            df_conn_sum, names="Componente", values="Minutos_Reales", hole=0.35,
            title="Distribución de tiempo en conexión (min/% )",
            color="Componente", color_discrete_map=CONN_COLOR_MAP
        )

        df_stack = dfc_local.copy()
        df_stack["Conn_Label"] = df_stack["Profundidad_m"].fillna(df_stack["Conn_No"]).astype(float).astype(int).astype(str)
        df_stack["Componente"] = pd.Categorical(df_stack["Componente"], categories=CONN_ORDER, ordered=True)

        df_stack_g = df_stack.groupby(["Conn_Label", "Componente"], as_index=False)["Minutos_Reales"].sum().sort_values(["Conn_Label", "Componente"])

        per_conn = df_stack.groupby("Conn_Label", as_index=False).first()[["Conn_Label", "Conn_Tipo", "Angulo_Bucket"]]
        per_conn["Std_Total"] = per_conn.apply(
            lambda r: float(CONN_STDS.get((r["Conn_Tipo"], r["Angulo_Bucket"]), {}).get("TOTAL", 0.0)),
            axis=1,
        )
        std_line = float(per_conn["Std_Total"].mean()) if not per_conn.empty else 0.0

        fig_conn_stack = px.bar(
            df_stack_g,
            x="Conn_Label",
            y="Minutos_Reales",
            color="Componente",
            category_orders={"Componente": CONN_ORDER},
            color_discrete_map=CONN_COLOR_MAP,
            barmode="stack",
            title="Conexiones perforando",
            labels={"Conn_Label": "Profundidad (m)", "Minutos_Reales": "Tiempo (min)"},
        )

        if std_line > 0:
            fig_conn_stack.add_hline(
                y=std_line,
                line_dash="dash",
                line_color="#9C640C",
                annotation_text=f"{std_line:.1f}",
                annotation_position="top left",
                annotation_font_color="#9C640C",
            )

        df_tot = df_stack.groupby("Conn_Label", as_index=False)["Minutos_Reales"].sum().rename(columns={"Minutos_Reales": "Real_Total"})
        tot_map = dict(zip(df_tot["Conn_Label"].astype(str), df_tot["Real_Total"]))
        for x in sorted(df_tot["Conn_Label"].astype(str).unique(), key=lambda v: float(v) if v.replace(".", "", 1).isdigit() else v):
            y = float(tot_map.get(x, 0))
            fig_conn_stack.add_annotation(x=x, y=y, text=f"<b>{y:.0f}</b>", showarrow=False, yshift=10)

        fig_conn_stack.update_layout(legend_title_text="", xaxis_tickangle=0)
        figs["conn_stack"] = fig_conn_stack

    return figs

df_json = df.to_json(orient="split") if not df.empty else ""
df_conn_json = df_conn.to_json(orient="split") if not df_conn.empty else ""
figs = _make_figs(df_json, df_conn_json, modo_reporte) if show_charts else {"tiempos": None, "act_pie": None, "act_bar": None, "conn_pie": None, "conn_stack": None}

# =====================================================================
# NAV PRO: TABS
# =====================================================================
st.session_state.df = _ensure_rowid(st.session_state.df)

tab_resumen, tab_act, tab_conn, tab_viajes, tab_bha, tab_rop, tab_detalle, tab_comp, tab_estadisticas, tab_corridas, tab_drillspot, tab_general, tab_ejecutivo, tab_export = st.tabs([

    "Resumen", "Indicadores (Actividades)", "Conexiones", "Viajes y conexiones", 
    "BHA (Arma/Desarma)", "ROP", "Detalle", "Comparativa de Etapas", 
    "Estadísticas por Etapa", "Estadísticas por Corrida", "Estadísticas DrillSpot",
    "Reporte General del Pozo", "Ejecutivo", "Exportar"
])
# =====================================================================
# TAB: RESUMEN
# =====================================================================
# =====================================================================
# TAB: RESUMEN (MODIFICADO CON FILTRO DE ETAPA)
# =====================================================================
with tab_resumen:
    st.subheader("Indicador de desempeño (principal)")
    
    # --- FILTRO DE ETAPA EN EL RESUMEN ---
    col_filtro1, col_filtro2 = st.columns([1, 3])
    with col_filtro1:
        # Obtener todas las etapas disponibles
        etapas_disponibles = sorted(df["Etapa"].unique().tolist()) if not df.empty else ["Sin datos"]
        
        # Selector de etapa para el resumen
        etapa_resumen = st.selectbox(
            "Etapa para resumen",
            options=etapas_disponibles,
            index=etapas_disponibles.index(etapa) if etapa in etapas_disponibles else 0,
            key="etapa_resumen"
        )
    
    # Filtrar datos por la etapa seleccionada
    if etapa_resumen != "Sin datos":
        df_resumen_filtrado = df[df["Etapa"] == etapa_resumen].copy()
        df_conn_filtrado = df_conn[df_conn["Etapa"] == etapa_resumen].copy()
    else:
        df_resumen_filtrado = pd.DataFrame()
        df_conn_filtrado = pd.DataFrame()
    
    # Recalcular KPIs con datos filtrados
    total_prog_filtrado = float(df_resumen_filtrado["Horas_Prog"].sum()) if not df_resumen_filtrado.empty else 0.0
    total_real_filtrado = float(df_resumen_filtrado["Horas_Reales"].sum()) if not df_resumen_filtrado.empty else 0.0
    tp_h_filtrado = float(df_resumen_filtrado[df_resumen_filtrado["Tipo"] == "TP"]["Horas_Reales"].sum()) if not df_resumen_filtrado.empty else 0.0
    tnpi_h_filtrado = float(df_resumen_filtrado[df_resumen_filtrado["Tipo"] == "TNPI"]["Horas_Reales"].sum()) if not df_resumen_filtrado.empty else 0.0
    tnp_h_filtrado = float(df_resumen_filtrado[df_resumen_filtrado["Tipo"] == "TNP"]["Horas_Reales"].sum()) if not df_resumen_filtrado.empty else 0.0
    eficiencia_dia_filtrado = clamp_0_100(safe_pct(tp_h_filtrado, total_real_filtrado)) if total_real_filtrado > 0 else 0.0
    
    # Recalcular KPIs de conexiones filtradas
    conn_real_min_filtrado = 0.0
    conn_std_min_filtrado = 0.0
    conn_tnpi_min_filtrado = 0.0
    conn_tnp_min_filtrado = 0.0
    eff_conn_filtrado = 0.0
    
    if not df_conn_filtrado.empty:
        conn_real_min_filtrado = float(df_conn_filtrado.groupby(["Conn_No"])["Minutos_Reales"].sum().sum())
        per_conn2_filtrado = df_conn_filtrado.groupby("Conn_No", as_index=False).first()[["Conn_No", "Conn_Tipo", "Angulo_Bucket"]]
        per_conn2_filtrado["Std_Total"] = per_conn2_filtrado.apply(
            lambda r: float(CONN_STDS.get((r["Conn_Tipo"], r["Angulo_Bucket"]), {}).get("TOTAL", 0.0)),
            axis=1
        )
        conn_std_min_filtrado = float(per_conn2_filtrado["Std_Total"].sum())
        
        conn_tp_min_filtrado = min(conn_real_min_filtrado, conn_std_min_filtrado) if conn_std_min_filtrado > 0 else conn_real_min_filtrado
        conn_tnpi_min_filtrado = max(0.0, conn_real_min_filtrado - conn_std_min_filtrado) if conn_std_min_filtrado > 0 else 0.0
        eff_conn_filtrado = clamp_0_100(safe_pct(conn_tp_min_filtrado, conn_real_min_filtrado)) if conn_real_min_filtrado > 0 else 0.0
    
    # Gauge con eficiencia filtrada
    fig_g = build_gauge(f"Eficiencia - {etapa_resumen}", eficiencia_dia_filtrado) if PLOTLY_IMG_OK else None
    col_g, col_t = st.columns([1.05, 2.0])

    with col_g:
        if fig_g is not None:
            st.plotly_chart(fig_g, use_container_width=True)
        else:
            st.info("Para gauge instala kaleido: pip install -U kaleido")

    with col_t:
        # KPIs específicos de la etapa seleccionada
        kpi_rows = [
            {"kpi": "Horas Totales", "real": f"{total_real_filtrado:.1f} h", "tnpi": f"{tnpi_h_filtrado:.1f} h", "eff": eficiencia_dia_filtrado},
            {"kpi": "Conexión perforando", "real": f"{(conn_real_min_filtrado/60.0):.2f} h", "tnpi": f"{(conn_tnpi_min_filtrado/60.0):.2f} h", "eff": eff_conn_filtrado},
        ]
        
        # Agregar metros y ROP si tenemos datos por etapa
        if modo_reporte == "Perforación" and etapa_resumen != "Sin datos":
            # Obtener datos específicos de esta etapa
            etapa_data_resumen = get_etapa_data(etapa_resumen)
            
            mr_total = float(etapa_data_resumen.get("metros_real_dia", 0.0)) + float(etapa_data_resumen.get("metros_real_noche", 0.0))
            tnpi_m_h = float(etapa_data_resumen.get("tnpi_metros_h", 0.0))
            mp_total = float(etapa_data_resumen.get("metros_prog_total", 0.0))
            
            eff_m = clamp_0_100(safe_pct(mr_total, mp_total)) if mp_total > 0 else 0.0
            
            kpi_rows.insert(0, {"kpi": "Metros perforados", "real": f"{mr_total:.0f} m", "tnpi": f"{tnpi_m_h:.2f} h", "eff": eff_m})
        
        components.html(kpi_table_html(kpi_rows), height=300, scrolling=False)
    
    # Mostrar indicador claro de qué etapa estamos viendo
    with col_filtro2:
        st.markdown(f"""
            <div style='background: rgba(46, 134, 193, 0.1); padding: 10px; border-radius: 10px; border-left: 4px solid #2E86C1; margin-top: 10px;'>
                <div style='font-size: 14px; color: #2E86C1; font-weight: bold;'>Etapa seleccionada:</div>
                <div style='font-size: 18px; color: white; font-weight: bold;'>{etapa_resumen}</div>
                <div style='font-size: 12px; color: rgba(255,255,255,0.7); margin-top: 5px;'>
                    {len(df_resumen_filtrado)} actividades | {len(df_conn_filtrado)} conexiones
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    # ------------------------------
    # Avance de profundidad (solo Perforación)
    # ------------------------------
    if modo_reporte == "Perforación" and etapa_resumen != "Sin datos":
        # Obtener datos específicos de esta etapa
        etapa_data_resumen = get_etapa_data(etapa_resumen)
        
        pt_prog = float(etapa_data_resumen.get("pt_programada_m", 0.0) or 0.0)
        prof_act = float(etapa_data_resumen.get("prof_actual_m", 0.0) or 0.0)
        
        avance = (prof_act / pt_prog) if pt_prog > 0 else 0.0
        avance = max(0.0, min(1.0, avance))
        
        st.markdown("### Avance de profundidad")
        st.progress(avance)
        
        c1, c2, c3 = st.columns(3)
        c1.metric("PT programada (m)", f"{pt_prog:,.0f}")
        c2.metric("Profundidad actual (m)", f"{prof_act:,.0f}")
        c3.metric("Avance", f"{avance*100:.1f}%")
        
        st.divider()

    if show_charts and etapa_resumen != "Sin datos":
        st.divider()
        st.subheader(f"Gráficas - {etapa_resumen}")
        
        # Generar figuras específicas para esta etapa
        if not df_resumen_filtrado.empty:
            # Tiempos (TP vs TNPI vs TNP)
            df_tiempos = df_resumen_filtrado.groupby("Tipo")["Horas_Reales"].sum().reset_index()
            if not df_tiempos.empty:
                fig_tiempos = px.pie(df_tiempos, names="Tipo", values="Horas_Reales", 
                                     hole=0.55, title=f"TP vs TNPI vs TNP - {etapa_resumen}")
                st.plotly_chart(fig_tiempos, use_container_width=True)
            
            # Actividades
            df_act = df_resumen_filtrado.groupby("Actividad", as_index=False)["Horas_Reales"].sum().sort_values("Horas_Reales", ascending=False)
            if not df_act.empty:
                fig_act_pie = px.pie(df_act.head(8), names="Actividad", values="Horas_Reales", 
                                     hole=0.35, title=f"Top Actividades - {etapa_resumen}")
                st.plotly_chart(fig_act_pie, use_container_width=True)

    # -----------------------------------------------------------------
    # RESUMEN DIARIO (mismas gráficas pero por Fecha)
    # -----------------------------------------------------------------
    with st.expander("Resumen diario (por fecha)", expanded=False):
        if not df_resumen_filtrado.empty and "Fecha" in df_resumen_filtrado.columns:
            fechas_disp = (
                sorted(df_resumen_filtrado["Fecha"].astype(str).dropna().unique().tolist())
                if not df_resumen_filtrado["Fecha"].isna().all()
                else []
            )
        else:
            fechas_disp = []
        if len(fechas_disp) == 0:
            st.info("No hay fechas disponibles para generar el resumen diario.")
        else:
            fecha_resumen = st.selectbox(
                "Fecha para resumen diario",
                options=fechas_disp,
                index=len(fechas_disp) - 1,
                key="fecha_resumen_diario"
            )

            df_diario = df_resumen_filtrado[df_resumen_filtrado["Fecha"].astype(str) == str(fecha_resumen)].copy()

            if df_diario.empty:
                st.info(f"No hay datos para la fecha {fecha_resumen} (etapa {etapa_resumen}).")
            else:
                # KPIs diarios
                total_real_d = float(df_diario["Horas_Reales"].sum()) if "Horas_Reales" in df_diario.columns else 0.0
                tp_h_d = float(df_diario[df_diario["Tipo"] == "TP"]["Horas_Reales"].sum()) if "Tipo" in df_diario.columns else 0.0
                tnpi_h_d = float(df_diario[df_diario["Tipo"] == "TNPI"]["Horas_Reales"].sum()) if "Tipo" in df_diario.columns else 0.0
                tnp_h_d = float(df_diario[df_diario["Tipo"] == "TNP"]["Horas_Reales"].sum()) if "Tipo" in df_diario.columns else 0.0
                eff_d = clamp_0_100(safe_pct(tp_h_d, total_real_d)) if total_real_d > 0 else 0.0

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Horas (Real)", f"{total_real_d:.2f}")
                c2.metric("TP (h)", f"{tp_h_d:.2f}")
                c3.metric("TNPI (h)", f"{tnpi_h_d:.2f}")
                c4.metric("TNP (h)", f"{tnp_h_d:.2f}")
                c5.metric("Eficiencia", f"{eff_d:.1f}%")

                # Gráfica KPI diaria
                df_tiempos_d = df_diario.groupby("Tipo")["Horas_Reales"].sum().reset_index()
                if not df_tiempos_d.empty:
                    fig_tiempos_d = px.pie(
                        df_tiempos_d, names="Tipo", values="Horas_Reales",
                        hole=0.55, title=f"TP vs TNPI vs TNP (Diario) - {fecha_resumen} / {etapa_resumen}"
                    )
                    st.plotly_chart(fig_tiempos_d, use_container_width=True)

                # Actividades diarias
                df_act_d = (
                    df_diario.groupby("Actividad", as_index=False)["Horas_Reales"]
                    .sum()
                    .sort_values("Horas_Reales", ascending=False)
                )
                if not df_act_d.empty:
                    fig_act_pie_d = px.pie(
                        df_act_d.head(10), names="Actividad", values="Horas_Reales",
                        hole=0.35, title=f"Top Actividades (Diario) - {fecha_resumen} / {etapa_resumen}"
                    )
                    st.plotly_chart(fig_act_pie_d, use_container_width=True)

                # Tabla resumen diario
                with st.expander("Ver tabla diaria (etapa + fecha)", expanded=False):
                    cols_show = [c for c in ["Fecha","Etapa","Actividad","Tipo","Horas_Prog","Horas_Reales","Categoria_TNPI","Detalle_TNPI","Categoria_TNP","Detalle_TNP","Comentario"] if c in df_diario.columns]
                    st.dataframe(df_diario[cols_show], use_container_width=True, height=260)

# =====================================================================
# TAB: INDICADORES ACTIVIDADES
# =====================================================================
with tab_act:
    # --- NUEVO: Vista de indicadores (diario vs acumulado) ---
    vista_ind = st.radio(
        "Vista de indicadores",
        ["Día seleccionado", "Acumulado (toda la jornada)"],
        index=0,
        horizontal=True,
        key="vista_indicadores",
    )

    # Base dataframe para indicadores
    df_ind_base = st.session_state.get("df", pd.DataFrame()).copy()

    # Filtrar por fecha seleccionada (puede incluir varias etapas)
    if vista_ind == "Día seleccionado":
        fecha_sel = st.session_state.get("fecha_val", None)
        if fecha_sel is not None and "Fecha" in df_ind_base.columns:
            df_ind_base["_Fecha_dt"] = pd.to_datetime(df_ind_base["Fecha"], errors="coerce")
            try:
                fecha_date = fecha_sel if hasattr(fecha_sel, "year") else pd.to_datetime(fecha_sel).date()
            except Exception:
                fecha_date = pd.to_datetime(fecha_sel, errors="coerce").date()
            df_ind_base = df_ind_base[df_ind_base["_Fecha_dt"].dt.date == fecha_date].copy()
            df_ind_base.drop(columns=["_Fecha_dt"], inplace=True, errors="ignore")

    st.subheader("Indicador de desempeño por actividades")
    rows_act = []
    if not df_ind_base.empty:
        g = df_ind_base.groupby(["Actividad", "Tipo"], as_index=False)["Horas_Reales"].sum()
        piv = g.pivot_table(index="Actividad", columns="Tipo", values="Horas_Reales", aggfunc="sum", fill_value=0.0).reset_index()
        for col in ["TP", "TNPI", "TNP"]:
            if col not in piv.columns:
                piv[col] = 0.0
        piv["Real"] = piv["TP"] + piv["TNPI"] + piv["TNP"]
        def _safe_pct(n, d):
            return (float(n) / float(d) * 100.0) if float(d) > 0 else 0.0
        piv["Eficiencia"] = piv.apply(lambda r: max(0.0, min(100.0, _safe_pct(r["TP"], r["Real"]))), axis=1)
        piv = piv.sort_values("Real", ascending=False)
        for _, r in piv.iterrows():
            rows_act.append({
                "name": str(r["Actividad"]),
                "real": f"{float(r['Real']):.2f}",
                "tnpi": f"{float(r['TNPI']):.2f}",
                "tnp": f"{float(r['TNP']):.2f}",
                "eff": float(r["Eficiencia"]),
                "semaforo": _semaforo_text(float(r["Eficiencia"]))
            })
    if rows_act:
        components.html(indicators_table_html("Indicador de desempeño por actividades", rows_act, kind="actividad"), height=520, scrolling=True)
    else:
        st.info("Aún no hay datos suficientes para indicador por actividades.")

# =====================================================================
# TAB: CONEXIONES
# =====================================================================
with tab_conn:
    if st.session_state.get("_toast_conn", False):
        st.success("Conexión agregada correctamente")
        st.session_state["_toast_conn"] = False

    st.subheader("Conexiones perforando")

    if modo_reporte != "Perforación":
        st.info("Cambia a modo **Perforación** para ver conexiones.")
    else:
        # ------------------------------
        # Selector de etapa (para separar gráficas por etapa)
        # ------------------------------
        etapas_conn = sorted(df_conn["Etapa"].dropna().unique().tolist()) if not df_conn.empty else []
        etapa_conn_view = st.selectbox(
            "Etapa para conexiones",
            options=etapas_conn if etapas_conn else ["Sin datos"],
            index=(etapas_conn.index(etapa) if etapas_conn and etapa in etapas_conn else 0),
            key="etapa_conn_view",
            help="Filtra las conexiones y sus gráficas por etapa (evita mezclar varias etapas en la misma gráfica).",
        )

        df_conn_view = df_conn[df_conn["Etapa"] == etapa_conn_view].copy() if (etapa_conn_view != "Sin datos" and not df_conn.empty) else pd.DataFrame()

        # ------------------------------
        # Gráficas (pie + stacked) por etapa
        # ------------------------------
        if show_charts:
            if df_conn_view.empty:
                st.info("Aún no hay datos de conexiones para la etapa seleccionada.")
            else:
                # Pie por componentes
                if {"Componente", "Minutos_Reales"}.issubset(df_conn_view.columns):
                    df_conn_sum = df_conn_view.groupby("Componente", as_index=False)["Minutos_Reales"].sum()
                    df_conn_sum["Componente"] = pd.Categorical(df_conn_sum["Componente"], categories=CONN_ORDER, ordered=True)
                    df_conn_sum = df_conn_sum.sort_values("Componente")

                    fig_conn_pie = px.pie(
                        df_conn_sum,
                        names="Componente",
                        values="Minutos_Reales",
                        hole=0.35,
                        title=f"Distribución de tiempo en conexión — {etapa_conn_view}",
                        color="Componente",
                        color_discrete_map=CONN_COLOR_MAP,
                    )
                    st.plotly_chart(fig_conn_pie, use_container_width=True)

                # Stacked por conexión/profundidad
                df_stack = df_conn_view.copy()
                df_stack["Conn_Label"] = df_stack["Profundidad_m"].fillna(df_stack["Conn_No"]).astype(float).astype(int).astype(str)
                df_stack["Componente"] = pd.Categorical(df_stack["Componente"], categories=CONN_ORDER, ordered=True)
                df_stack_g = (
                    df_stack.groupby(["Conn_Label", "Componente"], as_index=False)["Minutos_Reales"]
                    .sum()
                    .sort_values(["Conn_Label", "Componente"])
                )

                per_conn = df_stack.groupby("Conn_Label", as_index=False).first()[["Conn_Label", "Conn_Tipo", "Angulo_Bucket"]]
                per_conn["Std_Total"] = per_conn.apply(
                    lambda r: float(CONN_STDS.get((r["Conn_Tipo"], r["Angulo_Bucket"]), {}).get("TOTAL", 0.0)),
                    axis=1,
                )
                std_line = float(per_conn["Std_Total"].mean()) if not per_conn.empty else 0.0

                fig_conn_stack = px.bar(
                    df_stack_g,
                    x="Conn_Label",
                    y="Minutos_Reales",
                    color="Componente",
                    category_orders={"Componente": CONN_ORDER},
                    color_discrete_map=CONN_COLOR_MAP,
                    barmode="stack",
                    title=f"Conexiones perforando — {etapa_conn_view}",
                    labels={"Conn_Label": "Profundidad (m)", "Minutos_Reales": "Tiempo (min)"},
                )

                if std_line > 0:
                    fig_conn_stack.add_hline(
                        y=std_line,
                        line_dash="dash",
                        line_color="#9C640C",
                        annotation_text=f"{std_line:.1f}",
                        annotation_position="top left",
                        annotation_font_color="#9C640C",
                    )

                df_tot = (
                    df_stack.groupby("Conn_Label", as_index=False)["Minutos_Reales"]
                    .sum()
                    .rename(columns={"Minutos_Reales": "Real_Total"})
                )
                tot_map = dict(zip(df_tot["Conn_Label"].astype(str), df_tot["Real_Total"]))
                for x in sorted(df_tot["Conn_Label"].astype(str).unique(), key=lambda v: float(v) if v.replace(".", "", 1).isdigit() else v):
                    y = float(tot_map.get(x, 0))
                    fig_conn_stack.add_annotation(x=x, y=y, text=f"<b>{y:.0f}</b>", showarrow=False, yshift=10)

                fig_conn_stack.update_layout(legend_title_text="", xaxis_tickangle=0)
                st.plotly_chart(fig_conn_stack, use_container_width=True)

        st.subheader("Indicador de desempeño por conexiones")
        rows_conn = []
        if not df_conn_view.empty:
            per = df_conn_view.groupby(["Conn_No", "Profundidad_m"], as_index=False).agg(
                real_min=("Minutos_Reales", "sum"),
                tnpi_min=("Minutos_TNPI", "sum"),
                tnp_min=("Minutos_TNP", "sum") if "Minutos_TNP" in df_conn_view.columns else ("Minutos_TNPI", "sum"),
            )
            per["eff"] = per.apply(
                lambda r: clamp_0_100(safe_pct(r["real_min"] - r["tnpi_min"] - r.get("tnp_min", 0.0), r["real_min"])) if r["real_min"] > 0 else 0.0,
                axis=1,
            )
            per = per.sort_values("Conn_No", ascending=True)

            for _, r in per.iterrows():
                name = f"#{int(r['Conn_No'])}  (Prof {float(r['Profundidad_m']):.0f} m)"
                rows_conn.append(
                    {
                        "name": name,
                        "real": f"{float(r['real_min']):.0f}",
                        "tnpi": f"{float(r['tnpi_min']):.0f}",
                        "tnp": f"{float(r.get('tnp_min', 0.0)):.0f}",
                        "eff": float(r["eff"]),
                    }
                )

        if rows_conn:
            components.html(indicators_table_html(f"Indicador de desempeño por conexiones — {etapa_conn_view}", rows_conn, kind="conexion"), height=420, scrolling=True)
        else:
            st.info("Aún no hay conexiones para indicador en la etapa seleccionada.")


# =====================================================================
# TAB: ROP (REAL VS PROGRAMADO)
# =====================================================================
# =====================================================================
with tab_rop:
    st.subheader("ROP del día – Real vs Programado")

    if modo_reporte != "Perforación":
        st.info("Esta pestaña aplica para modo **Perforación**.")
    else:
        # --- NUEVO: captura por fecha (evita que se "arrastre" al cambiar de día) ---
        fecha_key = str(st.session_state.get("fecha_val", datetime.today().date()))
        def _get_by_date(etapa_data: dict, k: str, default: float = 0.0) -> float:
            try:
                return float((etapa_data.get(k, {}) or {}).get(fecha_key, default))
            except Exception:
                return float(default)



        # --- NUEVO: aplicar reseteos pendientes ANTES de instanciar widgets (evita StreamlitAPIException) ---
        if st.session_state.get("_pending_widget_resets"):
            for _k, _v in list(st.session_state["_pending_widget_resets"].items()):
                st.session_state[_k] = _v
            st.session_state["_pending_widget_resets"].clear()

        # FIX: asegurar que etapa_data_rop exista antes de usarse
        etapa_data_rop = get_etapa_data(etapa)
        c1, c2, c3 = st.columns(3)
        with c1:
            # Obtener/actualizar datos de ROP para esta etapa
            rop_prog_val = float(etapa_data_rop.get("rop_prog_total", 0.0))
            rop_prog_val = st.number_input(
                f"ROP programada - {etapa} (m/h)",
                min_value=0.0, step=0.1,
                value=rop_prog_val,
                key=f"rop_prog_{etapa}",
            )
            etapa_data_rop["rop_prog_total"] = float(rop_prog_val)
            st.session_state.drill_day["rop_prog_total"] = float(rop_prog_val)
            
        with c2:
            rop_dia_val = _get_by_date(etapa_data_rop, "rop_real_dia_by_date", 0.0)
            rop_dia_val = st.number_input(
                f"ROP real Día - {etapa} (m/h)",
                min_value=0.0, step=0.1,
                value=float(rop_dia_val),
                key=f"rop_real_dia_{etapa}_{fecha_key}",
            )
            etapa_data_rop["rop_real_dia"] = float(rop_dia_val)
            st.session_state.drill_day["rop_real_dia"] = float(rop_dia_val)

            # Guardar histórico por fecha (1 valor por día)
            _fecha_key = str(st.session_state.get("fecha_val", datetime.today().date()))
            etapa_data_rop.setdefault("rop_real_dia_by_date", {})
            if float(rop_dia_val) > 0:
                etapa_data_rop["rop_real_dia_by_date"][_fecha_key] = float(rop_dia_val)
            
        with c3:
            rop_noche_val = _get_by_date(etapa_data_rop, "rop_real_noche_by_date", 0.0)
            rop_noche_val = st.number_input(
                f"ROP real Noche - {etapa} (m/h)",
                min_value=0.0, step=0.1,
                value=float(rop_noche_val),
                key=f"rop_real_noche_{etapa}_{fecha_key}",
            )
            etapa_data_rop["rop_real_noche"] = float(rop_noche_val)
            st.session_state.drill_day["rop_real_noche"] = float(rop_noche_val)

            _fecha_key = str(st.session_state.get("fecha_val", datetime.today().date()))
            etapa_data_rop.setdefault("rop_real_noche_by_date", {})
            if float(rop_noche_val) > 0:
                etapa_data_rop["rop_real_noche_by_date"][_fecha_key] = float(rop_noche_val)

        # Sincroniza (compatibilidad con otros bloques que lean claves sueltas)
        st.session_state["rop_prog_total"] = float(st.session_state.drill_day["rop_prog_total"])
        st.session_state["rop_real_diurno"] = float(st.session_state.drill_day["rop_real_dia"])
        st.session_state["rop_real_nocturno"] = float(st.session_state.drill_day["rop_real_noche"])

        rp = float(st.session_state.drill_day.get("rop_prog_total", 0.0) or 0.0)
        rd = float(st.session_state.drill_day.get("rop_real_dia", 0.0) or 0.0)
        rn = float(st.session_state.drill_day.get("rop_real_noche", 0.0) or 0.0)

        # Promedio ponderado por metros (si están capturados), si no, promedio simple de turnos no-cero
        md = float(st.session_state.drill_day.get("metros_real_dia", 0.0) or 0.0)
        mn = float(st.session_state.drill_day.get("metros_real_noche", 0.0) or 0.0)
        if (md + mn) > 0:
            rr_avg = ((rd * md) + (rn * mn)) / (md + mn)
        else:
            vals = [v for v in [rd, rn] if v > 0]
            rr_avg = sum(vals) / len(vals) if vals else 0.0

        eff_rop_day = clamp_0_100(safe_pct(rr_avg, rp)) if rp > 0 else 0.0
        sk, sl, sc = status_from_eff(eff_rop_day)

        k1, k2, k3, k4 = st.columns([1.2, 1.2, 1.2, 1.0])
        k1.metric("ROP real promedio (m/h)", f"{rr_avg:.2f}")
        k2.metric("ROP programada (m/h)", f"{rp:.2f}")
        k3.metric("Eficiencia ROP (%)", f"{eff_rop_day:.0f}%")
        with k4:
            st.markdown(f"""<div style="display:flex;align-items:center;gap:10px;margin-top:28px;">
                <span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:{sc};box-shadow:0 0 0 2px rgba(255,255,255,0.08);"></span>
                <div style="font-weight:800;font-size:22px;letter-spacing:0.5px;">{sl}</div>
            </div>""", unsafe_allow_html=True)

        # Gráfica
        df_rop = pd.DataFrame([
            {"Turno": "Día", "Programado (m/h)": rp, "Real (m/h)": rd},
            {"Turno": "Noche", "Programado (m/h)": rp, "Real (m/h)": rn},
        ])
        fig_rop = px.bar(df_rop, x="Turno", y=["Programado (m/h)", "Real (m/h)"], barmode="group", text_auto=True)
        fig_rop.update_layout(margin=dict(l=10, r=10, t=30, b=10), height=340, legend_title_text="Serie")
        st.plotly_chart(fig_rop, use_container_width=True)

        # Detalle + semáforo por turno
        def _eff_turno(real_v: float, prog_v: float) -> float:
            return clamp_0_100(safe_pct(real_v, prog_v)) if prog_v > 0 else 0.0

        rows = []
        for turno_lbl, real_v in [("Día", rd), ("Noche", rn)]:
            e = _eff_turno(real_v, rp)
            _, _, c = status_from_eff(e)
            rows.append({
                "Turno": turno_lbl,
                "ROP Programado (m/h)": round(rp, 2),
                "ROP Real (m/h)": round(real_v, 2),
                "Eficiencia (%)": round(e, 0),
                "Semáforo": "🟢" if e >= 85 else ("🟡" if e >= 70 else "🔴"),
            })
        st.markdown("### Detalle")
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

       # ---------------------- Metros perforados (día): Programado vs Real ----------------------
        st.subheader(f"Metros perforados (día) - {etapa}")

        # Obtener datos específicos de esta etapa
        etapa_data_rop = get_etapa_data(etapa)
        
        # Inputs (mismo estilo que ROP, pero para metros) - AHORA POR ETAPA
        colm1, colm2, colm3 = st.columns(3)
        
        with colm1:
            mp = st.number_input(
                f"Metros programados - {etapa} (m)",
                min_value=0.0,
                value=float(etapa_data_rop.get("metros_prog_total", 0.0)),
                step=1.0,
                key=f"metros_prog_{etapa}",
            )
            # Guardar en datos por etapa
            etapa_data_rop["metros_prog_total"] = float(mp)
        
        with colm2:
            mr_d = st.number_input(
                f"Metros reales Día - {etapa} (m)",
                min_value=0.0,
                value=float(_get_by_date(etapa_data_rop, "metros_real_dia_by_date", 0.0)),
                step=1.0,
                key=f"metros_real_dia_{etapa}_{fecha_key}",
            )
            etapa_data_rop["metros_real_dia"] = float(mr_d)
        
        with colm3:

            _fecha_key = str(st.session_state.get("fecha_val", datetime.today().date()))
            etapa_data_rop.setdefault("metros_real_dia_by_date", {})
            if float(mr_d) > 0:
                etapa_data_rop["metros_real_dia_by_date"][_fecha_key] = float(mr_d)
            mr_n = st.number_input(
                f"Metros reales Noche - {etapa} (m)",
                min_value=0.0,
                value=float(_get_by_date(etapa_data_rop, "metros_real_noche_by_date", 0.0)),
                step=1.0,
                key=f"metros_real_noche_{etapa}_{fecha_key}",
            )
            etapa_data_rop["metros_real_noche"] = float(mr_n)


        # --- NUEVO: botón para registrar SOLO el día de la ETAPA ACTUAL ---
        col_reg1, col_reg2 = st.columns([1.4, 2.6])
        with col_reg1:
            if st.button(f"Registrar día (etapa {etapa})", use_container_width=True, key=f"btn_registrar_dia_{etapa}_{fecha_key}"):
                etapa_data_rop.setdefault("metros_real_dia_by_date", {})
                etapa_data_rop.setdefault("metros_real_noche_by_date", {})
                etapa_data_rop.setdefault("rop_real_dia_by_date", {})
                etapa_data_rop.setdefault("rop_real_noche_by_date", {})

                # Guardar valores del día seleccionado
                etapa_data_rop["metros_real_dia_by_date"][fecha_key] = float(mr_d)
                etapa_data_rop["metros_real_noche_by_date"][fecha_key] = float(mr_n)
                etapa_data_rop["rop_real_dia_by_date"][fecha_key] = float(st.session_state.get(f"rop_real_dia_{etapa}_{fecha_key}", 0.0))
                etapa_data_rop["rop_real_noche_by_date"][fecha_key] = float(st.session_state.get(f"rop_real_noche_{etapa}_{fecha_key}", 0.0))

                # Limpia la captura visible de ese día (para que el siguiente día empiece limpio)
                st.session_state.setdefault("_pending_widget_resets", {})
                st.session_state["_pending_widget_resets"][f"metros_real_dia_{etapa}_{fecha_key}"] = 0.0
                st.session_state["_pending_widget_resets"][f"metros_real_noche_{etapa}_{fecha_key}"] = 0.0
                st.session_state["_pending_widget_resets"][f"rop_real_dia_{etapa}_{fecha_key}"] = 0.0
                st.session_state["_pending_widget_resets"][f"rop_real_noche_{etapa}_{fecha_key}"] = 0.0

                st.success("Día registrado ✅ (se guardó en el histórico por fecha de la etapa)")
                st.rerun()
        with col_reg2:
            st.caption("Al cambiar la fecha, los inputs ahora son independientes por día. Este botón confirma el registro del día para la etapa actual y limpia la captura.")

        # Mantener compatibilidad (opcional)

            _fecha_key = str(st.session_state.get("fecha_val", datetime.today().date()))
            etapa_data_rop.setdefault("metros_real_noche_by_date", {})
            if float(mr_n) > 0:
                etapa_data_rop["metros_real_noche_by_date"][_fecha_key] = float(mr_n)
        st.session_state["drill_day"]["metros_prog_total"] = float(mp)
        st.session_state["drill_day"]["metros_real_diurno"] = float(mr_d)
        st.session_state["drill_day"]["metros_real_nocturno"] = float(mr_n)

        mr_total = float(mr_d) + float(mr_n)
        eff_m = 0.0
        if mp > 0:
            eff_m = max(0.0, min(100.0, (mr_total / mp) * 100.0))

        # KPIs tipo "pro" (como ROP)
        kpi1, kpi2, kpi3, kpi4 = st.columns([1.2, 1.2, 1.2, 1.0])
        with kpi1:
            st.metric("Metros reales (total)", f"{mr_total:.0f} m")
        with kpi2:
            st.metric("Metros programados", f"{mp:.0f} m")
        with kpi3:
            st.metric("Eficiencia metros (%)", f"{eff_m:.0f}%")
        with kpi4:
            _st_key, _st_label, _st_color = status_from_eff(eff_m)
            st.markdown(
                f"""<div style='display:flex;align-items:center;gap:10px;'>
                        <span style='height:12px;width:12px;border-radius:50%;background:{_st_color};display:inline-block;'></span>
                        <span style='font-size:22px;font-weight:800;'>{_st_label}</span>
                    </div>""",
                unsafe_allow_html=True,
            )

        # Gráfica: Programado vs Real (Día / Noche / Total)
        df_m = pd.DataFrame(
            {
                "Concepto": ["Programado", "Real (Día)", "Real (Noche)", "Real (Total)"],
                "Metros": [mp, mr_d, mr_n, mr_total],
            }
        )

        if df_m["Metros"].sum() > 0:
            is_dark = st.session_state.get("ui_mode", "Nocturno") == "Nocturno"
            fig_m = px.bar(
                df_m,
                x="Concepto",
                y="Metros",
                text="Metros",
                color="Concepto",
                title="Metros perforados — Programado vs Real",
                template="plotly_dark" if is_dark else "plotly_white",
                color_discrete_map={
                    "Programado": "#636EFA",
                    "Real (Día)": "#00CC96",
                    "Real (Noche)": "#AB63FA",
                    "Real (Total)": "#EF553B",
                },
            )
            fig_m.update_traces(textposition="outside")
            fig_m.update_layout(margin=dict(l=10, r=10, t=60, b=10), height=420)
            st.plotly_chart(fig_m, use_container_width=True)
        else:
            st.info("Aún no hay datos para metros perforados.")

        # Tabla corta con semáforo (bolita)
        df_kpi_m = pd.DataFrame(
            [
                {
                    "KPI": "Metros perforados (día)",
                    "Programado_m": round(mp, 2),
                    "Real_Diurno_m": round(mr_d, 2),
                    "Real_Nocturno_m": round(mr_n, 2),
                    "Real_Total_m": round(mr_total, 2),
                    "Eficiencia_pct": round(eff_m, 1),
                    "Semáforo": semaforo_dot(eff_m),
                }
            ]
        )
        st.dataframe(df_kpi_m, use_container_width=True)



        # ¿Existe TNPI por perforación?
        if not df.empty:
            df_perf_tnpi = df[(df.get("Operacion", "") == "Perforación") & (df.get("Tipo", "") == "TNPI")]
            tnpi_perf_h = float(df_perf_tnpi["Horas_Reales"].sum()) if not df_perf_tnpi.empty else 0.0
        else:
            tnpi_perf_h = 0.0

        st.markdown("### TNPI por perforación")
        if tnpi_perf_h > 0:
            st.warning(f"Sí hay TNPI de perforación registrado: **{tnpi_perf_h:.2f} h**.")
        else:
            st.success("No se detecta TNPI de perforación registrado en el día.")

        st.caption("Tip: si registras TNPI por viajes/conexiones, lo verás en su pestaña y también impacta la eficiencia general del día.")


# =====================================================================
# TAB: DETALLE
# =====================================================================
# =====================================================================
# NUEVA TAB: COMPARATIVA DE ETAPAS
# =====================================================================
with tab_comp:
    st.subheader("📊 Comparativa de Etapas")

    # Estiliza select/multiselect para que no se vea con borde rojo (tema oscuro)
    st.markdown(
        """
        <style>
        div[data-baseweb="select"] > div{
            border-color: rgba(255,255,255,0.18) !important;
            box-shadow: none !important;
        }
        div[data-baseweb="select"] > div:focus-within{
            border-color: rgba(255,255,255,0.35) !important;
            box-shadow: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if df.empty:
        st.info("No hay datos disponibles. Por favor, captura algunas actividades primero.")
    else:
        etapas = sorted(df["Etapa"].dropna().unique().tolist())

                # Selector de etapas (sin chips rojos): 2 selectbox
        col_cmp1, col_cmp2 = st.columns(2)
        with col_cmp1:
            etapa_cmp_a = st.selectbox("Etapa A", options=etapas, index=0 if len(etapas)>0 else None, key="etapa_cmp_a")
        with col_cmp2:
            idx_b = 1 if len(etapas)>1 else 0
            etapa_cmp_b = st.selectbox("Etapa B", options=etapas, index=idx_b if len(etapas)>0 else None, key="etapa_cmp_b")

        etapas_seleccionadas = [e for e in [etapa_cmp_a, etapa_cmp_b] if e]

        if not etapas_seleccionadas:
            st.info("Selecciona al menos una etapa.")
        else:
            comparativa_data = []
            for etapa_comp in etapas_seleccionadas:
                df_etapa_comp = df[df["Etapa"] == etapa_comp].copy()

                total_h = float(df_etapa_comp["Horas_Reales"].sum()) if not df_etapa_comp.empty else 0.0
                tp_h = float(df_etapa_comp[df_etapa_comp["Tipo"] == "TP"]["Horas_Reales"].sum()) if not df_etapa_comp.empty else 0.0
                tnpi_h = float(df_etapa_comp[df_etapa_comp["Tipo"] == "TNPI"]["Horas_Reales"].sum()) if not df_etapa_comp.empty else 0.0
                tnp_h = float(df_etapa_comp[df_etapa_comp["Tipo"] == "TNP"]["Horas_Reales"].sum()) if not df_etapa_comp.empty else 0.0
                eff = clamp_0_100(safe_pct(tp_h, total_h)) if total_h > 0 else 0.0

                # Conexiones (si existe df_conn)
                if "Etapa" in df_conn.columns and "Conn_No" in df_conn.columns:
                    conexiones = int(df_conn[df_conn["Etapa"] == etapa_comp]["Conn_No"].nunique()) if not df_conn.empty else 0
                else:
                    conexiones = 0

                comparativa_data.append(
                    {
                        "Etapa": etapa_comp,
                        "Horas Totales": total_h,
                        "TP (h)": tp_h,
                        "TNPI (h)": tnpi_h,
                        "TNP (h)": tnp_h,
                        "Eficiencia %": eff,
                        "Conexiones": conexiones,
                    }
                )

            df_grafica = pd.DataFrame(comparativa_data)

            if show_charts:
                # Barras: Eficiencia
                fig_comp = px.bar(
                    df_grafica,
                    x="Etapa",
                    y="Eficiencia %",
                    title="Comparativa de Eficiencia por Etapa",
                    text="Eficiencia %",
                    color="Eficiencia %",
                    color_continuous_scale=["#E74C3C", "#F1C40F", "#2ECC71"],
                )
                fig_comp.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_comp.update_layout(height=420, coloraxis_showscale=False)
                st.plotly_chart(fig_comp, use_container_width=True)

                # Radar (se mantiene) + alternativa “más pro”: Heatmap normalizado
                categorias = ["Horas Totales", "TP (h)", "TNPI (h)", "TNP (h)", "Eficiencia %", "Conexiones"]

                if len(etapas_seleccionadas) <= 5:
                    fig_radar = go.Figure()
                    for etapa_comp in etapas_seleccionadas:
                        row = df_grafica[df_grafica["Etapa"] == etapa_comp].iloc[0]
                        vals = []
                        for cat in categorias:
                            v = float(row[cat])
                            if cat == "Eficiencia %":
                                vals.append(v)
                            else:
                                vmax = float(df_grafica[cat].max()) if float(df_grafica[cat].max()) > 0 else 1.0
                                vals.append((v / vmax) * 100.0)
                        fig_radar.add_trace(
                            go.Scatterpolar(
                                r=vals,
                                theta=categorias,
                                fill="toself",
                                name=str(etapa_comp),
                                opacity=0.35,
                            )
                        )
                    fig_radar.update_layout(
                        title="Radar comparativo (normalizado 0–100)",
                        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                        height=520,
                        legend_title_text="Etapa",
                    )
                    st.plotly_chart(fig_radar, use_container_width=True)
                else:
                    st.info("Radar oculto: seleccionaste más de 5 etapas.")

                # Heatmap “pro” (misma info, más legible para muchas etapas)
                df_hm = df_grafica.set_index("Etapa")[categorias].copy()

                # Normaliza columnas a 0-100 (Eficiencia ya está en 0-100)
                for col in df_hm.columns:
                    if col == "Eficiencia %":
                        df_hm[col] = df_hm[col].astype(float).clip(0, 100)
                    else:
                        vmax = float(df_hm[col].max()) if float(df_hm[col].max()) > 0 else 1.0
                        df_hm[col] = (df_hm[col].astype(float) / vmax) * 100.0

                fig_hm = go.Figure(
                    data=go.Heatmap(
                        z=df_hm.values,
                        x=df_hm.columns.tolist(),
                        y=df_hm.index.tolist(),
                        colorbar=dict(title="0–100"),
                    )
                )
                fig_hm.update_layout(
                    title="Comparativo normalizado (heatmap 0–100)",
                    height=420 + (18 * len(df_hm.index)),
                    margin=dict(l=20, r=20, t=60, b=20),
                )
                st.plotly_chart(fig_hm, use_container_width=True)

                # --- Análisis TNP (Comparativo por etapas) ---
                st.markdown("### 🔵 Análisis de TNP (comparativo)")
                df_cmp_sel = df[df["Etapa"].isin(etapas_seleccionadas)].copy()
                df_tnp_cmp = df_cmp_sel[df_cmp_sel["Tipo"] == "TNP"].copy()

                if df_tnp_cmp.empty:
                    st.info("No hay registros TNP en las etapas seleccionadas.")
                else:
                    # Normaliza etiquetas para evitar NaN / '-'
                    for c, fb in [("Categoria_TNP", "Sin categoría"), ("Detalle_TNP", "Sin detalle")]:
                        if c not in df_tnp_cmp.columns:
                            df_tnp_cmp[c] = fb
                        df_tnp_cmp[c] = (
                            df_tnp_cmp[c]
                            .astype(str)
                            .replace({"nan": fb, "None": fb, "-": fb, "": fb})
                            .fillna(fb)
                        )

                    df_tnp_cat = (
                        df_tnp_cmp.groupby(["Etapa", "Categoria_TNP"], as_index=False)["Horas_Reales"]
                        .sum()
                        .sort_values(["Etapa", "Horas_Reales"], ascending=[True, False])
                    )
                    fig_tnp_cat = px.bar(
                        df_tnp_cat,
                        x="Horas_Reales",
                        y="Etapa",
                        color="Categoria_TNP",
                        orientation="h",
                        title="TNP por categoría y etapa (h)",
                    )
                    st.plotly_chart(fig_tnp_cat, use_container_width=True)

                    df_tnp_det = (
                        df_tnp_cmp.groupby(["Detalle_TNP"], as_index=False)["Horas_Reales"]
                        .sum()
                        .sort_values("Horas_Reales", ascending=False)
                        .head(10)
                    )
                    fig_tnp_det = px.bar(
                        df_tnp_det,
                        x="Horas_Reales",
                        y="Detalle_TNP",
                        orientation="h",
                        title="Top 10 - Detalles TNP (h)",
                    )
                    st.plotly_chart(fig_tnp_det, use_container_width=True)


            # Tabla resumen
            st.dataframe(
                df_grafica.sort_values("Etapa"),
                use_container_width=True,
                hide_index=True,
            )

with tab_viajes:
    st.subheader("Viajes y conexiones de TP")

    # --- FILTRO DE ETAPA (Viajes y conexiones) ---
    _df_main = st.session_state.df
    _etapas_v = sorted(_df_main["Etapa"].dropna().unique().tolist()) if (not _df_main.empty and "Etapa" in _df_main.columns) else []
    etapa_viajes_sel = st.selectbox(
        "Etapa para viajes",
        options=_etapas_v,
        index=0 if _etapas_v else None,
        help="Filtra la vista/registro de viajes por etapa."
    ) if _etapas_v else None


    if "viajes_hourly_store" not in st.session_state:
        # Store por tipo de viaje (actividad)
        st.session_state["viajes_hourly_store"] = {}

    colA, colB, colC = st.columns([1.4, 1.0, 1.0])

    with colA:
        viaje_tipo = st.selectbox(
            "Tipo de viaje",
            options=sorted(list(VIAJE_CATALOG.keys())) if "VIAJE_CATALOG" in globals() else [],
            help="Selecciona el tipo de viaje (catálogo de objetivos)."
        )

    # Standards por catálogo
    vel_std = float(VIAJE_CATALOG.get(viaje_tipo, {}).get("vel_mh", 0.0)) if viaje_tipo else 0.0
    tconn_std = float(VIAJE_CATALOG.get(viaje_tipo, {}).get("tconn_min", 0.0)) if viaje_tipo else 0.0

    
    # Aliases (compatibilidad con bloques de cálculo/registro)
    v_std_mh = vel_std
    conn_std_min = tconn_std
    with colB:
        considerar_conexion = st.toggle(
            "Considerar tiempo de conexión",
            value=True,
            help="Si lo apagas, se omite el KPI de conexiones (solo viaje)."
        )

    with colC:
        distancia_manual = st.number_input(
            "Longitud (m) (opcional)",
            min_value=0.0,
            step=1.0,
            value=float(st.session_state.get("viaje_distancia_m", 0.0) or 0.0),
            help="Si importas KPIs, la longitud se calcula automáticamente; aquí puedes ajustar manual."
        )
        st.session_state["viaje_distancia_m"] = float(distancia_manual)

    st.caption(f"**Estándar:** {vel_std:.0f} m/h | **Conexión estándar:** {tconn_std:.1f} min")


    # ------------------------------
    # CORTE DE TURNOS (editable)
    # ------------------------------
    with st.expander("Corte de turnos (para colorear Día/Noche)", expanded=False):
        cts1, cts2 = st.columns(2)
        with cts1:
            day_start = st.number_input(
                "Inicio turno Día (hora 0–23)",
                min_value=0, max_value=23,
                value=int(st.session_state.get("day_start", 6)),
                step=1,
                key="viajes_day_start",
            )
        with cts2:
            day_end = st.number_input(
                "Fin turno Día (hora 0–23)",
                min_value=0, max_value=23,
                value=int(st.session_state.get("day_end", 18)),
                step=1,
                key="viajes_day_end",
            )
        st.session_state["day_start"] = int(day_start)
        st.session_state["day_end"] = int(day_end)
        st.caption(
            "Regla: Día si la hora está entre Inicio (incl.) y Fin (excl.). "
            "Si Inicio > Fin, se asume que el turno Día cruza medianoche."
        )

    # ------------------------------
    # IMPORTAR KPIs DrillSpot
    # ------------------------------
    with st.expander("Importar KPIs de DrillSpot (XLSX) para autocalcular por hora", expanded=False):
        up_kpi = st.file_uploader("Sube el export de KPIs (XLSX)", type=["xlsx"], key="kpi_xlsx_viajes")

        direction_default = default_trip_direction_from_activity(viaje_tipo) if viaje_tipo else "Trip In"
        direction = st.selectbox("Dirección para el cálculo", options=["Trip In", "Trip Out"], index=0 if direction_default == "Trip In" else 1)

        if st.button("Calcular automáticamente desde el XLSX", use_container_width=True, disabled=(up_kpi is None or not viaje_tipo)):
            df_kpi = load_drillspot_kpi_xlsx(up_kpi)
            hourly_df, meta = compute_viaje_conn_hourly_from_kpi(df_kpi, direction=direction)

            # Guarda en session por tipo de viaje
            st.session_state["viajes_hourly_store"][viaje_tipo] = {
                "hourly": hourly_df,
                "meta": meta,
                "direction": direction,
                "considerar_conexion": considerar_conexion,
            }
            # Si hay longitud del KPI, úsala (pero permite ajustar)
            if meta.get("distance_m_total", 0.0) > 0:
                st.session_state["viaje_distancia_m"] = float(meta["distance_m_total"])
            st.success("KPIs importados y calculados ✅ (puedes editar manualmente abajo)")

    # ------------------------------
    # DATA MANUAL / EDITABLE
    # ------------------------------
    store = st.session_state["viajes_hourly_store"].get(viaje_tipo, {})
    hourly_df = store.get("hourly")
    meta = store.get("meta", {}) if isinstance(store, dict) else {}

    if hourly_df is None or not isinstance(hourly_df, pd.DataFrame) or hourly_df.empty:
        hourly_df = pd.DataFrame({"hour": list(range(24)), "speed_mh": [0.0]*24, "conn_min": [0.0]*24})

    st.markdown("### Carga manual (por hora)")
    st.caption("Ingresa la **velocidad promedio (m/h)** por hora y (opcional) el **tiempo de conexión promedio (min)** por hora. "
               "Si importaste el XLSX, aquí podrás ajustar valores puntuales.")

    editable = hourly_df.copy()
    editable = editable.sort_values("hour").reset_index(drop=True)
    editable.rename(columns={"hour": "Hora", "speed_mh": "Velocidad real (m/h)", "conn_min": "Conexión real (min)"}, inplace=True)

    edited = st.data_editor(
        editable,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Hora": st.column_config.NumberColumn("Hora", min_value=0, max_value=23, step=1, disabled=True),
            "Velocidad real (m/h)": st.column_config.NumberColumn("Velocidad real (m/h)", min_value=0.0, step=1.0),
            "Conexión real (min)": st.column_config.NumberColumn("Conexión real (min)", min_value=0.0, step=0.1),
        },
        num_rows="fixed"
    )

    csave1, csave2 = st.columns([1, 1])
    with csave1:
        if st.button("Guardar ajustes manuales", use_container_width=True, disabled=(not viaje_tipo)):
            h2 = edited.rename(columns={"Hora": "hour", "Velocidad real (m/h)": "speed_mh", "Conexión real (min)": "conn_min"}).copy()
            h2["hour"] = h2["hour"].astype(int)
            for c in ["speed_mh", "conn_min"]:
                h2[c] = pd.to_numeric(h2[c], errors="coerce").fillna(0.0)

            st.session_state["viajes_hourly_store"][viaje_tipo] = {
                "hourly": h2,
                "meta": meta,
                "direction": store.get("direction", default_trip_direction_from_activity(viaje_tipo)),
                "considerar_conexion": considerar_conexion,
            }
            st.success("Ajustes guardados ✅")

    with csave2:
        if st.button("Limpiar (poner en cero)", use_container_width=True, disabled=(not viaje_tipo)):
            h2 = pd.DataFrame({"hour": list(range(24)), "speed_mh": [0.0]*24, "conn_min": [0.0]*24})
            st.session_state["viajes_hourly_store"][viaje_tipo] = {
                "hourly": h2,
                "meta": {},
                "direction": store.get("direction", default_trip_direction_from_activity(viaje_tipo)),
                "considerar_conexion": considerar_conexion,
            }
            st.success("Valores reiniciados ✅")
            st.rerun()

    # Recupera la versión guardada (después de edición)
    store = st.session_state["viajes_hourly_store"].get(viaje_tipo, {})
    hourly_df = store.get("hourly", pd.DataFrame({"hour": list(range(24)), "speed_mh": [0.0]*24, "conn_min": [0.0]*24}))
    hourly_df = hourly_df.sort_values("hour").reset_index(drop=True)

    # ------------------------------
    # ESTÁNDAR VARIABLE POR HORA (OPCIONAL)
    # ------------------------------
    usar_std_variable = st.checkbox(
        "Estándar variable por hora (opcional)",
        value=bool(st.session_state.get(f"viaje_std_var_{viaje_tipo}", False)),
        key=f"viaje_std_var_{viaje_tipo}",
        help="Actívalo solo cuando el estándar cambie durante el viaje (por tramo / lingadas vs TxT, etc.). "
             "Si está apagado, se usa el estándar general (línea roja fija) como está hoy."
    )

    std_hourly_df = None
    if usar_std_variable and viaje_tipo:
        st.caption("Edita el estándar por hora. Esto NO reemplaza tu estándar general; solo se usa si activas este modo.")
        std_store = store.get("std_hourly")
        if std_store is None or not isinstance(std_store, pd.DataFrame) or std_store.empty:
            std_store = pd.DataFrame({
                "hour": list(range(24)),
                "std_speed_mh": [float(v_std_mh or 0.0)] * 24,
                "std_conn_min": [float(tconn_std or 0.0)] * 24,
                "conn_count": [0] * 24,
            })

        std_edit = std_store.copy().sort_values("hour").reset_index(drop=True)
        std_edit.rename(columns={
            "hour": "Hora",
            "std_speed_mh": "Estándar velocidad (m/h)",
            "std_conn_min": "Estándar conexión (min)",
            "conn_count": "Conexiones (#) en la hora",
        }, inplace=True)

        std_edited = st.data_editor(
            std_edit,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Hora": st.column_config.NumberColumn("Hora", min_value=0, max_value=23, step=1, disabled=True),
                "Estándar velocidad (m/h)": st.column_config.NumberColumn("Estándar velocidad (m/h)", min_value=0.0, step=1.0),
                "Estándar conexión (min)": st.column_config.NumberColumn("Estándar conexión (min)", min_value=0.0, step=0.1),
                "Conexiones (#) en la hora": st.column_config.NumberColumn("Conexiones (#) en la hora", min_value=0, step=1),
            },
        )

        cstd1, cstd2 = st.columns(2)
        with cstd1:
            if st.button("Guardar estándar por hora", use_container_width=True, disabled=(not viaje_tipo)):
                s2 = std_edited.copy()
                s2.rename(columns={
                    "Hora": "hour",
                    "Estándar velocidad (m/h)": "std_speed_mh",
                    "Estándar conexión (min)": "std_conn_min",
                    "Conexiones (#) en la hora": "conn_count",
                }, inplace=True)
                s2["hour"] = s2["hour"].astype(int)
                for c in ["std_speed_mh", "std_conn_min"]:
                    s2[c] = pd.to_numeric(s2[c], errors="coerce").fillna(0.0)
                s2["conn_count"] = pd.to_numeric(s2["conn_count"], errors="coerce").fillna(0).astype(int)

                # Persistimos junto con el store del viaje
                st.session_state["viajes_hourly_store"][viaje_tipo] = {
                    "hourly": hourly_df,
                    "std_hourly": s2,
                    "meta": meta,
                    "direction": store.get("direction", default_trip_direction_from_activity(viaje_tipo)),
                    "considerar_conexion": considerar_conexion,
                }
                st.success("Estándar por hora guardado ✅")
                st.rerun()

        with cstd2:
            if st.button("Reset estándar por hora", use_container_width=True, disabled=(not viaje_tipo)):
                s2 = pd.DataFrame({
                    "hour": list(range(24)),
                    "std_speed_mh": [float(v_std_mh or 0.0)] * 24,
                    "std_conn_min": [float(tconn_std or 0.0)] * 24,
                    "conn_count": [0] * 24,
                })
                st.session_state["viajes_hourly_store"][viaje_tipo] = {
                    "hourly": hourly_df,
                    "std_hourly": s2,
                    "meta": meta,
                    "direction": store.get("direction", default_trip_direction_from_activity(viaje_tipo)),
                    "considerar_conexion": considerar_conexion,
                }
                st.success("Estándar por hora reiniciado ✅")
                st.rerun()

        # Recarga (después de guardar/reset)
        store = st.session_state["viajes_hourly_store"].get(viaje_tipo, {})
        std_hourly_df = store.get("std_hourly")
        if std_hourly_df is not None and isinstance(std_hourly_df, pd.DataFrame) and not std_hourly_df.empty:
            std_hourly_df = std_hourly_df.sort_values("hour").reset_index(drop=True)


    # ------------------------------
    # GRÁFICAS
    # ------------------------------
    st.divider()
    st.markdown("### Gráficas")

    df_plot = hourly_df.copy()
    df_plot["hour_str"] = df_plot["hour"].astype(int)

    # Turno por hora (para colores Día/Noche)
    day_start = int(st.session_state.get("day_start", 6))
    day_end = int(st.session_state.get("day_end", 18))

    def _is_day(h: int) -> bool:
        if day_start == day_end:
            return True  # todo el día (caso extremo)
        if day_start < day_end:
            return day_start <= h < day_end
        # Cruza medianoche
        return (h >= day_start) or (h < day_end)

    df_plot["Turno"] = df_plot["hour"].astype(int).apply(lambda h: "Día" if _is_day(h) else "Noche")


    fig_v = px.bar(
        df_plot,
        x="hour_str",
        y="speed_mh",
        color="Turno",
        color_discrete_map={"Día": "#1f77b4", "Noche": "#ff7f0e"},
        labels={"hour_str": "Hora", "speed_mh": "m/h", "Turno": "Turno"},
        title=f"Viaje – {viaje_tipo}"
    )
    if usar_std_variable and std_hourly_df is not None and isinstance(std_hourly_df, pd.DataFrame) and not std_hourly_df.empty:
        # Línea estándar variable (por hora)
        _s = std_hourly_df.copy()
        _s["hour_str"] = _s["hour"].astype(int)
        fig_v.add_scatter(
            x=_s["hour_str"],
            y=_s["std_speed_mh"],
            mode="lines",
            name="Estándar",
            line=dict(dash="dash", color="red"),
        )
    elif vel_std > 0:
        fig_v.add_hline(
            y=vel_std,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Estándar {vel_std:.0f}",
            annotation_position="top left",
        )
    fig_v.update_layout(showlegend=True, legend_title_text='', xaxis=dict(dtick=1))
    st.plotly_chart(fig_v, use_container_width=True)

    if considerar_conexion:
        fig_c = px.bar(
            df_plot,
            x="hour_str",
            y="conn_min",
            color="Turno",
            color_discrete_map={"Día": "#1f77b4", "Noche": "#ff7f0e"},
            labels={"hour_str": "Hora", "conn_min": "min", "Turno": "Turno"},
            title=f"Conexiones – {viaje_tipo}"
        )
        if usar_std_variable and std_hourly_df is not None and isinstance(std_hourly_df, pd.DataFrame) and not std_hourly_df.empty:
            _s = std_hourly_df.copy()
            _s["hour_str"] = _s["hour"].astype(int)
            fig_c.add_scatter(
                x=_s["hour_str"],
                y=_s["std_conn_min"],
                mode="lines",
                name="Estándar",
                line=dict(dash="dash", color="red"),
            )
        elif tconn_std > 0:
            fig_c.add_hline(
                y=tconn_std,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Estándar {tconn_std:.1f}",
                annotation_position="top left",
            )

        fig_c.update_layout(showlegend=True, legend_title_text='', xaxis=dict(dtick=1))
        st.plotly_chart(fig_c, use_container_width=True)

    # ------------------------------
    # RESUMEN (TABLA)
    # ------------------------------
    st.markdown("### Resumen")
    dist = float(st.session_state.get("viaje_distancia_m", 0.0) or 0.0)
    if isinstance(meta, dict) and meta.get("distance_m_total", 0.0) and dist <= 0:
        dist = float(meta.get("distance_m_total", 0.0))

    # Real promedio (sobre horas con dato > 0)
    speed_vals = hourly_df["speed_mh"].astype(float)
    speed_real = float(speed_vals[speed_vals > 0].mean()) if (speed_vals > 0).any() else 0.0

    conn_vals = hourly_df["conn_min"].astype(float)
    conn_real = float(conn_vals[conn_vals > 0].mean()) if (conn_vals > 0).any() else 0.0

    

    # Aliases (compatibilidad con lógica TNPI/registro)
    v_real_mh = speed_real
    conn_real_min = float(conn_real or 0.0)
    sum_df = pd.DataFrame([{
        "Tipo de viaje": viaje_tipo or "-",
        "Longitud (m)": dist if dist > 0 else "-",
        "Estándar (m/h)": vel_std if vel_std > 0 else "-",
        "Real (m/h)": round(speed_real, 1) if speed_real > 0 else "-",
        "Estándar (min)": tconn_std if (considerar_conexion and tconn_std > 0) else "-",
        "Real (min)": round(conn_real, 2) if (considerar_conexion and conn_real > 0) else "-",
    }])

    st.dataframe(sum_df, use_container_width=True, hide_index=True)


    # ------------------------------
    # REGISTRO EN ACTIVIDADES (para que cuente en TNPI / distribución / detalle)
    # ------------------------------
    st.markdown("### Registro en actividades")

    # Conexiones totales (para convertir min/conn a horas)
    n_conn_total_default = int(st.session_state.get("viaje_n_conn", 0) or 0)

    with st.expander("Configurar registro (opcional)", expanded=False):
        col_r1, col_r2, col_r3 = st.columns([1, 1, 1])
        with col_r1:
            turno_viaje = st.radio("Turno del viaje", options=["Diurno", "Nocturno"], horizontal=True, key=f"viaje_turno_{viaje_tipo}")
        with col_r2:
            n_conn_total = st.number_input(
                "Conexiones totales (#)",
                min_value=0,
                step=1,
                value=n_conn_total_default,
                key=f"viaje_nconn_total_{viaje_tipo}"
            )
        
        conexiones_total = int(n_conn_total or 0)
        with col_r3:
            actividad_nombre = st.text_input(
                "Nombre en actividades",
                value=(viaje_tipo if (viaje_tipo or '').lower().startswith('viaje') else f"Viaje {viaje_tipo}") if viaje_tipo else "Viaje",
                key=f"viaje_actname_{viaje_tipo}"
            )

        st.caption("El cálculo de horas usa: Horas = Distancia/Velocidad + (#Conexiones × min/conexión)/60 (si está habilitado).")

        # Permite override de horas reales si no hay suficientes datos
        auto_real_h = 0.0
        if dist > 0 and speed_real > 0:
            auto_real_h = dist / speed_real
            if considerar_conexion and n_conn_total and conn_real > 0:
                auto_real_h += (float(n_conn_total) * float(conn_real) / 60.0)

        horas_reales_override = st.number_input(
            "Horas reales (override, opcional)",
            min_value=0.0,
            step=0.1,
            value=float(auto_real_h) if auto_real_h > 0 else 0.0,
            key=f"viaje_realh_override_{viaje_tipo}",
            help="Si no quieres usar el cálculo automático (por velocidad), escribe aquí las horas reales totales del viaje."
        )

        # Categoría/detalle para TNPI si aplica
        cat_opts = (cat_list if 'cat_list' in globals() else ["-"])
        categoria_viaje = st.selectbox(
            "Categoría TNPI (si aplica)",
            options=cat_opts,
            index=0,
            key=f"viaje_cat_{viaje_tipo}"
        )
        detalle_viaje = st.text_input(
            "Detalle TNPI (si aplica)",
            value="",
            key=f"viaje_det_{viaje_tipo}"
        )
        comentario_viaje = st.text_input(
            "Comentario (opcional)",
            value="",
            key=f"viaje_com_{viaje_tipo}"
        )

    # Horas estándar (desde catálogo) y reales (auto/override)
    n_conn_used = int(st.session_state.get(f"viaje_nconn_total_{viaje_tipo}", n_conn_total_default) or 0)

    # ------------------------------
    # Cálculos (estándar/real/TNPI) para registro
    # ------------------------------
    # Si NO está activado estándar variable por hora: usamos el estándar general (línea roja fija) como hasta ahora.
    # Si SÍ está activado: usamos std_hourly_df (por hora) para calcular estándar/real y TNPI por velocidad + conexiones.

    tnpi_vel_h = 0.0
    tnpi_conn_h = 0.0
    std_h_viaje = 0.0
    std_h_conn = 0.0
    real_h_viaje = 0.0
    real_h_conn = 0.0

    if usar_std_variable and std_hourly_df is not None and isinstance(std_hourly_df, pd.DataFrame) and not std_hourly_df.empty:
        # --- Viaje por horas (velocidad) ---
        _h = hourly_df.copy().sort_values("hour").reset_index(drop=True)
        _s = std_hourly_df.copy().sort_values("hour").reset_index(drop=True)

        # merge por hora
        _m = pd.merge(_h, _s, on="hour", how="left")
        _m["speed_mh"] = pd.to_numeric(_m["speed_mh"], errors="coerce").fillna(0.0)
        _m["std_speed_mh"] = pd.to_numeric(_m["std_speed_mh"], errors="coerce").fillna(0.0)

        # distancia por hora inferida (m). Si no cuadra con dist, escalamos para que sume "dist".
        _m["dist_h"] = _m["speed_mh"].clip(lower=0.0)  # m/h * 1h
        dist_infer = float(_m["dist_h"].sum() or 0.0)
        dist_obj = float(dist or 0.0)

        if dist_obj > 0 and dist_infer > 0:
            factor = dist_obj / dist_infer
            _m["dist_h"] = _m["dist_h"] * factor
        elif dist_obj > 0 and dist_infer == 0:
            # Sin distribución por hora: cae al método global
            _m["dist_h"] = 0.0

        # tiempo real por hora = dist_h / v_real (si v_real>0)
        _m["t_real_h"] = 0.0
        mask_vr = _m["speed_mh"] > 0
        _m.loc[mask_vr, "t_real_h"] = _m.loc[mask_vr, "dist_h"] / _m.loc[mask_vr, "speed_mh"]

        # tiempo estándar por hora = dist_h / v_std (si v_std>0)
        _m["t_std_h"] = 0.0
        mask_vs = _m["std_speed_mh"] > 0
        _m.loc[mask_vs, "t_std_h"] = _m.loc[mask_vs, "dist_h"] / _m.loc[mask_vs, "std_speed_mh"]

        # TNPI por velocidad (solo si real > std)
        tnpi_vel_h = float((_m["t_real_h"] - _m["t_std_h"]).clip(lower=0.0).sum() or 0.0)

        std_h_viaje = float(_m["t_std_h"].sum() or 0.0)
        real_h_viaje = float(_m["t_real_h"].sum() or 0.0)

        # --- Conexiones por horas ---
        _m["conn_min"] = pd.to_numeric(_m["conn_min"], errors="coerce").fillna(0.0)
        _m["std_conn_min"] = pd.to_numeric(_m["std_conn_min"], errors="coerce").fillna(0.0)
        _m["conn_count"] = pd.to_numeric(_m["conn_count"], errors="coerce").fillna(0).astype(int)

        real_h_conn = float((_m["conn_min"] * _m["conn_count"]).sum() / 60.0) if considerar_conexion else 0.0
        std_h_conn = float((_m["std_conn_min"] * _m["conn_count"]).sum() / 60.0) if considerar_conexion else 0.0

        tnpi_conn_h = max(0.0, real_h_conn - std_h_conn) if considerar_conexion else 0.0

        std_h = std_h_viaje + (std_h_conn if considerar_conexion else 0.0)
        real_h = real_h_viaje + (real_h_conn if considerar_conexion else 0.0)
        tnpi_h = tnpi_vel_h + tnpi_conn_h
        tp_h = max(0.0, real_h - tnpi_h)

    else:
        # --- Estándar global (como estaba) ---
        std_h = 0.0
        if dist > 0 and vel_std > 0:
            std_h = dist / vel_std
            if considerar_conexion and n_conn_used and tconn_std > 0:
                std_h += (float(n_conn_used) * float(tconn_std) / 60.0)

        real_h = float(st.session_state.get(f"viaje_realh_override_{viaje_tipo}", 0.0) or 0.0)
        if real_h <= 0 and dist > 0 and speed_real > 0:
            real_h = dist / speed_real
            if considerar_conexion and n_conn_used and conn_real > 0:
                real_h += (float(n_conn_used) * float(conn_real) / 60.0)


        # Componentes (global)
        std_h_viaje = (dist / vel_std) if (dist > 0 and vel_std > 0) else 0.0
        real_h_viaje = (dist / speed_real) if (dist > 0 and speed_real > 0) else 0.0
        std_h_conn = (float(n_conn_used) * float(tconn_std) / 60.0) if (considerar_conexion and n_conn_used and tconn_std > 0) else 0.0
        real_h_conn = (float(n_conn_used) * float(conn_real) / 60.0) if (considerar_conexion and n_conn_used and conn_real > 0) else 0.0

        # TNPI por velocidad (solo si v_real < v_std) y TNPI por conexiones (si aplica)
        tnpi_vel_h = max(0.0, (dist / speed_real) - (dist / vel_std)) if (dist > 0 and speed_real > 0 and vel_std > 0) else 0.0
        tnpi_conn_h = max(0.0, ((float(n_conn_used) * float(conn_real) / 60.0) - (float(n_conn_used) * float(tconn_std) / 60.0))) if (considerar_conexion and n_conn_used and conn_real > 0 and tconn_std > 0) else 0.0
        tnpi_h = tnpi_vel_h + (tnpi_conn_h if considerar_conexion else 0.0)

        tp_h = max(0.0, real_h - tnpi_h)



    cM1, cM2, cM3 = st.columns(3)
    cM1.metric("Estándar (h)", f"{std_h:.2f}")
    cM2.metric("Real (h)", f"{real_h:.2f}")
    cM3.metric("TNPI por exceso (h)", f"{tnpi_h:.2f}")

    # Botón para registrar en el DataFrame principal (st.session_state.df)
    # Decide si al registrar quieres separar automáticamente el exceso como TNPI (sin perder el estándar general).
    auto_tnpi_por_desempeno = st.toggle(
        "Registrar TNPI automáticamente (exceso vs estándar)",
        value=True,
        help="Si está activo: se registra TP hasta el estándar y el exceso como TNPI. Si está apagado: se registra una sola fila con el tipo seleccionado (TP/TNPI/TNP)."
    )

    # Cuando el usuario quiere capturar TNPI de viajes de forma manual (p. ej. causas exógenas)
    # puede que no haya estándar calculable (std_h = 0) o que el TNPI por desempeño resulte 0.
    # En esos casos, este selector asegura que el registro se guarde como TNPI/TNP según corresponda.
    tipo_time_viaje = st.selectbox(
        "Tipo de tiempo a registrar (si no hay TNPI automático)",
        ["TP", "TNPI", "TNP"],
        index=1,
        key="tipo_time_viaje",
        help="Si el TNPI por desempeño sale 0 (o no hay estándar), selecciona TNPI para contabilizarlo en causa–raíz."
    )

    comp_tnpi_viaje = st.selectbox(
        "Componente TNPI (Viajes)",
        ["Velocidad", "Conexiones", "Otro"],
        index=0,
        key="comp_tnpi_viaje",
        help="Usado para graficar/desglosar TNPI de viajes en el tab Ejecutivo cuando el registro es manual (sin TNPI automático)."
    )

    if st.button("Registrar este viaje en actividades", use_container_width=True):
        # Validaciones básicas
        if float(real_h or 0.0) <= 0.0:
            st.warning("No hay horas para registrar (revisa longitud, velocidades y/o conexiones).")
        else:
            # TNPI calculado por desempeño (exceso en tiempo por velocidad + exceso en tiempo por conexiones)
            _tnpi_total_h = float(max(0.0, (tnpi_vel_h or 0.0) + (tnpi_conn_h or 0.0)))
            _std_h = float(std_h or 0.0)
            _real_h = float(real_h or 0.0)
            _tp_h = float(max(0.0, _real_h - _tnpi_total_h))

            # Estándares (para trazabilidad en el registro)
            # Aliases para compatibilidad (evita NameError si cambian nombres)
            conn_std = tconn_std
            conn_real = conn_real_min
            _std_speed_mh = float(v_std_mh or 0.0)
            _real_speed_mh = float(v_real_mh or 0.0)
            try:
                _std_conn_min = float(conn_std or 0.0)
            except Exception:
                _std_conn_min = 0.0
            try:
                _real_conn_min = float(conn_real or 0.0)
            except Exception:
                _real_conn_min = 0.0

            # Base común del registro (mismo esquema que el registro general)
            #
            # IMPORTANTE:
            # - El tab "Ejecutivo" filtra TNPI de viajes por Origen.
            # - Para evitar ceros por mismatch de etiquetas, usamos siempre:
            #     Origen = "Viajes y conexiones"
            # - Además, cuando se calcula TNPI automático por desempeño,
            #   registramos TNPI separado por componente (Velocidad / Conexiones)
            #   para que el executive pueda desglosarlo.
            _base = {
                "Equipo": equipo,
                "Pozo": pozo,
                "Etapa": ((etapa_viajes_sel or etapa) if "etapa_viajes_sel" in globals() else etapa),
                "Fecha": fecha,
                "Equipo_Tipo": st.session_state.get("equipo_tipo_val", ""),
                "Seccion": etapa,
                "Corrida": corrida,
                "Tipo_Agujero": tipo_agujero,
                "Operacion": operacion,
                "Turno": turno_registro if "turno_registro" in locals() else turno,
                "Actividad": actividad_registro if "actividad_registro" in locals() else "Viaje",
                "Detalle_TNPI": detalle_registro if "detalle_registro" in locals() else "",
                "Categoria_TNPI": categoria_tnpi_registro if "categoria_tnpi_registro" in locals() else "",
                "Origen": "Viajes y conexiones",
                "Longitud_m": float(dist or 0.0),
                "std_speed_mh": _std_speed_mh,
                "real_speed_mh": _real_speed_mh,
                "std_conn_min": _std_conn_min,
                "real_conn_min": _real_conn_min,
            }

            _rows = []
            if auto_tnpi_por_desempeno and _tnpi_total_h > 0.0:
                # 1) Parte productiva (TP) hasta el estándar
                _rows.append({
                    **_base,
                    "Tipo": "TP",
                    "Horas_Prog": _std_h,
                    "Horas_Reales": _tp_h,
                    "TP_h": _tp_h,
                    "TNPI_h": 0.0,
                    "TNP_h": 0.0,
                    "ROP_mh": _real_speed_mh,
                })
                # 2) Exceso como TNPI, separado por componente
                #    (esto habilita el desglose Velocidad vs Conexiones en el tab Ejecutivo)
                _detalle_user = (detalle_registro if "detalle_registro" in locals() else "").strip()
                _cat_user = (categoria_tnpi_registro if "categoria_tnpi_registro" in locals() else "").strip()

                if float(tnpi_vel_h or 0.0) > 0.0:
                    _rows.append({
                        **_base,
                        "Tipo": "TNPI",
                        "Categoria_TNPI": _cat_user,
                        "Detalle_TNPI": f"Velocidad - {_detalle_user}" if _detalle_user else "Velocidad",
                        "Horas_Prog": 0.0,
                        "Horas_Reales": float(tnpi_vel_h),
                        "TP_h": 0.0,
                        "TNPI_h": float(tnpi_vel_h),
                        "TNP_h": 0.0,
                        "ROP_mh": 0.0,
                    })
                if float(tnpi_conn_h or 0.0) > 0.0:
                    _rows.append({
                        **_base,
                        "Tipo": "TNPI",
                        "Categoria_TNPI": _cat_user,
                        "Detalle_TNPI": f"Conexiones - {_detalle_user}" if _detalle_user else "Conexiones",
                        "Horas_Prog": 0.0,
                        "Horas_Reales": float(tnpi_conn_h),
                        "TP_h": 0.0,
                        "TNPI_h": float(tnpi_conn_h),
                        "TNP_h": 0.0,
                        "ROP_mh": 0.0,
                    })
            else:
                # Registro tradicional: una sola fila con el tipo elegido
                _tipo = st.session_state.get("tipo_time_viaje", "TNPI")
                
                # Registro tradicional: una sola fila con el tipo elegido
                _tipo = st.session_state.get("tipo_time_viaje", "TNPI")

                # Para registros manuales de TNPI (sin TNPI automático), prefijamos el detalle con el componente
                # para que el tab Ejecutivo pueda desglosar y graficar (Velocidad vs Conexiones).
                _base_row = dict(_base)
                if _tipo == "TNPI":
                    _comp = st.session_state.get("comp_tnpi_viaje", "Otro") or "Otro"
                    _det = str(_base_row.get("Detalle_TNPI", "") or "").strip()
                    if _comp in ("Velocidad", "Conexiones"):
                        if _det:
                            if not _det.lower().startswith(_comp.lower()):
                                _det = f"{_comp} - {_det}"
                        else:
                            _det = _comp
                    _base_row["Detalle_TNPI"] = _det

                _rows.append({
                    **_base_row,
                    "Tipo": _tipo,
                    "Horas_Prog": _std_h,
                    "Horas_Reales": _real_h,
                    "TP_h": _real_h if _tipo == "TP" else 0.0,
                    "TNPI_h": _real_h if _tipo == "TNPI" else 0.0,
                    "TNP_h": _real_h if _tipo == "TNP" else 0.0,
                    "ROP_mh": _real_speed_mh if _tipo == "TP" else 0.0,
                })

            if not _rows:
                st.warning("No hay horas para registrar (revisa longitud, velocidades y/o conexiones).")
            else:
                nueva = pd.DataFrame(_rows)
                st.session_state.df = pd.concat([st.session_state.df, nueva], ignore_index=True)
                st.success(f"Registro agregado: {len(_rows)} fila(s).")
                st.rerun()

with tab_bha:
    st.subheader("BHA (Arma/Desarma)")

    df_bha = st.session_state.df_bha
    # --- FILTRO DE ETAPA (BHA) ---
    if (not df_bha.empty) and ("Etapa" in df_bha.columns):
        _etapas_bha = sorted(df_bha["Etapa"].dropna().unique().tolist())
        etapa_bha_sel = st.selectbox(
            "Etapa para BHA",
            options=_etapas_bha,
            index=0 if _etapas_bha else None,
            help="Filtra los registros BHA por etapa."
        ) if _etapas_bha else None
        df_bha = df_bha[df_bha["Etapa"] == etapa_bha_sel] if etapa_bha_sel else df_bha

    if df_bha.empty:
        st.info("Aún no hay registros BHA para graficar.")
    else:
        n_bha = n_max_bha = min(50, len(df_bha))
        if n_max_bha <= 1:
            n_bha = n_max_bha
            st.caption("Mostrando el único registro disponible." if n_bha == 1 else "Sin registros para graficar.")
        else:
            n_bha = st.slider("Últimos registros a graficar", min_value=1, max_value=n_max_bha, value=min(12, n_max_bha))
        df_bha_last = df_bha.tail(n_bha).copy()

        # Eficiencia y semáforo (igual que en otras vistas)
        df_bha_last["Eficiencia_pct"] = df_bha_last.apply(
            lambda r: (float(r.get("Estandar_h", 0.0)) / float(r.get("Real_h", 0.0)) * 100.0) if float(r.get("Real_h", 0.0) or 0.0) > 0 else 0.0,
            axis=1
        )
        df_bha_last["Semáforo"] = df_bha_last["Eficiencia_pct"].apply(semaforo_dot)

        def _bha_label(row):
            try:
                t = int(row.get("BHA_Tipo", 0))
            except Exception:
                t = row.get("BHA_Tipo", "")
            return f"T{t} - {row.get('Accion','')}".strip(" -")

        df_bha_last["Etiqueta"] = df_bha_last.apply(_bha_label, axis=1)

        df_long = df_bha_last.melt(
            id_vars=["Etiqueta"],
            value_vars=["Estandar_h", "Real_h"],
            var_name="Serie",
            value_name="Horas"
        )

        fig_bha = px.bar(
            df_long,
            x="Etiqueta",
            y="Horas",
            color="Serie",
            barmode="group",
            title="BHA: Estándar vs Real (últimos registros)"
        )
        fig_bha.update_layout(xaxis_title="Etiqueta", yaxis_title="Horas", legend_title="Serie")
        fig_bha.update_traces(texttemplate="%{y:.0f}", textposition="inside")
        st.plotly_chart(fig_bha, use_container_width=True)

        st.dataframe(df_bha_last, use_container_width=True, hide_index=True)


with tab_detalle:

    st.markdown("## Edición manual (TNPI/TNP/TP)")
    st.caption("Puedes editar tipo, horas y causas. Al guardar, se actualizan las gráficas automáticamente.")

    df_det = st.session_state.get("df", pd.DataFrame()).copy()
    df_det = _ensure_rowid(df_det)

    if df_det.empty:
        st.info("No hay registros para editar.")
    else:
        with st.expander("Editar registros en tabla (guardar cambios)", expanded=False):
            editable_cols = ["RowID", "Fecha", "Etapa", "Actividad", "Tipo", "Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP", "Horas_Prog", "Horas_Reales", "Comentario"]
            show_cols = [c for c in editable_cols if c in df_det.columns]

            cat_opts = ["-"]
            det_opts = ["-"]
            if "df_tnpi_cat" in globals():
                if "Categoria_TNPI" in df_tnpi_cat.columns:
                    cat_opts = sorted(df_tnpi_cat["Categoria_TNPI"].dropna().unique().tolist())
                if "Detalle_TNPI" in df_tnpi_cat.columns:
                    det_opts = sorted(df_tnpi_cat["Detalle_TNPI"].dropna().unique().tolist())

            edited = st.data_editor(
                df_det[show_cols],
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_config={
                    "Tipo": st.column_config.SelectboxColumn("Tipo", options=["TP", "TNPI", "TNP"]),
                    "Categoria_TNPI": st.column_config.SelectboxColumn("Categoría TNPI", options=cat_opts),
                    "Detalle_TNPI": st.column_config.SelectboxColumn("Detalle TNPI", options=det_opts),
                    "Horas_Prog": st.column_config.NumberColumn("Horas Prog", min_value=0.0, step=0.25, format="%.2f"),
                    "Horas_Reales": st.column_config.NumberColumn("Horas Reales", min_value=0.0, step=0.25, format="%.2f"),
                },
                key="detalle_editor_df",
            )

            if st.button("Guardar cambios (Detalle)", use_container_width=True):
                ed = edited.copy()
                if "Horas_Prog" in ed.columns:
                    ed["Horas_Prog"] = pd.to_numeric(ed["Horas_Prog"], errors="coerce").fillna(0.0)
                if "Horas_Reales" in ed.columns:
                    ed["Horas_Reales"] = pd.to_numeric(ed["Horas_Reales"], errors="coerce").fillna(0.0)

                mask_not_tnpi = ed["Tipo"].astype(str).str.upper() != "TNPI"
                if "Categoria_TNPI" in ed.columns:
                    ed.loc[mask_not_tnpi, "Categoria_TNPI"] = "-"
                if "Detalle_TNPI" in ed.columns:
                    ed.loc[mask_not_tnpi, "Detalle_TNPI"] = "-"

                master = st.session_state.get("df", pd.DataFrame()).copy()
                master = _ensure_rowid(master)
                master = master.set_index("RowID")
                ed2 = ed.set_index("RowID")
                common = [c for c in ed2.columns if c in master.columns]
                master.update(ed2[common])
                master = master.reset_index()
                st.session_state.df = _ensure_rowid(master)
                st.success("Cambios guardados. Las gráficas se actualizaron.")
                st.rerun()

    st.subheader("Detalle de actividades")
    # Eficiencia por fila (si hay estándar): Horas_Prog / Horas_Reales
    df_disp = df.copy()
    if "Horas_Prog" in df_disp.columns and "Horas_Reales" in df_disp.columns:
        hr = pd.to_numeric(df_disp["Horas_Reales"], errors="coerce").fillna(0.0)
        hp = pd.to_numeric(df_disp["Horas_Prog"], errors="coerce").fillna(0.0)
        df_disp["Eficiencia_pct"] = np.where(hr > 0, (hp / hr) * 100.0, 0.0)
        df_disp["Eficiencia_pct"] = df_disp["Eficiencia_pct"].clip(lower=0, upper=100)
    df_disp = _coalesce_duplicate_columns(df_disp)
    st.dataframe(add_semaforo_column(df_disp), use_container_width=True, height=340)

    if modo_reporte == "Perforación":
        st.subheader("Detalle de conexiones")
        dfc = df_conn.copy()
        if "Minutos_Estandar" in dfc.columns and "Minutos_Reales" in dfc.columns:
            mr = pd.to_numeric(dfc["Minutos_Reales"], errors="coerce").fillna(0.0)
            ms = pd.to_numeric(dfc["Minutos_Estandar"], errors="coerce").fillna(0.0)
            dfc["Eficiencia_pct"] = np.where(mr > 0, (ms / mr) * 100.0, 0.0)
            dfc["Eficiencia_pct"] = dfc["Eficiencia_pct"].clip(lower=0, upper=100)
        st.dataframe(add_semaforo_column(dfc), use_container_width=True, height=280)

    if not df_bha.empty:
        st.subheader("Detalle BHA")
        st.dataframe(add_semaforo_column(df_bha), use_container_width=True, height=280)

# =====================================================================
# TAB: ESTADÍSTICAS POR ETAPA
# =====================================================================
# =====================================================================
# TAB: ESTADÍSTICAS POR ETAPA
# =====================================================================
with tab_estadisticas:
    st.subheader("📊 Estadísticas por Etapa")
    
    # Selector de modo: Etapa actual vs Todas las etapas
    col_modo1, col_modo2 = st.columns([1, 3])
    
    with col_modo1:
        modo_estadisticas = st.radio(
            "Modo de análisis",
            options=["Etapa actual", "Todas las etapas"],
            horizontal=True,
            key="modo_estadisticas"
        )
    
    if modo_estadisticas == "Etapa actual":
        # Filtro para seleccionar etapa
        etapas_disponibles = df["Etapa"].unique().tolist() if not df.empty else []
        if not etapas_disponibles:
            st.info("No hay datos disponibles. Por favor, captura algunas actividades primero.")
        else:
            etapa_seleccionada = st.selectbox("Seleccionar etapa para análisis", etapas_disponibles)
            
            # Filtrar datos por etapa
            df_etapa = df[df["Etapa"] == etapa_seleccionada].copy()
            df_conn_etapa = df_conn[df_conn["Seccion"] == etapa_seleccionada].copy()
            df_bha_etapa = df_bha[df_bha["Etapa"] == etapa_seleccionada].copy()
            
            # ---- SECCIÓN 1: KPIs PRINCIPALES ----
            st.markdown("### 📈 KPIs Principales")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                tp_h_etapa = float(df_etapa[df_etapa["Tipo"] == "TP"]["Horas_Reales"].sum()) if not df_etapa.empty else 0.0
                st.metric("TP (h)", f"{tp_h_etapa:.1f}")
            
            with col2:
                tnpi_h_etapa = float(df_etapa[df_etapa["Tipo"] == "TNPI"]["Horas_Reales"].sum()) if not df_etapa.empty else 0.0
                st.metric("TNPI (h)", f"{tnpi_h_etapa:.1f}")
            
            with col3:
                tnp_h_etapa = float(df_etapa[df_etapa["Tipo"] == "TNP"]["Horas_Reales"].sum()) if not df_etapa.empty else 0.0
                st.metric("TNP (h)", f"{tnp_h_etapa:.1f}")
            
            with col4:
                total_h_etapa = float(df_etapa["Horas_Reales"].sum()) if not df_etapa.empty else 0.0
                eficiencia_etapa = clamp_0_100(safe_pct(tp_h_etapa, total_h_etapa)) if total_h_etapa > 0 else 0.0
                sk, sl, sc = status_from_eff(eficiencia_etapa)
                st.markdown(f"""
                    <div style="text-align:center">
                        <div style="font-size:24px;font-weight:bold;color:{sc}">{eficiencia_etapa:.0f}%</div>
                        <div style="font-size:12px;color:#888">Eficiencia</div>
                    </div>
                """, unsafe_allow_html=True)
            
            # ---- SECCIÓN 2: GRÁFICAS ----
            st.markdown("### 📊 Distribuciones")
            
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                # Distribución de tiempos
                if not df_etapa.empty:
                    df_tiempos = df_etapa.groupby("Tipo")["Horas_Reales"].sum().reset_index()
                    fig_tiempos = px.pie(df_tiempos, names="Tipo", values="Horas_Reales", 
                                        title="Distribución de Tiempos (%)", hole=0.4,
                                        color="Tipo", color_discrete_map={"TP": "#2ECC71", "TNPI": "#E74C3C", "TNP": "#F1C40F"})
                    fig_tiempos.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_tiempos, use_container_width=True)
                else:
                    st.info("No hay datos de tiempos")
            
            with col_chart2:
                # Distribución de operaciones
                if not df_etapa.empty:
                    df_operaciones = df_etapa.groupby("Operacion")["Horas_Reales"].sum().reset_index()
                    df_operaciones = df_operaciones.sort_values("Horas_Reales", ascending=False).head(5)
                    fig_operaciones = px.bar(df_operaciones, x="Operacion", y="Horas_Reales",
                                            title="Top 5 - Operaciones (h)", text_auto=True,
                                            color="Horas_Reales", color_continuous_scale="Viridis")
                    st.plotly_chart(fig_operaciones, use_container_width=True)
                else:
                    st.info("No hay datos de operaciones")
            
            # ---- SECCIÓN 3: TABLAS DETALLADAS ----
            st.markdown("### 📋 Detalles Específicos")
            
            # Inicializar variables fuera de los tabs
            df_conn_summary = pd.DataFrame()
            conexiones_count = 0
            
            tab1, tab2, tab3 = st.tabs(["📊 Metros y ROP", "🔧 BHA", "🔗 Conexiones"])
            
            with tab1:
                # Metros perforados y ROP
                if modo_reporte == "Perforación":
                    # Usar datos por etapa (no globales) para que Programado/Real correspondan a la etapa seleccionada
                    etapa_data = get_etapa_data(etapa_seleccionada)

                    # Metros programados por etapa: usamos PT programada (m)
                    mp_etapa = float(etapa_data.get("pt_programada_m", 0.0) or 0.0)

                    # Metros reales por etapa: acumulado de metros diarios capturados (día + noche)
                    _mr_d_map = etapa_data.get("metros_real_dia_by_date", {}) or {}
                    _mr_n_map = etapa_data.get("metros_real_noche_by_date", {}) or {}
                    mr_etapa = float(sum(_mr_d_map.values()) + sum(_mr_n_map.values()))
                    if mr_etapa == 0.0:
                        mr_etapa = float((etapa_data.get("metros_real_dia", 0.0) or 0.0) + (etapa_data.get("metros_real_noche", 0.0) or 0.0))

                    # ROP programada por etapa (meta)
                    rp_etapa = float(etapa_data.get("rop_prog_etapa", 0.0) or 0.0)

                    # ROP real promedio por etapa: promedio simple de los ROP diarios capturados (manual)
                    _rop_d_map = etapa_data.get("rop_real_dia_by_date", {}) or {}
                    _rop_n_map = etapa_data.get("rop_real_noche_by_date", {}) or {}
                    _rop_vals = [float(v) for v in list(_rop_d_map.values()) + list(_rop_n_map.values()) if float(v) > 0]
                    if _rop_vals:
                        rr_etapa = float(sum(_rop_vals) / len(_rop_vals))
                    else:
                        _tmp = []
                        if float(etapa_data.get("rop_real_dia", 0.0) or 0.0) > 0:
                            _tmp.append(float(etapa_data.get("rop_real_dia", 0.0) or 0.0))
                        if float(etapa_data.get("rop_real_noche", 0.0) or 0.0) > 0:
                            _tmp.append(float(etapa_data.get("rop_real_noche", 0.0) or 0.0))
                        rr_etapa = float(sum(_tmp) / len(_tmp)) if _tmp else 0.0

                    eficiencia_metros = (mr_etapa / mp_etapa * 100) if mp_etapa > 0 else 0.0
                    eficiencia_rop = (rr_etapa / rp_etapa * 100) if rp_etapa > 0 else 0.0

                    df_metros = pd.DataFrame({
                        "Concepto": ["Programado", "Real", "Eficiencia"],
                        "Metros (m)": [mp_etapa, mr_etapa, eficiencia_metros],
                        "ROP (m/h)": [rp_etapa, rr_etapa, eficiencia_rop],
                    })

                    # Semáforos SOLO en la fila de eficiencia
                    df_metros["Semáforo Metros"] = ""
                    df_metros["Semáforo ROP"] = ""
                    df_metros.loc[df_metros["Concepto"] == "Eficiencia", "Semáforo Metros"] = semaforo_dot(eficiencia_metros)
                    df_metros.loc[df_metros["Concepto"] == "Eficiencia", "Semáforo ROP"] = semaforo_dot(eficiencia_rop)

                    st.dataframe(df_metros, use_container_width=True, hide_index=True)
                else:
                    st.info("Esta sección aplica solo para el modo Perforación.")
            with tab2:
                # BHA
                if not df_bha_etapa.empty:
                    df_bha_display = df_bha_etapa.copy()
                    df_bha_display["Eficiencia_pct"] = df_bha_display.apply(
                        lambda r: (r["Estandar_h"] / r["Real_h"] * 100) if r["Real_h"] > 0 else 0,
                        axis=1
                    )
                    df_bha_display["Semáforo"] = df_bha_display["Eficiencia_pct"].apply(semaforo_dot)
                    
                    # Gráfica de BHA
                    fig_bha_etapa = px.bar(df_bha_display, x="BHA_Tipo", y=["Estandar_h", "Real_h"],
                                          title="BHA: Estándar vs Real por Tipo", barmode="group",
                                          labels={"value": "Horas", "variable": "Tipo"})
                    st.plotly_chart(fig_bha_etapa, use_container_width=True)
                    
                    st.dataframe(df_bha_display[["BHA_Tipo", "BHA_Componentes", "Accion", "Estandar_h", "Real_h", "TNPI_h", "Eficiencia_pct", "Semáforo"]], 
                               use_container_width=True, hide_index=True)
                else:
                    st.info("No hay datos BHA para esta etapa")
            
            with tab3:
                # Conexiones
                if not df_conn_etapa.empty:
                    # Resumen por conexión
                    df_conn_summary = df_conn_etapa.groupby("Conn_No").agg({
                        "Minutos_Reales": "sum",
                        "Minutos_TNPI": "sum"
                    }).reset_index()
                    df_conn_summary["TP_min"] = df_conn_summary["Minutos_Reales"] - df_conn_summary["Minutos_TNPI"]
                    df_conn_summary["Eficiencia_pct"] = df_conn_summary.apply(
                        lambda r: (r["TP_min"] / r["Minutos_Reales"] * 100) if r["Minutos_Reales"] > 0 else 0,
                        axis=1
                    )
                    df_conn_summary["Semáforo"] = df_conn_summary["Eficiencia_pct"].apply(semaforo_dot)
                    conexiones_count = len(df_conn_summary)
                    
                    # Gráfica de conexiones
                    fig_conn_etapa = px.bar(df_conn_summary, x="Conn_No", y=["TP_min", "Minutos_TNPI"],
                                           title="Conexiones: TP vs TNPI", barmode="stack",
                                           labels={"value": "Minutos", "variable": "Tipo"})
                    st.plotly_chart(fig_conn_etapa, use_container_width=True)
                    
                    st.dataframe(df_conn_summary[["Conn_No", "Minutos_Reales", "TP_min", "Minutos_TNPI", "Eficiencia_pct", "Semáforo"]],
                               use_container_width=True, hide_index=True)
                else:
                    st.info("No hay datos de conexiones para esta etapa")
                    conexiones_count = 0
            
            # ---- SECCIÓN 4: ANÁLISIS TNPI ----
            st.markdown("### 🔍 Análisis de TNPI")
            
            if tnpi_h_etapa > 0:
                # Top causas de TNPI
                df_tnpi_causas = df_etapa[df_etapa["Tipo"] == "TNPI"].groupby(["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP"])["Horas_Reales"].sum().reset_index()
                df_tnpi_causas = df_tnpi_causas.sort_values("Horas_Reales", ascending=False).head(10)
                
                col_causas1, col_causas2 = st.columns(2)
                
                with col_causas1:
                    # Gráfica de causas
                    if not df_tnpi_causas.empty:
                        fig_causas = px.bar(df_tnpi_causas, x="Detalle_TNPI", y="Horas_Reales",
                                           title="Top 10 - Causas de TNPI (h)",
                                           color="Horas_Reales", color_continuous_scale="Reds")
                        fig_causas.update_layout(xaxis_tickangle=45)
                        st.plotly_chart(fig_causas, use_container_width=True)
                
                with col_causas2:
                    # Tabla de causas
                    if not df_tnpi_causas.empty:
                        st.dataframe(df_tnpi_causas[["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP", "Horas_Reales"]],
                                   use_container_width=True, hide_index=True)
                
                # Distribución por categoría
                df_tnpi_cat = df_etapa[df_etapa["Tipo"] == "TNPI"].groupby("Categoria_TNPI")["Horas_Reales"].sum().reset_index()
                if not df_tnpi_cat.empty:
                    fig_tnpi_cat = px.pie(df_tnpi_cat, names="Categoria_TNPI", values="Horas_Reales",
                                         title="TNPI por Categoría (%)", hole=0.3)
                    st.plotly_chart(fig_tnpi_cat, use_container_width=True)
            else:
                st.success("🎉 No hay TNPI registrado para esta etapa")
            

            # ---- SECCIÓN 4B: ANÁLISIS TNP ----
            st.markdown("### 🔵 Análisis de TNP")

            df_tnp_etapa = df_etapa[df_etapa["Tipo"] == "TNP"].copy()
            if not df_tnp_etapa.empty:
                # Normalizar nulos/guiones para evitar 'nan'
                for col in ["Categoria_TNP", "Detalle_TNP"]:
                    if col not in df_tnp_etapa.columns:
                        df_tnp_etapa[col] = ""
                    df_tnp_etapa[col] = (
                        df_tnp_etapa[col]
                        .astype(str)
                        .replace({"nan": "", "None": "", "-": ""})
                        .fillna("")
                        .str.strip()
                    )

                df_tnp_etapa["Categoria_TNP"] = df_tnp_etapa["Categoria_TNP"].replace({"": "Sin categoría"})
                df_tnp_etapa["Detalle_TNP"] = df_tnp_etapa["Detalle_TNP"].replace({"": "Sin detalle"})

                # Top detalles
                df_tnp_top = (
                    df_tnp_etapa.groupby(["Categoria_TNP", "Detalle_TNP"])["Horas_Reales"]
                    .sum()
                    .reset_index()
                    .sort_values("Horas_Reales", ascending=False)
                    .head(10)
                )

                col_tnp1, col_tnp2 = st.columns(2)
                with col_tnp1:
                    if not df_tnp_top.empty:
                        fig_tnp_top = px.bar(
                            df_tnp_top,
                            x="Detalle_TNP",
                            y="Horas_Reales",
                            title="Top 10 - Causas de TNP (h)",
                            color="Horas_Reales",
                            color_continuous_scale="Blues",
                        )
                        fig_tnp_top.update_layout(xaxis_tickangle=45)
                        st.plotly_chart(fig_tnp_top, use_container_width=True)

                with col_tnp2:
                    st.dataframe(
                        df_tnp_top[["Categoria_TNP", "Detalle_TNP", "Horas_Reales"]],
                        use_container_width=True,
                        hide_index=True,
                    )

                # Distribución por categoría
                df_tnp_cat = (
                    df_tnp_etapa.groupby("Categoria_TNP")["Horas_Reales"]
                    .sum()
                    .reset_index()
                    .sort_values("Horas_Reales", ascending=False)
                )
                if not df_tnp_cat.empty:
                    fig_tnp_cat = px.pie(
                        df_tnp_cat,
                        names="Categoria_TNP",
                        values="Horas_Reales",
                        title="TNP por Categoría (%)",
                        hole=0.3,
                    )
                    st.plotly_chart(fig_tnp_cat, use_container_width=True)
            else:
                st.success("🎉 No hay TNP registrado para esta etapa")

            # ---- SECCIÓN 5: RESUMEN EJECUTIVO ----
            st.markdown("### 📋 Resumen Ejecutivo")
            
            # Crear resumen ejecutivo
            resumen_data = {
                "Métrica": ["Horas Totales", "TP (Horas Productivas)", "TNPI (Horas No Productivas)", 
                           "TNP (Tiempo No Productivo)", "Eficiencia General", "Metros Perforados", 
                           "ROP Promedio", "Conexiones Realizadas", "Operaciones BHA"],
                "Valor": [
                    f"{total_h_etapa:.1f} h",
                    f"{tp_h_etapa:.1f} h",
                    f"{tnpi_h_etapa:.1f} h",
                    f"{tnp_h_etapa:.1f} h",
                    f"{eficiencia_etapa:.0f}%",
                    f"{mr_etapa:.0f} m" if modo_reporte == "Perforación" else "N/A",
                    f"{rr_etapa:.1f} m/h" if modo_reporte == "Perforación" else "N/A",
                    f"{conexiones_count}",
                    f"{len(df_bha_etapa)}" if not df_bha_etapa.empty else "0"
                ],
                "Estado": [
                    "🟢" if total_h_etapa > 0 else "⚪",
                    "🟢" if tp_h_etapa > 0 else "⚪",
                    "🟡" if 0 < tnpi_h_etapa < 5 else ("🔴" if tnpi_h_etapa >= 5 else "🟢"),
                    "🟡" if 0 < tnp_h_etapa < 3 else ("🔴" if tnp_h_etapa >= 3 else "🟢"),
                    semaforo_dot(eficiencia_etapa),
                    "🟢" if mr_etapa > 0 else "⚪",
                    "🟢" if rr_etapa > 0 else "⚪",
                    "🟢" if conexiones_count > 0 else "⚪",
                    "🟢" if len(df_bha_etapa) > 0 else "⚪"
                ]
            }
            
            df_resumen = pd.DataFrame(resumen_data)
            st.dataframe(df_resumen, use_container_width=True, hide_index=True)
            
            # Botón para exportar reporte de etapa
            col_exp1, col_exp2 = st.columns(2)
            with col_exp1:
                if st.button("📥 Exportar Reporte de Etapa (PDF)", use_container_width=True):
                    # Aquí iría la lógica para exportar el reporte de etapa
                    st.success("Funcionalidad de exportación en desarrollo")
            
            with col_exp2:
                if st.button("📊 Generar Dashboard Ejecutivo", use_container_width=True):
                    st.success("Dashboard generado para revisión ejecutiva")
    
    else:  # Modo "Todas las etapas"
        st.info("Mostrando estadísticas consolidadas de todas las etapas")
        
        # Mostrar un mensaje y botón para ir al reporte general
        st.markdown("""
        **Para ver el reporte general completo con todas las etapas, por favor ve a la pestaña:**
        ### 📊 **"Reporte General del Pozo"**
        
        Allí encontrarás:
        - KPIs consolidados de todas las etapas
        - Gráficas de distribución general
        - Análisis de TNPI por categoría y etapa
        - Tablas resumen detalladas
        - Opciones de exportación
        """)
        
        # Botón para ir directamente al tab general
        if st.button("Ir a Reporte General del Pozo", use_container_width=True):
            # No hay forma directa de cambiar tabs en Streamlit, pero podemos usar session state
            st.session_state["active_tab"] = "Reporte General del Pozo"
            st.rerun()

# =====================================================================

# =====================================================================
# TAB: ESTADÍSTICAS POR CORRIDA
# =====================================================================
with tab_corridas:
    st.subheader("Estadísticas por corrida")

    if "corridas_manual" not in st.session_state:
        st.session_state.corridas_manual = []  # lista de dicts: {Nombre, Etapa, Prof_ini, Prof_fin, Fecha_ini, Fecha_fin}

    df = st.session_state.get("df", pd.DataFrame()).copy()
    if df.empty:
        st.info("Aún no hay actividades cargadas para calcular estadísticas.")
    else:
        metodo = st.radio(
            "Método de definición de corrida",
            options=["Por Corrida (campo Corrida)", "Auto por BHA (Arma/Desarma)", "Por profundidad (manual)"],
            horizontal=True,
        )

        # -----------------------------
        # 1) Por columna Corrida
        # -----------------------------
        if metodo == "Por Corrida (campo Corrida)":
            if "Corrida" not in df.columns:
                st.warning('No existe la columna "Corrida" en la tabla de actividades.')
            else:
                corridas = [c for c in df["Corrida"].dropna().unique().tolist() if str(c).strip() != ""]
                if not corridas:
                    st.info('No hay valores en "Corrida". Puedes capturar el campo Corrida en el sidebar o usar los otros métodos.')
                else:
                    corrida_sel = st.selectbox("Selecciona corrida", options=sorted(corridas, key=lambda x: str(x)))
                    d = df[df["Corrida"] == corrida_sel].copy()

                    # KPIs
                    d["Horas_Reales"] = pd.to_numeric(d["Horas_Reales"], errors="coerce").fillna(0.0)
                    d["Horas_Prog"] = pd.to_numeric(d["Horas_Prog"], errors="coerce").fillna(0.0)
                    total_h = float(d["Horas_Reales"].sum())
                    tp_h = float(d.loc[d["Tipo"] == "TP", "Horas_Reales"].sum())
                    tnpi_h = float(d.loc[d["Tipo"] == "TNPI", "Horas_Reales"].sum())
                    tnp_h = float(d.loc[d["Tipo"] == "TNP", "Horas_Reales"].sum())
                    eff = (tp_h / total_h * 100.0) if total_h > 0 else 0.0

                    c1, c2, c3, c4, c5 = st.columns(5)
                    c1.metric("Total (h)", f"{total_h:.2f}")
                    c2.metric("TP (h)", f"{tp_h:.2f}")
                    c3.metric("TNPI (h)", f"{tnpi_h:.2f}")
                    c4.metric("TNP (h)", f"{tnp_h:.2f}")
                    c5.metric("Eficiencia (%)", f"{eff:.1f}")

                    st.markdown("### Composición de tiempos (TP/TNPI/TNP)")
                    try:
                        _comp = (
                            d.groupby(["Actividad", "Tipo"], dropna=False)["Horas_Reales"]
                            .sum()
                            .reset_index()
                        )
                        _comp = _comp[_comp["Horas_Reales"] > 0]
                        if _comp.empty:
                            st.info("No hay horas para graficar en la corrida seleccionada.")
                        else:
                            fig_stack = px.bar(
                                _comp,
                                x="Horas_Reales",
                                y="Actividad",
                                color="Tipo",
                                orientation="h",
                                title="Horas por actividad (apilado por tipo)",
                            )
                            fig_stack.update_layout(xaxis_title="Horas", yaxis_title="Actividad", barmode="stack")
                            st.plotly_chart(fig_stack, use_container_width=True)

                            _tot = (
                                d.groupby("Tipo", dropna=False)["Horas_Reales"]
                                .sum()
                                .reset_index()
                            )
                            _tot = _tot[_tot["Horas_Reales"] > 0]
                            if not _tot.empty:
                                fig_donut = px.pie(
                                    _tot,
                                    names="Tipo",
                                    values="Horas_Reales",
                                    title="Composición total de tiempos",
                                    hole=0.35,
                                )
                                st.plotly_chart(fig_donut, use_container_width=True)
                    except Exception as _e:
                        st.warning(f"No pude generar gráficas combinadas: {_e}")

                    st.markdown("### Distribución TNPI (por horas)")
                    pareto = (
                        d[d["Tipo"] == "TNPI"]
                        .groupby(["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP"], dropna=False)["Horas_Reales"]
                        .sum()
                        .sort_values(ascending=False)
                        .reset_index()
                    )
                    st.dataframe(pareto, use_container_width=True, hide_index=True)

                    # --- Gráficas (pro) TNPI por corrida ---
                    try:
                        if not pareto.empty:
                            _top = pareto.copy().head(12)
                            _top["Etiqueta"] = _top["Detalle_TNPI"].astype(str)

                            fig_bar = px.bar(
                                _top.sort_values("Horas_Reales", ascending=True),
                                x="Horas_Reales",
                                y="Etiqueta",
                                orientation="h",
                                title="Top TNPI por detalle (h)",
                            )
                            fig_bar.update_layout(xaxis_title="Horas", yaxis_title="Detalle TNPI")
                            st.plotly_chart(fig_bar, use_container_width=True)

                            _cat = (
                                pareto.groupby("Categoria_TNPI", dropna=False)["Horas_Reales"]
                                .sum()
                                .sort_values(ascending=False)
                                .reset_index()
                            )
                            _cat = _cat[_cat["Horas_Reales"] > 0]
                            if not _cat.empty:
                                fig_pie = px.pie(
                                    _cat,
                                    names="Categoria_TNPI",
                                    values="Horas_Reales",
                                    title="Distribución TNPI por categoría",
                                    hole=0.35,
                                )
                                st.plotly_chart(fig_pie, use_container_width=True)
                    except Exception as _e:
                        st.warning(f"No pude generar gráficas por corrida: {_e}")


                st.markdown("### Distribución TNP (por horas)")
                try:
                    df_tnp = d[d["Tipo"] == "TNP"].copy() if "d" in locals() else pd.DataFrame()
                    if df_tnp.empty and "d" in locals():
                        df_tnp = d[d["Tipo"] == "TNP"].copy()

                    if df_tnp.empty:
                        st.info("No hay registros TNP para la corrida seleccionada.")
                    else:
                        # Tabla: resumen TNP por actividad (o por detalle si existe)
                        # Fallback: si Detalle_TNP no viene poblado, usar Detalle_TNPI (compatibilidad con versiones viejas)
                        df_tnp["_Detalle_TNP_view"] = "-"
                        if "Detalle_TNP" in df_tnp.columns:
                            df_tnp["_Detalle_TNP_view"] = df_tnp["Detalle_TNP"].astype(str)
                        if (df_tnp["_Detalle_TNP_view"].astype(str).str.strip().eq("-").all()
                            and "Detalle_TNPI" in df_tnp.columns):
                            df_tnp["_Detalle_TNP_view"] = df_tnp["Detalle_TNPI"].astype(str)

                        df_tnp["_Categoria_TNP_view"] = "-"
                        if "Categoria_TNP" in df_tnp.columns:
                            df_tnp["_Categoria_TNP_view"] = df_tnp["Categoria_TNP"].astype(str)
                        if (df_tnp["_Categoria_TNP_view"].astype(str).str.strip().eq("-").all()
                            and "Categoria_TNPI" in df_tnp.columns):
                            df_tnp["_Categoria_TNP_view"] = df_tnp["Categoria_TNPI"].astype(str)

                        if df_tnp["_Detalle_TNP_view"].astype(str).str.strip().ne("-").any():
                            grp_cols = ["_Categoria_TNP_view", "_Detalle_TNP_view"]
                            label_col = "_Detalle_TNP_view"
                        else:
                            grp_cols = ["Actividad"]
                            label_col = "Actividad"

                        tnp_tbl = (
                            df_tnp.groupby(grp_cols, dropna=False)["Horas_Reales"]
                            .sum()
                            .sort_values(ascending=False)
                            .reset_index()
                        )
                        
                        # Mostrar nombres amigables si estamos usando columnas de vista
                        if "_Detalle_TNP_view" in tnp_tbl.columns:
                            tnp_tbl = tnp_tbl.rename(columns={
                                "_Categoria_TNP_view": "Categoria_TNP",
                                "_Detalle_TNP_view": "Detalle_TNP",
                            })
                            if label_col == "_Detalle_TNP_view":
                                label_col = "Detalle_TNP"
                        st.dataframe(tnp_tbl, use_container_width=True, hide_index=True)

                        # Barras: Top 12
                        top_tnp = tnp_tbl.head(12).copy()
                        fig_tnp_bar = px.bar(
                            top_tnp.sort_values("Horas_Reales", ascending=True),
                            x="Horas_Reales",
                            y=label_col,
                            orientation="h",
                            title="Top TNP (h)",
                        )
                        fig_tnp_bar.update_layout(xaxis_title="Horas", yaxis_title=label_col)
                        st.plotly_chart(fig_tnp_bar, use_container_width=True)

                        # Donut: por categoría si existe, si no, por actividad
                        if "_Categoria_TNP_view" in df_tnp.columns and df_tnp["_Categoria_TNP_view"].astype(str).str.strip().ne("-").any():
                            cat_tbl = (
                                df_tnp.groupby("_Categoria_TNP_view", dropna=False)["Horas_Reales"]
                                .sum()
                                .sort_values(ascending=False)
                                .reset_index()
                            )
                            cat_tbl = cat_tbl[cat_tbl["Horas_Reales"] > 0]
                            if not cat_tbl.empty:
                                fig_tnp_pie = px.pie(
                                    cat_tbl,
                                    names="_Categoria_TNP_view",
                                    values="Horas_Reales",
                                    title="Distribución TNP por categoría",
                                    hole=0.35,
                                )
                                st.plotly_chart(fig_tnp_pie, use_container_width=True)
                        else:
                            # fallback: distribución por actividad (Top 8 + Otros)
                            pie_df = tnp_tbl.head(8).copy()
                            otros = float(tnp_tbl["Horas_Reales"].sum() - pie_df["Horas_Reales"].sum())
                            if otros > 0:
                                pie_df = pd.concat([pie_df, pd.DataFrame([{label_col: "Otros", "Horas_Reales": otros}])], ignore_index=True)
                            fig_tnp_pie = px.pie(
                                pie_df,
                                names=label_col,
                                values="Horas_Reales",
                                title="Distribución TNP",
                                hole=0.35,
                            )
                            st.plotly_chart(fig_tnp_pie, use_container_width=True)
                except Exception as _e:
                    st.warning(f"No pude generar gráficas TNP por corrida: {_e}")


                    st.markdown("### Detalle de actividades")
                    st.dataframe(d, use_container_width=True, hide_index=True)

        # -----------------------------
        # 2) Auto por BHA
        # -----------------------------
        elif metodo == "Auto por BHA (Arma/Desarma)":
            df_bha = st.session_state.get("df_bha", pd.DataFrame()).copy()
            if df_bha.empty or "Accion" not in df_bha.columns:
                st.info("No hay registros de BHA para calcular corridas automáticamente.")
            else:
                # Normalizar fecha
                if "Fecha" in df_bha.columns:
                    df_bha["Fecha"] = pd.to_datetime(df_bha["Fecha"], errors="coerce")
                df_bha = df_bha.sort_values(["Fecha"], na_position="last")

                # Crear RunId incremental: inicia con acciones tipo 'Arma'
                def _is_start(a):
                    a = str(a).strip().lower()
                    return a.startswith("arma") or a.startswith("arm") or a.startswith("a")

                run_id = 0
                run_ids = []
                for a in df_bha["Accion"].fillna("").tolist():
                    if _is_start(a):
                        run_id += 1
                    run_ids.append(run_id if run_id > 0 else 1)
                df_bha["RunId_Auto"] = run_ids

                run_sel = st.selectbox("Selecciona corrida (auto por BHA)", options=sorted(df_bha["RunId_Auto"].unique().tolist()))
                # Filtrar actividades que pertenecen a esa corrida usando fecha ventana de BHA
                win = df_bha[df_bha["RunId_Auto"] == run_sel].copy()
                tmin = win["Fecha"].min()
                tmax = win["Fecha"].max()

                d = df.copy()
                if "Fecha" in d.columns:
                    d["Fecha_dt"] = pd.to_datetime(d["Fecha"], errors="coerce")
                    d = d[(d["Fecha_dt"] >= tmin) & (d["Fecha_dt"] <= tmax)].copy()

                d["Horas_Reales"] = pd.to_numeric(d["Horas_Reales"], errors="coerce").fillna(0.0)
                total_h = float(d["Horas_Reales"].sum())
                tp_h = float(d.loc[d["Tipo"] == "TP", "Horas_Reales"].sum())
                tnpi_h = float(d.loc[d["Tipo"] == "TNPI", "Horas_Reales"].sum())
                eff = (tp_h / total_h * 100.0) if total_h > 0 else 0.0

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Ventana", f"{tmin.date()} → {tmax.date()}" if pd.notna(tmin) and pd.notna(tmax) else "—")
                c2.metric("Total (h)", f"{total_h:.2f}")
                c3.metric("TNPI (h)", f"{tnpi_h:.2f}")
                c4.metric("Eficiencia (%)", f"{eff:.1f}")

                st.markdown("### Eventos BHA de la corrida")
                st.dataframe(win, use_container_width=True, hide_index=True)

                st.markdown("### Actividades dentro de la ventana")
                st.dataframe(d.drop(columns=["Fecha_dt"], errors="ignore"), use_container_width=True, hide_index=True)

        # -----------------------------
        # 3) Manual por profundidad
        # -----------------------------
        else:
            st.markdown("Define corridas manuales por profundidad (y opcionalmente por fechas) para calcular estadísticas.")
            with st.expander("Crear / editar corrida (manual)", expanded=False):
                nombre = st.text_input("Nombre corrida", "")
                etapa_sel = st.selectbox("Etapa", options=sorted(df["Etapa"].dropna().unique().tolist(), key=lambda x: str(x)))
                prof_ini = st.number_input("Profundidad inicio (m)", min_value=0.0, value=0.0, step=1.0)
                prof_fin = st.number_input("Profundidad fin (m)", min_value=0.0, value=0.0, step=1.0)

                # Fecha opcional para filtrar actividades (más confiable que inferir por profundidad sin log continuo)
                usar_fechas = st.checkbox("Filtrar actividades por rango de fechas (opcional)", value=True)
                fecha_ini = st.date_input("Fecha inicio", value=pd.Timestamp.today().date())
                fecha_fin = st.date_input("Fecha fin", value=pd.Timestamp.today().date())

                if st.button("Guardar corrida manual", use_container_width=True):
                    if not nombre.strip():
                        st.warning("Escribe un nombre para la corrida.")
                    elif prof_fin <= prof_ini:
                        st.warning("Profundidad fin debe ser mayor que profundidad inicio.")
                    else:
                        st.session_state.corridas_manual.append({
                            "Nombre": nombre.strip(),
                            "Etapa": etapa_sel,
                            "Prof_ini": float(prof_ini),
                            "Prof_fin": float(prof_fin),
                            "Usar_fechas": bool(usar_fechas),
                            "Fecha_ini": str(fecha_ini),
                            "Fecha_fin": str(fecha_fin),
                        })
                        st.success("Corrida guardada.")
                        st.rerun()

            if not st.session_state.corridas_manual:
                st.info("Aún no has creado corridas manuales.")
            else:
                nombres = [c["Nombre"] for c in st.session_state.corridas_manual]
                corrida_sel = st.selectbox("Selecciona corrida manual", options=nombres)
                corrida = next(c for c in st.session_state.corridas_manual if c["Nombre"] == corrida_sel)

                d = df[df["Etapa"] == corrida["Etapa"]].copy()
                if corrida.get("Usar_fechas", True):
                    d["Fecha_dt"] = pd.to_datetime(d["Fecha"], errors="coerce")
                    d = d[
                        (d["Fecha_dt"] >= pd.to_datetime(corrida["Fecha_ini"])) &
                        (d["Fecha_dt"] <= pd.to_datetime(corrida["Fecha_fin"]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
                    ].copy()

                d["Horas_Reales"] = pd.to_numeric(d["Horas_Reales"], errors="coerce").fillna(0.0)
                total_h = float(d["Horas_Reales"].sum())
                tp_h = float(d.loc[d["Tipo"] == "TP", "Horas_Reales"].sum())
                tnpi_h = float(d.loc[d["Tipo"] == "TNPI", "Horas_Reales"].sum())
                eff = (tp_h / total_h * 100.0) if total_h > 0 else 0.0
                metros = float(corrida["Prof_fin"] - corrida["Prof_ini"])
                rop = (metros / tp_h) if tp_h > 0 else 0.0

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Metros (ΔProf)", f"{metros:.1f} m")
                c2.metric("TP (h)", f"{tp_h:.2f}")
                c3.metric("TNPI (h)", f"{tnpi_h:.2f}")
                c4.metric("ROP (m/h)", f"{rop:.2f}")

                st.markdown("### Distribución TNPI (por horas)")
                pareto = (
                    d[d["Tipo"] == "TNPI"]
                    .groupby(["Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP"], dropna=False)["Horas_Reales"]
                    .sum()
                    .sort_values(ascending=False)
                    .reset_index()
                )
                st.dataframe(pareto, use_container_width=True, hide_index=True)

                st.markdown("### Detalle de actividades (filtrado)")
                st.dataframe(d.drop(columns=["Fecha_dt"], errors="ignore"), use_container_width=True, hide_index=True)


# =====================================================================
# TAB: ESTADÍSTICAS DRILLSPOT (KPI EXPORT)
# =====================================================================
with tab_drillspot:
    st.subheader("Estadísticas DrillSpot (KPI Export)")
    st.caption("Estas estadísticas se calculan aparte y no modifican tus actividades. Carga el KPI Export (CSV o XLSX) para activar la vista.")

    up = st.file_uploader("Cargar KPI Export de DrillSpot", type=["csv", "xlsx"])
    if up is None:
        st.info("Carga un archivo para ver las estadísticas DrillSpot.")
    else:
        try:
            if up.name.lower().endswith(".csv"):
                kpi_raw = pd.read_csv(up)
            else:
                # intenta hoja por defecto; si falla, lee la primera
                xls = pd.ExcelFile(up)
                sheet = "KPI Report" if "KPI Report" in xls.sheet_names else xls.sheet_names[0]
                kpi_raw = pd.read_excel(up, sheet_name=sheet)

            # Limpieza robusta: eliminar fila de unidades típica (Start Time == 'date')
            if "Start Time" in kpi_raw.columns:
                kpi = kpi_raw.copy()
                kpi = kpi[kpi["Start Time"].astype(str).str.lower() != "date"].copy()
            else:
                kpi = kpi_raw.copy()

            # Tipos
            if "Start Time" in kpi.columns:
                kpi["Start Time"] = pd.to_datetime(kpi["Start Time"], errors="coerce")
            if "End Time" in kpi.columns:
                kpi["End Time"] = pd.to_datetime(kpi["End Time"], errors="coerce")
            for c in ["Start Bit Depth", "End Bit Depth", "Duration"]:
                if c in kpi.columns:
                    kpi[c] = pd.to_numeric(kpi[c], errors="coerce")

            # Duración en horas (Duration viene en minutos en el export)
            if "Duration" in kpi.columns:
                kpi["Duration_h"] = kpi["Duration"] / 60.0

            st.success(f"Archivo cargado: {up.name} ({len(kpi):,} filas)")

            # Resumen general
            total_h = float(kpi["Duration_h"].sum()) if "Duration_h" in kpi.columns else 0.0
            prof_ini = float(kpi["Start Bit Depth"].min()) if "Start Bit Depth" in kpi.columns else 0.0
            prof_fin = float(kpi["End Bit Depth"].max()) if "End Bit Depth" in kpi.columns else 0.0
            net_m = prof_fin - prof_ini
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total (h)", f"{total_h:.2f}")
            c2.metric("Prof. inicio (m)", f"{prof_ini:.2f}")
            c3.metric("Prof. fin (m)", f"{prof_fin:.2f}")
            c4.metric("ΔProf neta (m)", f"{net_m:.2f}")

            # Stats por Run
            if "Run" in kpi.columns:
                st.markdown("### Resumen por Run")
                run = (
                    kpi.groupby("Run", dropna=False)
                    .agg(
                        Inicio=("Start Bit Depth", "min"),
                        Fin=("End Bit Depth", "max"),
                        Delta=("End Bit Depth", lambda s: float(s.max()) - float(kpi.loc[s.index, "Start Bit Depth"].min()) if "Start Bit Depth" in kpi.columns else 0.0),
                        Horas=("Duration_h", "sum"),
                        Eventos=("KPI", "count"),
                    )
                    .reset_index()
                )
                st.dataframe(run, use_container_width=True, hide_index=True)
                try:
                    r = run.copy()
                    try:
                        r["Run_num"] = pd.to_numeric(r["Run"], errors="coerce")
                        r = r.sort_values("Run_num")
                    except Exception:
                        pass

                    fig_run = px.bar(
                        r,
                        x="Run",
                        y="Horas",
                        title="Horas por Run",
                    )
                    fig_run.update_layout(
                        xaxis_title="Run",
                        yaxis_title="Horas",
                        legend_title_text="",
                    )
                    st.plotly_chart(fig_run, use_container_width=True)
                except Exception as _e:
                    st.warning(f"No pude generar gráfica por Run: {_e}")
            # Pareto KPI
            if "KPI" in kpi.columns and "Duration_h" in kpi.columns:
                st.markdown("### Pareto (KPI por horas)")
                pareto = (
                    kpi.groupby("KPI", dropna=False)["Duration_h"]
                    .sum()
                    .sort_values(ascending=False)
                    .reset_index()
                )
                st.dataframe(pareto, use_container_width=True, hide_index=True)
                # ------------------------------
                # Gráficas (Plotly)
                # ------------------------------
                try:
                    # Barras: Top KPIs por horas
                    top = pareto.head(12).copy()
                    fig_bar = px.bar(
                        top.sort_values("Duration_h", ascending=True),
                        x="Duration_h",
                        y="KPI",
                        orientation="h",
                        title="Top KPIs por horas",
                    )
                    fig_bar.update_layout(xaxis_title="Horas", yaxis_title="KPI")
                    st.plotly_chart(fig_bar, use_container_width=True)

                    # Pastel: distribución (Top 8 + Otros)
                    pie_df = pareto.head(8).copy()
                    otros_h = float(pareto["Duration_h"].sum() - pie_df["Duration_h"].sum())
                    if otros_h > 0:
                        pie_df = pd.concat(
                            [pie_df, pd.DataFrame([{"KPI": "Otros", "Duration_h": otros_h}])],
                            ignore_index=True,
                        )

                    fig_pie = px.pie(
                        pie_df,
                        names="KPI",
                        values="Duration_h",
                        title="Distribución de horas por KPI",
                        hole=0.35,
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                except Exception as _e:
                    st.warning(f"No pude generar gráficas KPI: {_e}")
            st.markdown("### Datos crudos (preview)")
            st.dataframe(kpi.head(200), use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"No pude leer el archivo. Error: {e}")

# NUEVA TAB: REPORTE GENERAL DEL POZO (TODAS LAS ETAPAS)
# =====================================================================
with tab_general:
    st.subheader("📊 Reporte General del Pozo - Todas las Etapas")
    
    # Verificar si hay datos
    if df.empty:
        st.info("No hay datos disponibles. Por favor, captura algunas actividades primero.")
    else:
        # ---- ESTILO: quitar chips rojos de multiselect (usar tonos neutros) ----
        st.markdown(
            """
            <style>
            /* Multiselect tags/chips */
            div[data-baseweb="tag"]{
                background-color: rgba(255,255,255,0.10) !important;
                border: 1px solid rgba(255,255,255,0.18) !important;
            }
            div[data-baseweb="tag"] span{
                color: rgba(255,255,255,0.90) !important;
            }
            /* 'x' button inside tag */
            div[data-baseweb="tag"] svg{
                fill: rgba(255,255,255,0.70) !important;
            }
            /* Select/multiselect control border */
            div[data-baseweb="select"] > div{
                border-color: rgba(255,255,255,0.18) !important;
                box-shadow: none !important;
            }
            div[data-baseweb="select"] > div:focus-within{
                border-color: rgba(255,255,255,0.35) !important;
                box-shadow: none !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # ---- FILTROS ----
        col_filt1, col_filt2, col_filt3 = st.columns(3)

        with col_filt1:
            if "Fecha" in df.columns:
                _fechas_dt = pd.to_datetime(df["Fecha"], errors="coerce")
                fechas_disponibles = sorted(_fechas_dt.dt.date.dropna().unique().tolist())
            else:
                fechas_disponibles = []
            fecha_seleccionada = st.selectbox(
                "Filtrar por fecha",
                options=["Todas las fechas"] + fechas_disponibles,
                index=0,
                key="filtro_fecha_general",
            )

        with col_filt2:
            # Selector de Tipo de tiempo (sin chips rojos): selectbox
            _opciones_tt = ['Todos', 'TP', 'TNPI', 'TNP']
            tipo_tiempo_sel = st.selectbox('Tipo de tiempo', options=_opciones_tt, index=0, key='tipo_tiempo_sel')
            tipos_tiempo_sel = ['TP','TNPI','TNP'] if tipo_tiempo_sel == 'Todos' else [tipo_tiempo_sel]

        with col_filt3:
            operaciones_disponibles = sorted(df["Operacion"].dropna().unique().tolist()) if "Operacion" in df.columns else []
            # Selector de Operación (sin chips rojos): selectbox
            _ops_operacion = ['Todas', 'Perforación', 'Viaje', 'Conexión', 'BHA', 'NPT', 'Otro']
            operacion_sel = st.selectbox('Filtrar por operación', options=_ops_operacion, index=0, key='operacion_sel')
            operaciones_sel = None if operacion_sel == 'Todas' else [operacion_sel]

        # Aplicar filtros
        df_filtrado = df.copy()

        if fecha_seleccionada != "Todas las fechas":
            df_filtrado = df_filtrado[df_filtrado["Fecha"] == fecha_seleccionada]

        # aplicar filtro de tipo de tiempo
        df_filtrado = df_filtrado[df_filtrado["Tipo"].isin(tipos_tiempo_sel)]

        # aplicar filtro de operación
        if operaciones_sel is not None:
            df_filtrado = df_filtrado[df_filtrado["Operacion"].isin(operaciones_sel)]

        # ---- KPIs GENERALES ----
        st.markdown("### 📈 KPIs Generales del Pozo")
        
        col_kpi1, col_kpi2, col_kpi3, col_kpi4, col_kpi5 = st.columns(5)
        
        with col_kpi1:
            total_horas = float(df_filtrado["Horas_Reales"].sum()) if not df_filtrado.empty else 0.0
            st.metric("Horas Totales", f"{total_horas:.1f} h")
        
        with col_kpi2:
            tp_horas = float(df_filtrado[df_filtrado["Tipo"] == "TP"]["Horas_Reales"].sum()) if not df_filtrado.empty else 0.0
            st.metric("TP (Horas Productivas)", f"{tp_horas:.1f} h")
        
        with col_kpi3:
            tnpi_horas = float(df_filtrado[df_filtrado["Tipo"] == "TNPI"]["Horas_Reales"].sum()) if not df_filtrado.empty else 0.0
            st.metric("TNPI (Horas)", f"{tnpi_horas:.1f} h")
        
        with col_kpi4:
            tnp_horas = float(df_filtrado[df_filtrado["Tipo"] == "TNP"]["Horas_Reales"].sum()) if not df_filtrado.empty else 0.0
            st.metric("TNP (Horas)", f"{tnp_horas:.1f} h")

        with col_kpi5:
            eficiencia_general = clamp_0_100(safe_pct(tp_horas, total_horas)) if total_horas > 0 else 0.0
            sk, sl, sc = status_from_eff(eficiencia_general)
            st.markdown(f"""
                <div style="text-align:center">
                    <div style="font-size:24px;font-weight:bold;color:{sc}">{eficiencia_general:.0f}%</div>
                    <div style="font-size:12px;color:#888">Eficiencia General</div>
                </div>
            """, unsafe_allow_html=True)
        
        # ---- GRÁFICAS GENERALES ----
        st.markdown("### 📊 Distribución General")
        
        # Gráfica 1: Horas por Etapa (Stacked)
        if not df_filtrado.empty:
            # Preparar datos para gráfica de etapas
            df_etapas = df_filtrado.groupby(["Etapa", "Tipo"])["Horas_Reales"].sum().reset_index()
            
            # Pivot table para stacked bar
            df_pivot = df_etapas.pivot_table(index="Etapa", columns="Tipo", values="Horas_Reales", fill_value=0).reset_index()
            
            # Ordenar por total de horas
            df_pivot["Total"] = df_pivot.sum(axis=1, numeric_only=True)
            df_pivot = df_pivot.sort_values("Total", ascending=True)
            
            fig_etapas = go.Figure()
            
            # Colores para los tipos
            colores = {"TP": "#2ECC71", "TNPI": "#E74C3C", "TNP": "#F1C40F"}
            
            for tipo in ["TNP", "TNPI", "TP"]:  # Orden inverso para mejor visualización
                if tipo in df_pivot.columns:
                    fig_etapas.add_trace(go.Bar(
                        name=tipo,
                        y=df_pivot["Etapa"],
                        x=df_pivot[tipo],
                        orientation='h',
                        marker_color=colores.get(tipo, "#3498DB"),
                        text=df_pivot[tipo].round(1),
                        textposition='auto',
                    ))
            
            fig_etapas.update_layout(
                title="Horas por Etapa - Todas las Etapas",
                barmode='stack',
                height=400,
                xaxis_title="Horas",
                yaxis_title="Etapa",
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig_etapas, use_container_width=True)
        
        # Gráfica 2: Distribución de actividades principales
        if not df_filtrado.empty:
            df_actividades = df_filtrado.groupby("Actividad")["Horas_Reales"].sum().reset_index()
            df_actividades = df_actividades.sort_values("Horas_Reales", ascending=False).head(10)
            
            fig_actividades = px.bar(
                df_actividades, 
                x="Horas_Reales", 
                y="Actividad", 
                orientation='h',
                title="Top 10 Actividades (Horas)",
                color="Horas_Reales",
                color_continuous_scale="Viridis"
            )
            fig_actividades.update_layout(height=400)
            st.plotly_chart(fig_actividades, use_container_width=True)
        
        # ---- ANÁLISIS DE CAUSAS (TNPI / TNP) ----
        st.markdown("### 🔎 Análisis de causas")

        col_a1, col_a2 = st.columns(2)

        with col_a1:
            st.markdown("#### 🔴 TNPI")
            df_tnpi_rg = df_filtrado[df_filtrado["Tipo"] == "TNPI"].copy()
            if df_tnpi_rg.empty:
                st.info("No hay registros TNPI para los filtros seleccionados.")
            else:
                for c, fb in [("Categoria_TNPI","Sin categoría"),("Detalle_TNPI","Sin detalle")]:
                    if c not in df_tnpi_rg.columns:
                        df_tnpi_rg[c]=fb
                    df_tnpi_rg[c]=df_tnpi_rg[c].fillna(fb).replace({"-":fb,"None":fb})
                df_cat = df_tnpi_rg.groupby("Categoria_TNPI", as_index=False)["Horas_Reales"].sum().sort_values("Horas_Reales", ascending=False)
                fig = px.pie(df_cat, names="Categoria_TNPI", values="Horas_Reales", hole=0.55, title="TNPI por categoría (h)")
                st.plotly_chart(fig, use_container_width=True)
                df_det = df_tnpi_rg.groupby("Detalle_TNPI", as_index=False)["Horas_Reales"].sum().sort_values("Horas_Reales", ascending=False).head(10)
                fig2 = px.bar(df_det, x="Horas_Reales", y="Detalle_TNPI", orientation='h', title="Top 10 - Detalle TNPI (h)")
                st.plotly_chart(fig2, use_container_width=True)

        with col_a2:
            st.markdown("#### 🟡 TNP")
            df_tnp_rg = df_filtrado[df_filtrado["Tipo"] == "TNP"].copy()
            if df_tnp_rg.empty:
                st.info("No hay registros TNP para los filtros seleccionados.")
            else:
                for c, fb in [("Categoria_TNP","Sin categoría"),("Detalle_TNP","Sin detalle")]:
                    if c not in df_tnp_rg.columns:
                        df_tnp_rg[c]=fb
                    df_tnp_rg[c]=df_tnp_rg[c].fillna(fb).replace({"-":fb,"None":fb})
                df_cat = df_tnp_rg.groupby("Categoria_TNP", as_index=False)["Horas_Reales"].sum().sort_values("Horas_Reales", ascending=False)
                fig = px.pie(df_cat, names="Categoria_TNP", values="Horas_Reales", hole=0.55, title="TNP por categoría (h)")
                st.plotly_chart(fig, use_container_width=True)
                df_det = df_tnp_rg.groupby("Detalle_TNP", as_index=False)["Horas_Reales"].sum().sort_values("Horas_Reales", ascending=False).head(10)
                fig2 = px.bar(df_det, x="Horas_Reales", y="Detalle_TNP", orientation='h', title="Top 10 - Detalle TNP (h)")
                st.plotly_chart(fig2, use_container_width=True)

        # ---- TABLAS DETALLADAS ----
        st.markdown("### 📋 Resumen por Etapa")
        
        # Crear resumen por etapa
        if not df_filtrado.empty:
            resumen_etapas = []
            etapas_unicas = sorted(df_filtrado["Etapa"].unique())
            
            for etapa_actual in etapas_unicas:
                df_etapa_actual = df_filtrado[df_filtrado["Etapa"] == etapa_actual]
                
                # Calcular KPIs para esta etapa
                total_etapa = float(df_etapa_actual["Horas_Reales"].sum())
                tp_etapa = float(df_etapa_actual[df_etapa_actual["Tipo"] == "TP"]["Horas_Reales"].sum())
                tnpi_etapa = float(df_etapa_actual[df_etapa_actual["Tipo"] == "TNPI"]["Horas_Reales"].sum())
                tnp_etapa = float(df_etapa_actual[df_etapa_actual["Tipo"] == "TNP"]["Horas_Reales"].sum())
                
                eficiencia_etapa = clamp_0_100(safe_pct(tp_etapa, total_etapa)) if total_etapa > 0 else 0.0
                
                # Contar conexiones para esta etapa
                conexiones_etapa = 0
                if not df_conn.empty and "Seccion" in df_conn.columns:
                    conexiones_etapa = len(df_conn[df_conn["Seccion"] == etapa_actual]["Conn_No"].unique())
                
                # Contar BHA para esta etapa
                bha_etapa = 0
                if not df_bha.empty and "Etapa" in df_bha.columns:
                    bha_etapa = len(df_bha[df_bha["Etapa"] == etapa_actual])
                
                resumen_etapas.append({
                    "Etapa": etapa_actual,
                    "Horas Totales": f"{total_etapa:.1f}",
                    "TP (h)": f"{tp_etapa:.1f}",
                    "TNPI (h)": f"{tnpi_etapa:.1f}",
                    "TNP (h)": f"{tnp_etapa:.1f}",
                    "Eficiencia (%)": f"{eficiencia_etapa:.0f}%",
                    "Conexiones": f"{conexiones_etapa}",
                    "Operaciones BHA": f"{bha_etapa}",
                    "Semáforo": semaforo_dot(eficiencia_etapa)
                })
            
            # Crear DataFrame y mostrar
            df_resumen_etapas = pd.DataFrame(resumen_etapas)
            st.dataframe(df_resumen_etapas, use_container_width=True, hide_index=True)
        
        # ---- ANÁLISIS DE TNPI GENERAL ----
        st.markdown("### 🔍 Análisis de TNPI - Todas las Etapas")
        
        if tnpi_horas > 0:
            # Top causas de TNPI en todas las etapas
            df_tnpi_general = df_filtrado[df_filtrado["Tipo"] == "TNPI"].copy()
            
            col_tnpi1, col_tnpi2 = st.columns(2)
            
            with col_tnpi1:
                # Por categoría
                df_tnpi_cat = df_tnpi_general.groupby("Categoria_TNPI")["Horas_Reales"].sum().reset_index()
                df_tnpi_cat = df_tnpi_cat.sort_values("Horas_Reales", ascending=False)
                
                if not df_tnpi_cat.empty:
                    fig_tnpi_cat = px.bar(
                        df_tnpi_cat, 
                        x="Horas_Reales", 
                        y="Categoria_TNPI", 
                        orientation='h',
                        title="TNPI por Categoría (h)",
                        color="Horas_Reales",
                        color_continuous_scale="Reds"
                    )
                    fig_tnpi_cat.update_layout(height=300)
                    st.plotly_chart(fig_tnpi_cat, use_container_width=True)
            
            with col_tnpi2:
                # Por etapa
                df_tnpi_etapa = df_tnpi_general.groupby("Etapa")["Horas_Reales"].sum().reset_index()
                df_tnpi_etapa = df_tnpi_etapa.sort_values("Horas_Reales", ascending=True)
                
                if not df_tnpi_etapa.empty:
                    fig_tnpi_etapa = px.bar(
                        df_tnpi_etapa, 
                        x="Horas_Reales", 
                        y="Etapa", 
                        orientation='h',
                        title="TNPI por Etapa (h)",
                        color="Horas_Reales",
                        color_continuous_scale="Oranges"
                    )
                    fig_tnpi_etapa.update_layout(height=300)
                    st.plotly_chart(fig_tnpi_etapa, use_container_width=True)
            
            # Tabla detallada de TNPI
            st.markdown("**Detalle de TNPI por etapa y categoría**")
            df_tnpi_detalle = df_tnpi_general.groupby(["Etapa", "Categoria_TNPI", "Detalle_TNPI", "Categoria_TNP", "Detalle_TNP"])["Horas_Reales"].sum().reset_index()
            df_tnpi_detalle = df_tnpi_detalle.sort_values(["Etapa", "Horas_Reales"], ascending=[True, False])
            
            if not df_tnpi_detalle.empty:
                st.dataframe(df_tnpi_detalle, use_container_width=True, height=300)
        else:
            st.success("🎉 No hay TNPI registrado en el período seleccionado")
        


        # ---- ANÁLISIS DE TNP GENERAL ----
        st.markdown("### 🔍 Análisis de TNP - Todas las Etapas")

        if tnp_horas > 0:
            df_tnp_general = df_filtrado[df_filtrado["Tipo"] == "TNP"].copy()

            # Normalizar (evitar NaN / '-')
            for col, fallback in [("Categoria_TNP", "Sin categoría"), ("Detalle_TNP", "Sin detalle")]:
                if col not in df_tnp_general.columns:
                    df_tnp_general[col] = fallback
                df_tnp_general[col] = (
                    df_tnp_general[col]
                    .fillna(fallback)
                    .astype(str)
                    .replace({"-": fallback, "None": fallback, "nan": fallback})
                )

            col_tnp1, col_tnp2 = st.columns(2)

            with col_tnp1:
                df_tnp_cat = df_tnp_general.groupby("Categoria_TNP")["Horas_Reales"].sum().reset_index()
                df_tnp_cat = df_tnp_cat.sort_values("Horas_Reales", ascending=True)
                if not df_tnp_cat.empty:
                    fig_tnp_cat = px.bar(
                        df_tnp_cat,
                        x="Horas_Reales",
                        y="Categoria_TNP",
                        orientation='h',
                        title="TNP por Categoría (h)",
                        color="Horas_Reales",
                        color_continuous_scale="Blues"
                    )
                    fig_tnp_cat.update_layout(height=300)
                    st.plotly_chart(fig_tnp_cat, use_container_width=True)

            with col_tnp2:
                df_tnp_etapa = df_tnp_general.groupby("Etapa")["Horas_Reales"].sum().reset_index()
                df_tnp_etapa = df_tnp_etapa.sort_values("Horas_Reales", ascending=True)
                if not df_tnp_etapa.empty:
                    fig_tnp_etapa = px.bar(
                        df_tnp_etapa,
                        x="Horas_Reales",
                        y="Etapa",
                        orientation='h',
                        title="TNP por Etapa (h)",
                        color="Horas_Reales",
                        color_continuous_scale="Teal"
                    )
                    fig_tnp_etapa.update_layout(height=300)
                    st.plotly_chart(fig_tnp_etapa, use_container_width=True)

            st.markdown("**Detalle de TNP por etapa, categoría y detalle**")
            df_tnp_detalle = df_tnp_general.groupby(["Etapa", "Categoria_TNP", "Detalle_TNP"])["Horas_Reales"].sum().reset_index()
            df_tnp_detalle = df_tnp_detalle.sort_values(["Etapa", "Horas_Reales"], ascending=[True, False])
            if not df_tnp_detalle.empty:
                st.dataframe(df_tnp_detalle, use_container_width=True, height=300)
        else:
            st.success("🎉 No hay TNP registrado en el período seleccionado")

        # ---- EXPORTAR REPORTE GENERAL ----
        st.markdown("### 📥 Exportar Reporte General")
        
        col_exp1, col_exp2 = st.columns(2)
        
        with col_exp1:
            if st.button("📊 Generar Reporte PDF", use_container_width=True):
                # Aquí iría la lógica para generar PDF del reporte general
                st.success("Reporte general generado (funcionalidad en desarrollo)")
        
        with col_exp2:
            if st.button("📈 Exportar Datos a Excel", use_container_width=True):
                # Crear Excel con datos generales
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_filtrado.to_excel(writer, sheet_name='Datos_Completos', index=False)
                    if not df_conn.empty:
                        df_conn.to_excel(writer, sheet_name='Conexiones', index=False)
                    if not df_bha.empty:
                        df_bha.to_excel(writer, sheet_name='BHA', index=False)
                
                output.seek(0)
                st.download_button(
                    label="Descargar Excel",
                    data=output,
                    file_name=f"Reporte_General_{pozo}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
# =====================================================================
# TAB: EJECUTIVO (Causa–raíz + Recomendaciones + PDF)
# =====================================================================
with tab_ejecutivo:
    st.subheader("Análisis causa–raíz (Viajes)")
    df_main = st.session_state.df.copy()

    # --- TNPI Viajes: velocidad vs conexiones ---
    df_vtnpi = df_main[(df_main["Tipo"] == "TNPI") & (df_main["Origen"].fillna("") == "Viajes y conexiones")].copy()

    vel_mask = df_vtnpi["Detalle_TNPI"].fillna("").str.contains("Velocidad", case=False)
    conn_mask = df_vtnpi["Detalle_TNPI"].fillna("").str.contains("Conexi", case=False)

    tnpi_total_h = float(df_vtnpi["Horas_Reales"].sum()) if not df_vtnpi.empty else 0.0
    tnpi_vel_h = float(df_vtnpi.loc[vel_mask, "Horas_Reales"].sum()) if (not df_vtnpi.empty) else 0.0
    tnpi_conn_h = float(df_vtnpi.loc[conn_mask, "Horas_Reales"].sum()) if (not df_vtnpi.empty) else 0.0
    tnpi_otros_h = max(0.0, tnpi_total_h - tnpi_vel_h - tnpi_conn_h)


    c1, c2, c3 = st.columns(3)
    c1.metric("TNPI Viajes – Velocidad (h)", f"{tnpi_vel_h:.2f}")
    c2.metric("TNPI Viajes – Conexiones (h)", f"{tnpi_conn_h:.2f}")
    c3.metric("TNPI Viajes – Total (h)", f"{tnpi_total_h:.2f}")

    # Donut % (si hay datos)
    fig_donut = None
    if tnpi_total_h > 0 and PLOTLY_IMG_OK:
        ddf = pd.DataFrame(
            {"Causa": ["Velocidad", "Conexiones", "Otros"], "Horas": [tnpi_vel_h, tnpi_conn_h, tnpi_otros_h]}
        )
        fig_donut = px.pie(ddf, names="Causa", values="Horas", hole=0.55, title="TNPI Viajes – Distribución (%)")
        fig_donut.update_traces(textinfo="percent+label")
        st.plotly_chart(fig_donut, use_container_width=True)
    elif tnpi_total_h == 0:
        st.info("Aún no hay TNPI de viajes registrado para el día.")

    st.divider()

    # --- Recomendaciones automáticas ---
    st.subheader("Recomendaciones automáticas")
    recos = []
    razones = []

    if tnpi_total_h == 0:
        recos.append("Sin TNPI en viajes registrado: mantener parámetros y disciplina operativa.")
    else:
        p_vel = tnpi_vel_h / tnpi_total_h if tnpi_total_h > 0 else 0.0
        p_conn = tnpi_conn_h / tnpi_total_h if tnpi_total_h > 0 else 0.0

        if p_conn >= 0.60:
            recos += [
                "Priorizar mejora de conexiones: checklist, roles claros y preparación previa (preconexión).",
                "Revisar herramientas/llave/MPD y tiempos muertos recurrentes durante conexiones.",
                "Validar handover turno a turno y asegurar que materiales/herramientas estén listos antes del pico de conexiones."
            ]
            razones.append(f"Conexiones representan {p_conn*100:.0f}% del TNPI de viajes.")
        if p_vel >= 0.60:
            recos += [
                "Priorizar mejora de velocidad de viaje: revisar arrastre/fricción y condiciones del hoyo.",
                "Ajustar prácticas (barrido/limpieza) y revisar límites operativos que reduzcan velocidad.",
                "Evaluar si el método (Lingadas/TxT) está siendo aplicado correctamente por tramo."
            ]
            razones.append(f"Velocidad representa {p_vel*100:.0f}% del TNPI de viajes.")

        if not recos:
            recos.append("TNPI distribuido entre velocidad y conexiones: atacar las 2 principales horas críticas y estandarizar el método por tramo.")

    # Horas críticas (top 3)
    try:
        df_viajes_h = st.session_state.get("viajes_por_hora_df", None)
    except Exception:
        df_viajes_h = None

    if df_viajes_h is not None and isinstance(df_viajes_h, pd.DataFrame) and not df_viajes_h.empty:
        # Espera columnas: hour, tnpi_vel_h, tnpi_conn_h o tnpi_total_h
        cand_cols = [c for c in df_viajes_h.columns if "tnpi" in c.lower()]
        if cand_cols:
            df_tmp = df_viajes_h.copy()
            if "tnpi_total_h" not in df_tmp.columns:
                # crea total si hay vel/conn
                vcol = "tnpi_vel_h" if "tnpi_vel_h" in df_tmp.columns else None
                ccol = "tnpi_conn_h" if "tnpi_conn_h" in df_tmp.columns else None
                if vcol or ccol:
                    df_tmp["tnpi_total_h"] = (df_tmp[vcol].fillna(0) if vcol else 0) + (df_tmp[ccol].fillna(0) if ccol else 0)
            top = df_tmp.sort_values("tnpi_total_h", ascending=False).head(3)
            horas = [f"{int(h):02d}:00" for h in top["hour"].tolist()] if "hour" in top.columns else []
            if horas and float(top["tnpi_total_h"].sum()) > 0:
                razones.append("Horas críticas (mayor TNPI): " + ", ".join(horas))

    if razones:
        st.caption(" • " + " • ".join(razones))

    for r in recos[:6]:
        st.write("• " + r)

    st.divider()

    # --- Export PDF Ejecutivo ---
    st.subheader("Export ejecutivo (PDF)")
    st.caption("Genera un PDF en tamaño Carta con KPIs + gráficas clave (Viajes/Conexiones) + recomendaciones.")

    # Tomamos las figuras de la pestaña de viajes si existen en session_state (si no, no falla)
    fig_speed = st.session_state.get("fig_viaje_speed", None)
    fig_conn = st.session_state.get("fig_viaje_conn", None)

    meta_pdf = {"equipo": st.session_state.get("equipo_val", ""), "pozo": st.session_state.get("pozo_val", ""), "etapa": etapa, "fecha": str(fecha)}
    kpis_pdf = {
        "Eficiencia global (%)": f"{_eff_prev:.0f}%",
        "TNPI Viajes (h)": f"{tnpi_total_h:.2f}",
        "TNPI Velocidad (h)": f"{tnpi_vel_h:.2f}",
        "TNPI Conexiones (h)": f"{tnpi_conn_h:.2f}",
    }

    charts_pdf = {}
    if fig_speed is not None:
        charts_pdf["Viaje – Velocidad por hora"] = fig_speed
    if fig_conn is not None:
        charts_pdf["Conexiones – Min/conn por hora"] = fig_conn
    if fig_donut is not None:
        charts_pdf["TNPI Viajes – Distribución (%)"] = fig_donut

    # Adjuntamos recomendaciones como string en KPIs (para que salgan en el PDF actual sin remaquetar)
    if recos:
        kpis_pdf["Recomendaciones"] = " | ".join(recos[:4])

    pdf_bytes = build_pdf(meta_pdf, kpis_pdf, charts_pdf)
    st.download_button(
        "Descargar PDF diario (Carta)",
        data=pdf_bytes,
        file_name=f"Reporte_DrillSpot_{pozo}_{str(fecha)}.pdf",
        mime="application/pdf",
        use_container_width=True,
    )


# TAB: EXPORTAR
# =====================================================================
with tab_export:
    st.subheader("Exportar (PDF / PowerPoint)")

    meta = {"equipo": st.session_state.get("equipo_val", ""), "pozo": st.session_state.get("pozo_val", ""), "etapa": etapa, "fecha": str(fecha)}
    kpis_export = {
        "Modo": modo_reporte,
        "TP (h)": f"{tp_h:.2f}",
        "TNPI (h)": f"{tnpi_h:.2f}",
        "TNP (h)": f"{tnp_h:.2f}",
        "Eficiencia del día": f"{eficiencia_dia:.0f}%",
    }

    if modo_reporte == "Perforación":
        kpis_export["PT programada (m)"] = f"{float(st.session_state.drill_day['pt_programada_m']):.0f}"
        kpis_export["Profundidad actual (m)"] = f"{float(st.session_state.drill_day['prof_actual_m']):.0f}"
        kpis_export["Metros programa (m)"] = f"{float(st.session_state.drill_day['metros_prog_total']):.0f}"
        kpis_export["Metros real (m)"] = f"{float(st.session_state.drill_day['metros_real_dia'] + st.session_state.drill_day['metros_real_noche']):.0f}"
        kpis_export["ROP programa (m/h)"] = f"{float(st.session_state.drill_day['rop_prog_total']):.2f}"
        rr_d = float(st.session_state.drill_day["rop_real_dia"])
        rr_n = float(st.session_state.drill_day["rop_real_noche"])
        rr_local = (rr_d + rr_n) / (2 if (rr_d > 0 and rr_n > 0) else 1) if (rr_d > 0 or rr_n > 0) else 0.0
        kpis_export["ROP real (m/h)"] = f"{rr_local:.2f}"

    charts_export = {}
    if show_charts:
        for key, label in [
            ("tiempos", "Distribución de tiempos"),
            ("act_pie", "Distribución actividades (pie)"),
            ("act_bar", "Distribución actividades (bar)"),
            ("conn_pie", "Distribución tiempo en conexión (pie)"),
            ("conn_stack", "Conexiones perforando (stack)"),
        ]:
            if figs.get(key) is not None:
                charts_export[label] = figs[key]

    col_pdf, col_ppt = st.columns(2)
    with col_pdf:
        pdf_bytes = build_pdf(meta, kpis_export, charts=charts_export)
        fname_pdf = f"Reporte_DrillSpot_{pozo}_{etapa}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        st.download_button("Descargar PDF", data=pdf_bytes, file_name=fname_pdf, mime="application/pdf", use_container_width=True)

    with col_ppt:
        pptx_bytes = build_pptx(meta, kpis_export, charts_export)
        fname_pptx = f"Reporte_DrillSpot_{pozo}_{etapa}_{datetime.now().strftime('%Y%m%d_%H%M')}.pptx"
        st.download_button(
            "Descargar PowerPoint",
            data=pptx_bytes,
            file_name=fname_pptx,
            mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
            use_container_width=True,
        )

    if not PLOTLY_IMG_OK:
        st.caption("Para exportar gráficas como imágenes instala: `pip install -U kaleido`.")

# NOTE: Added corrected Captura actividad block (see above).

# --- FIX: sincronizar depth_rows desde drill_day (por etapa) ---
rows = []
for _etapa_k, _data in st.session_state.drill_day.get("por_etapa", {}).items():
    rows.append({
        "Etapa": _etapa_k,
        "PT_programada_m": _data.get("pt_programada_m", 0.0),
        "PT_actual_m": _data.get("prof_actual_m", 0.0),
    })
st.session_state.depth_rows = pd.DataFrame(rows)