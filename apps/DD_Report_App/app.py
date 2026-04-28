import io
import re
import smtplib
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

st.set_page_config(page_title="Daily Report -> BOSS Dashboard", page_icon="🛢️", layout="wide")

GREEN = colors.HexColor("#68cbb3")
BORDER = colors.HexColor("#222222")

@dataclass
class Activity:
    start: str
    end: str
    text: str


def clean_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def read_any_file(uploaded_file) -> Tuple[Dict[str, pd.DataFrame], str]:
    name = uploaded_file.name.lower()
    data = uploaded_file.getvalue()
    if name.endswith((".xlsx", ".xls")):
        sheets = pd.read_excel(io.BytesIO(data), sheet_name=None, header=None, dtype=str)
        return sheets, "excel"
    if name.endswith(".csv"):
        return {"CSV": pd.read_csv(io.BytesIO(data), header=None, dtype=str)}, "csv"
    if name.endswith(".txt"):
        text = data.decode("utf-8", errors="ignore")
        rows = [[line] for line in text.splitlines()]
        return {"TXT": pd.DataFrame(rows)}, "txt"
    if name.endswith(".pdf"):
        try:
            import pdfplumber
        except Exception as exc:
            raise RuntimeError("Para leer PDF instala pdfplumber: pip install pdfplumber") from exc
        text_pages = []
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for page in pdf.pages:
                text_pages.append(page.extract_text() or "")
        rows = [[line] for line in "\n".join(text_pages).splitlines()]
        return {"PDF": pd.DataFrame(rows)}, "pdf"
    raise ValueError("Formato no soportado. Usa Excel, CSV, TXT o PDF.")


def dataframe_to_blob_text(sheets: Dict[str, pd.DataFrame]) -> str:
    chunks = []
    for sheet_name, df in sheets.items():
        chunks.append(f"\n--- SHEET: {sheet_name} ---\n")
        for _, row in df.iterrows():
            line = " ".join(clean_text(v) for v in row.tolist() if clean_text(v))
            if line:
                chunks.append(line)
    return "\n".join(chunks)


def find_value(text: str, labels: List[str], default: str = "") -> str:
    for label in labels:
        pattern = rf"{label}\s*[:\-]?\s*([^\n\r]+)"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            value = m.group(1).strip()
            value = re.split(r"\s{2,}|\t| FOLIO:| FECHA:| HORA:", value)[0].strip()
            return value
    return default


def extract_depth(text: str) -> str:
    patterns = [r"prof(?:undidad)?\.?\s*(?:actual)?\s*[:\-]?\s*([0-9,]+(?:\.[0-9]+)?)\s*m", r"(?:md|depth)\s*[:\-]?\s*([0-9,]+(?:\.[0-9]+)?)"]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1).replace(",", "")
    return "0.0"


def normalize_time(t: str) -> str:
    t = t.strip().lower().replace("hrs", "").replace("hr", "")
    m = re.match(r"(\d{1,2})(?::(\d{2}))?", t)
    if not m:
        return t
    h = int(m.group(1))
    minute = int(m.group(2) or 0)
    if h == 24:
        return "24:00"
    h = h % 24
    return f"{h:02d}:{minute:02d}"


def split_activity_text(text: str) -> List[Activity]:
    pattern = re.compile(r"(?P<start>\b\d{1,2}:?\d{0,2})\s*(?:-|a|A|–|—)\s*(?P<end>\d{1,2}:?\d{0,2})\s*(?:hrs?\.?|horas)?", re.IGNORECASE)
    matches = list(pattern.finditer(text))
    activities = []
    for i, match in enumerate(matches):
        start = normalize_time(match.group("start"))
        end = normalize_time(match.group("end"))
        content_start = match.end()
        content_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[content_start:content_end].strip(" .:-\n")
        if body:
            activities.append(Activity(start=start, end=end, text=body))
    return activities


def normalize_activity_sequence(activities: List[Activity]) -> List[Activity]:
    fixed = []
    for a in activities:
        start, end = a.start, a.end
        if start in {"24:00", "24:0"}:
            start = "00:00"
        if end == "00:00" and start != "00:00":
            end = "24:00"
        fixed.append(Activity(start, end, a.text))
    return fixed


def build_operation_text(activities: List[Activity], fallback_text: str) -> str:
    if not activities:
        return fallback_text[:5000]
    parts = []
    for a in normalize_activity_sequence(activities):
        parts.append(f"{a.start}-{a.end} hrs. {a.text}")
    return "\n\n".join(parts)


def extract_report(sheets: Dict[str, pd.DataFrame]) -> Dict[str, str]:
    blob = dataframe_to_blob_text(sheets)
    activities = split_activity_text(blob)
    operation_text = build_operation_text(activities, blob)
    today = datetime.now().strftime("%d/%m/%Y")
    return {
        "cliente": find_value(blob, ["CLIENTE", "CLIENT"], "PEMEX EXPLORACION Y PRODUCCION"),
        "compania": find_value(blob, ["COMPAÑÍA", "COMPANIA", "COMPANY"], ""),
        "pozo": find_value(blob, ["NOMBRE DEL POZO", "POZO", "WELL NAME", "WELL"], ""),
        "ciudad": find_value(blob, ["CIUDAD", "CITY"], ""),
        "estado": find_value(blob, ["ESTADO", "STATE"], ""),
        "folio": find_value(blob, ["FOLIO"], ""),
        "fecha": find_value(blob, ["FECHA", "DATE"], today),
        "hora": find_value(blob, ["HORA", "TIME"], "24:00 hrs"),
        "profundidad": extract_depth(blob),
        "ultimos_tiempos": "-",
        "velocidad_promedio": "-",
        "profundidad_atraso": "-",
        "record_barrena": "-",
        "tiempo_atraso": "-",
        "operacion_actual": "",
        "operacion": operation_text,
        "siguiente": find_value(blob, ["Siguiente", "Next"], ""),
        "programa": find_value(blob, ["Programa", "Program"], ""),
    }


def get_secret(name: str, default=None):
    try:
        return st.secrets[name]
    except Exception:
        return default


def sanitize_filename(value: str, default: str = "SIN_POZO") -> str:
    value = clean_text(value) or default
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    value = value.strip("._-")
    return value or default


def send_email_with_attachment(to_email: str, attachment_bytes: bytes, attachment_name: str, mime_type: str):
    smtp_server = get_secret("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(get_secret("SMTP_PORT", 587))
    smtp_user = get_secret("SMTP_USER")
    smtp_pass = get_secret("SMTP_PASS")

    if not smtp_user or not smtp_pass:
        raise RuntimeError("Faltan credenciales SMTP en .streamlit/secrets.toml. Configura SMTP_USER y SMTP_PASS antes de enviar.")

    msg = EmailMessage()
    msg["Subject"] = f"Daily Report para BOSS Dashboard - {attachment_name}"
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.set_content("Hola,\n\nAdjunto el Daily Report convertido al formato que puede leer BOSS Dashboard.\n\nSaludos.")
    maintype, subtype = mime_type.split("/", 1)
    msg.add_attachment(attachment_bytes, maintype=maintype, subtype=subtype, filename=attachment_name)
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def xml_escape(text: str) -> str:
    return (clean_text(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def split_operation_blocks(operation_text: str, max_chars: int = 1200) -> List[str]:
    raw = operation_text.replace("\r", "\n")
    blocks = [b.strip() for b in re.split(r"\n\s*\n+", raw) if b.strip()]
    if not blocks:
        blocks = [raw.strip()] if raw.strip() else ["-"]
    out: List[str] = []
    for block in blocks:
        block = re.sub(r"\s+", " ", block).strip()
        while len(block) > max_chars:
            cut = block.rfind(". ", 0, max_chars)
            if cut < max_chars * 0.45:
                cut = block.rfind(" ", 0, max_chars)
            if cut <= 0:
                cut = max_chars
            out.append(block[:cut].strip())
            block = block[cut:].strip(" .")
        if block:
            out.append(block)
    return out

def make_pdf(report: Dict[str, str]) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, rightMargin=12*mm, leftMargin=12*mm, topMargin=10*mm, bottomMargin=10*mm)
    styles = getSampleStyleSheet()
    normal = ParagraphStyle("normal", parent=styles["Normal"], fontName="Helvetica", fontSize=7.4, leading=8.8, alignment=TA_LEFT)
    bold = ParagraphStyle("bold", parent=normal, fontName="Helvetica-Bold")
    center = ParagraphStyle("center", parent=bold, alignment=TA_CENTER, fontSize=8.5, leading=10)
    story = []
    header = Table([
        [Paragraph("petricore", ParagraphStyle("logo", parent=center, fontSize=20, textColor=colors.HexColor("#666666"))), Paragraph('FORMATO<br/>“REPORTE DIARIO DE OPERACIÓN DEL REGISTRO DE HIDROCARBUROS”', center)],
        [Paragraph("Revisión", center), Paragraph("Vigente desde", center), Paragraph("Código", center), Paragraph("Aprobación", center)],
        [Paragraph("01", normal), Paragraph("20-Mar-2020", normal), Paragraph("WRS-FM003", normal), Paragraph("Gerencia WSS", normal)],
    ], colWidths=[50*mm, 40*mm, 40*mm, 40*mm])
    header.setStyle(TableStyle([("GRID", (0,0), (-1,-1), .6, BORDER), ("SPAN", (0,0), (0,0)), ("SPAN", (1,0), (3,0)), ("BACKGROUND", (1,0), (3,0), GREEN), ("ALIGN", (0,0), (-1,-1), "CENTER"), ("VALIGN", (0,0), (-1,-1), "MIDDLE")]))
    story += [header, Spacer(1, 12)]
    meta = Table([
        [Paragraph(f"<b>CLIENTE:</b> {report['cliente']}", normal), Paragraph(f"<b>FOLIO:</b> {report['folio']}", normal)],
        [Paragraph(f"<b>COMPAÑÍA:</b> {report['compania']}", normal), Paragraph(f"<b>FECHA:</b> {report['fecha']}", normal)],
        [Paragraph(f"<b>NOMBRE DEL POZO:</b> {report['pozo']}", normal), Paragraph(f"<b>HORA:</b> {report['hora']}", normal)],
        [Paragraph(f"<b>CIUDAD:</b> {report['ciudad']}", normal), ""],
        [Paragraph(f"<b>ESTADO:</b> {report['estado']}", normal), ""],
    ], colWidths=[125*mm, 45*mm])
    story += [meta, Spacer(1, 5)]
    summary = Table([
        ["Profundidad actual (m):", report["profundidad"], "Últimos Tiempos:", report["ultimos_tiempos"], "Velocidad Promedio (min/m)", report["velocidad_promedio"]],
        ["Profundidad tiempo de atraso:", report["profundidad_atraso"], "Record de barrena (h):", report["record_barrena"], "Tiempo de atraso: (min.)", report["tiempo_atraso"]],
        ["Operación actual:", report["operacion_actual"], "", "", "", ""],
    ], colWidths=[38*mm, 20*mm, 38*mm, 22*mm, 42*mm, 10*mm])
    summary.setStyle(TableStyle([("GRID", (0,0), (-1,-1), .5, BORDER), ("BACKGROUND", (0,0), (0,2), GREEN), ("BACKGROUND", (2,0), (2,1), GREEN), ("BACKGROUND", (4,0), (4,1), GREEN), ("SPAN", (1,2), (5,2)), ("FONT", (0,0), (-1,-1), "Helvetica-Bold", 7), ("VALIGN", (0,0), (-1,-1), "MIDDLE")]))
    story += [summary, Spacer(1, 8)]
    op_header = Table([[Paragraph("OPERACION", bold)]], colWidths=[170*mm])
    op_header.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), .5, BORDER), ("BACKGROUND", (0, 0), (-1, -1), GREEN), ("VALIGN", (0, 0), (-1, -1), "TOP")]))
    story.append(op_header)

    operation_style = ParagraphStyle("operation", parent=normal, fontName="Helvetica", fontSize=7.2, leading=8.6, spaceAfter=5, borderWidth=.35, borderColor=BORDER, borderPadding=4)
    for block in split_operation_blocks(report["operacion"]):
        story.append(Paragraph(xml_escape(block), operation_style))

    story.append(Paragraph(f"<b>Siguiente:</b> {xml_escape(report['siguiente']) or '-'}", operation_style))
    story.append(Paragraph(f"<b>Programa:</b> {xml_escape(report['programa']) or '-'}", operation_style))
    for title, headers in [
        ("Cromatografía %", ["Gas", "C1 %", "C2 %", "C3 %", "IC4 %", "NC4 %", "IC5 %", "NC5 %", "CO₂ p.p.m."]),
        ("Lecturas promedio de gas", ["Lecturas:", "Unidades:", "p.p.m.", "Profundidad (m):", "Lecturas:", "Unidades:", "p.p.m.", "Profundidad (m):"]),
        ("Análisis del lodo de perforación", ["Densidad (gr/cm³)", "Viscosidad (Seg)", "Filtrado (ml)", "Enjarre (mm)", "pH", "Salinidad (ppm)", "% Agua", "% Aceite", "% Sólidos"]),
        ("Parámetros de Perforación", ["Peso sobre barrena (ton)", "Temperatura entrada (ºC)", "Temperatura salida (ºC)", "Presión bomba (kg/cm²)", "Conductividad entrada", "Conductividad Salida", "Gasto (gpm)", "Densidad entrada", "Flujo (%)"]),
    ]:
        story.append(Spacer(1, 6))
        table = Table([[Paragraph(title, center)] , [Paragraph(h, center) for h in headers], ["" for _ in headers]], colWidths=[170*mm/len(headers)]*len(headers))
        table.setStyle(TableStyle([("GRID", (0,0), (-1,-1), .5, BORDER), ("SPAN", (0,0), (-1,0)), ("BACKGROUND", (0,0), (-1,1), GREEN), ("FONT", (0,0), (-1,-1), "Helvetica", 6.5), ("ALIGN", (0,0), (-1,-1), "CENTER")]))
        story.append(table)
    doc.build(story)
    return buf.getvalue()

st.markdown("""
<style>
.block-container{padding-top:1.2rem;max-width:1400px}.stDownloadButton button,.stButton button{border-radius:12px;font-weight:700}
</style>
""", unsafe_allow_html=True)

st.title("Conversor de Daily Report a formato BOSS Dashboard")
st.caption("Carga un Daily Report de cualquier compañía en Excel, CSV, TXT o PDF. La app extrae la operación, normaliza la secuencia horaria y genera un PDF con estructura similar al formato Petricore que BOSS Dashboard reconoce.")

with st.sidebar:
    st.header("Parsing Email")
    st.caption("Las credenciales SMTP no se muestran en la app. Se leen desde .streamlit/secrets.toml.")
    sender_email = get_secret("SMTP_USER", "No configurado")
    st.text_input("From email", value=sender_email, disabled=True)
    to_email = st.text_input("To email BOSS Parsing", value="solobox+pemex@rogii.com")

uploaded = st.file_uploader("Sube Daily Report", type=["xlsx", "xls", "csv", "txt", "pdf"])

if uploaded:
    try:
        sheets, file_type = read_any_file(uploaded)
        report = extract_report(sheets)
        st.success(f"Archivo leído como {file_type.upper()}")
        left, right = st.columns([1, 1])
        with left:
            st.subheader("Campos detectados / editables")
            for key in ["cliente", "compania", "pozo", "ciudad", "estado", "folio", "fecha", "hora", "profundidad"]:
                report[key] = st.text_input(key.replace("_", " ").title(), value=report[key])
            report["siguiente"] = st.text_area("Siguiente", value=report["siguiente"], height=80)
            report["programa"] = st.text_area("Programa", value=report["programa"], height=100)
        with right:
            st.subheader("Operación normalizada")
            st.info("Regla aplicada: las actividades deben cerrar en 24:00 y, si continúan al día siguiente, empezar en 00:00, por ejemplo 00:00-05:00 hrs.")
            report["operacion"] = st.text_area("Texto de operación", value=report["operacion"], height=520)
        well_for_file = sanitize_filename(report.get("pozo", ""))
        date_for_file = sanitize_filename(
            report.get("fecha", "").replace("/", "-"),
            datetime.now().strftime("%d-%m-%Y"),
        )
        default_output_name = f"{well_for_file}_BOSS_Dashboard_{date_for_file}.pdf"

        st.markdown("### 📎 Nombre del archivo adjunto de salida")
        output_name = st.text_input(
            "Así quedará el nombre del PDF que se descargará o enviará por email:",
            value=default_output_name,
            help="Puedes editarlo antes de descargar o enviar. Se recomienda mantener el nombre del pozo en el archivo.",
        )
        output_name = sanitize_filename(output_name.replace(".pdf", "")) + ".pdf"

        pdf_bytes = make_pdf(report)
        c1, c2 = st.columns(2)
        with c1:
            st.download_button("⬇️ Descargar PDF para BOSS", data=pdf_bytes, file_name=output_name, mime="application/pdf", use_container_width=True)
        with c2:
            if st.button("📧 Enviar por email", type="primary", use_container_width=True):
                if not to_email:
                    st.error("Completa el correo destino antes de enviar.")
                else:
                    send_email_with_attachment(to_email, pdf_bytes, output_name, "application/pdf")
                    st.success(f"PDF enviado correctamente a {to_email}")
        with st.expander("Vista previa del texto fuente detectado"):
            st.text(dataframe_to_blob_text(sheets)[:12000])
    except Exception as exc:
        st.exception(exc)
else:
    st.info("Esperando archivo. Ejemplo: Daily Report en Excel de Helios o PDF del formato Petricore.")
