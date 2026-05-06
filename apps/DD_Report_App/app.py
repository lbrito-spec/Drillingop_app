"""Daily Report → PDF para Rogii email parsing."""

import io
import re
import smtplib
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

st.set_page_config(page_title="Daily Report -> Rogii Email Parsing", page_icon="🛢️", layout="wide")

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
    value = str(value).replace("\u00a0", " ").replace("–", "-").replace("—", "-")
    return re.sub(r"[ \t]+", " ", value).strip()


def clean_multiline_text(value: str) -> str:
    value = value.replace("\u00a0", " ").replace("–", "-").replace("—", "-")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def xml_escape(text: str) -> str:
    text = clean_text(text)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sanitize_filename(value: str, default: str = "SIN_POZO") -> str:
    value = clean_text(value) or default
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    value = value.strip("._-")
    return value or default


def get_secret(name: str, default=None):
    try:
        return st.secrets[name]
    except Exception:
        return default


def dataframe_to_blob_text(sheets: Dict[str, pd.DataFrame]) -> str:
    chunks = []
    for sheet_name, df in sheets.items():
        chunks.append(f"\n--- SHEET: {sheet_name} ---\n")
        for _, row in df.iterrows():
            line = " ".join(clean_text(v) for v in row.tolist() if clean_text(v))
            if line:
                chunks.append(line)
    return "\n".join(chunks)


def ocr_pdf_bytes(data: bytes) -> str:
    """OCR para PDFs sin texto seleccionable. Usa opencv-headless en Linux/Docker."""
    try:
        import numpy as np
        from PIL import Image
        import fitz  # pymupdf
        from rapidocr_onnxruntime import RapidOCR
    except Exception as exc:  # ImportError/OSError/dlopen(libGL…) al cargar cv2/onnxruntime
        hint = (
            "pip: pymupdf rapidocr-onnxruntime onnxruntime Pillow numpy opencv-python. "
            "En Linux/OpenGL hace falta libGL.so.1 → en Streamlit usa packages.txt solo con línea libgl1. "
            "En este monorepo deja solo un requirements.txt en la raíz o renombra los demás."
        )
        raise RuntimeError(f"PDF escaneado: falló import/carga OCR ({type(exc).__name__}: {exc}). {hint}") from exc

    try:
        ocr_engine = RapidOCR()
        doc = fitz.open(stream=data, filetype="pdf")
        page_texts = []
        scale = fitz.Matrix(150 / 72, 150 / 72)
        for page in doc:
            pix = page.get_pixmap(matrix=scale, alpha=False)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            arr = np.array(img)
            result, _elapsed = ocr_engine(arr)
            lines = []
            if result:
                for item in result:
                    # (box, text, score) o variantes
                    if isinstance(item, (list, tuple)) and len(item) >= 2 and isinstance(item[1], str):
                        lines.append(item[1])
            page_texts.append("\n".join(lines))
        doc.close()
        return "\n\n".join(t for t in page_texts if t.strip())
    except ImportError as exc:
        if "libGL" in str(exc) or "libgl" in str(exc).lower():
            raise RuntimeError(
                "Falta libGL u OpenCV GUI en el servidor: en requirements usa "
                "opencv-python-headless (y en Streamlit Cloud, packages.txt con libgl1)."
            ) from exc
        raise RuntimeError(f"Error de importación al cargar OCR: {exc}") from exc


def read_any_file(uploaded_file) -> Tuple[Dict[str, pd.DataFrame], str, str]:
    name = uploaded_file.name.lower()
    data = uploaded_file.getvalue()

    if name.endswith((".xlsx", ".xls")):
        sheets = pd.read_excel(io.BytesIO(data), sheet_name=None, header=None, dtype=str)
        return sheets, "excel", dataframe_to_blob_text(sheets)

    if name.endswith(".csv"):
        sheets = {"CSV": pd.read_csv(io.BytesIO(data), header=None, dtype=str)}
        return sheets, "csv", dataframe_to_blob_text(sheets)

    if name.endswith(".txt"):
        text = data.decode("utf-8", errors="ignore")
        sheets = {"TXT": pd.DataFrame([[line] for line in text.splitlines()])}
        return sheets, "txt", text

    if name.endswith(".pdf"):
        try:
            import pdfplumber
        except Exception as exc:
            raise RuntimeError("Para leer PDF instala pdfplumber: pip install pdfplumber") from exc

        pages = []
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
        text = "\n".join(pages)
        if not clean_multiline_text(text):
            text = ocr_pdf_bytes(data)
        sheets = {"PDF": pd.DataFrame([[line] for line in text.splitlines()])}
        return sheets, "pdf", text

    raise ValueError("Formato no soportado. Usa Excel, CSV, TXT o PDF.")


def find_value(text: str, labels: List[str], default: str = "") -> str:
    for label in labels:
        pattern = rf"{label}\s*[:\-]?\s*([^\n\r]+)"
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            value = re.split(
                r"\s{2,}|\t| FOLIO:| FECHA:| HORA:| CLIENTE:| COMPAÑÍA:| COMPANIA:",
                value,
                flags=re.IGNORECASE,
            )[0].strip()
            return value
    return default


def extract_depth(text: str) -> str:
    patterns = [
        r"prof(?:undidad)?\.?\s*(?:actual)?\s*[:\-]?\s*([0-9,]+(?:\.[0-9]+)?)\s*m",
        r"(?:md|depth)\s*[:\-]?\s*([0-9,]+(?:\.[0-9]+)?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).replace(",", "")
    return "0.0"


def extract_between_markers(text: str, start_markers: List[str], stop_markers: List[str]) -> str:
    start_idx = -1
    for marker in start_markers:
        match = re.search(marker, text, re.IGNORECASE)
        if match:
            start_idx = match.end()
            break

    if start_idx < 0:
        start_idx = 0

    sub = text[start_idx:]

    stops = []
    for marker in stop_markers:
        match = re.search(marker, sub, re.IGNORECASE)
        if match:
            stops.append(match.start())

    if stops:
        sub = sub[: min(stops)]

    return clean_multiline_text(sub)


def extract_following_value(text: str, label: str) -> str:
    pattern = rf"{label}\s*[:\-]?\s*(.*?)(?=\n\s*(?:Siguiente|Programa|% Solubilidad|Cromatograf[ií]a|Lecturas promedio|An[aá]lisis del lodo|Par[aá]metros de Perforaci[oó]n)\b|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return clean_text(match.group(1))


def normalize_time(value: str) -> str:
    value = clean_text(value).lower().replace("hrs", "").replace("hr", "").strip()
    match = re.match(r"^(\d{1,2})(?::?(\d{2}))?$", value)
    if not match:
        return value

    hour = int(match.group(1))
    minute = int(match.group(2) or 0)

    if hour == 24:
        return "24:00"

    return f"{hour % 24:02d}:{minute:02d}"


def time_to_minutes(value: str) -> int:
    value = normalize_time(value)
    if value == "24:00":
        return 1440
    match = re.match(r"^(\d{2}):(\d{2})$", value)
    if not match:
        return 0
    return int(match.group(1)) * 60 + int(match.group(2))


def normalize_activity_sequence(activities: List[Activity]) -> List[Activity]:
    fixed = []

    for activity in activities:
        start = normalize_time(activity.start)
        end = normalize_time(activity.end)
        start_minutes = time_to_minutes(start)
        end_minutes = time_to_minutes(end)

        if end_minutes < start_minutes and end != "24:00":
            fixed.append(Activity(start=start, end="24:00", text=activity.text))
            fixed.append(Activity(start="00:00", end=end, text=activity.text))
        elif end == "00:00" and start != "00:00":
            fixed.append(Activity(start=start, end="24:00", text=activity.text))
        else:
            fixed.append(Activity(start=start, end=end, text=activity.text))

    # Si hay continuidad después de 24:00, conserva el orden de aparición.
    has_next_day_tail = any(a.start == "00:00" and index > 0 for index, a in enumerate(fixed))
    if not has_next_day_tail:
        fixed = sorted(fixed, key=lambda a: (time_to_minutes(a.start), time_to_minutes(a.end)))

    return fixed


def split_activity_text(text: str) -> List[Activity]:
    operation_text = extract_between_markers(
        text,
        start_markers=[r"\bOPERACIONES\b", r"\bOPERACION\b", r"\bOPERACIÓN\b"],
        stop_markers=[
            r"\n\s*% Solubilidad\b",
            r"\n\s*Cromatograf[ií]a\b",
            r"\n\s*Lecturas promedio de gas\b",
            r"\n\s*An[aá]lisis del lodo\b",
            r"\n\s*Par[aá]metros de Perforaci[oó]n\b",
            r"\n\s*Estimaci[oó]n de presi[oó]n\b",
            r"\n\s*Datos de barrena\b",
            r"\n\s*Hidr[aá]ulica\b",
            r"\n\s*Observaciones\b",
        ],
    )

    pattern = re.compile(
        r"(?P<start>\b\d{1,2}:?\d{2})\s*(?:-|a|A)\s*(?P<end>\d{1,2}:?\d{2})\s*(?:hrs?\.?|horas)?",
        re.IGNORECASE,
    )
    matches = list(pattern.finditer(operation_text))

    activities = []
    for index, match in enumerate(matches):
        start = normalize_time(match.group("start"))
        end = normalize_time(match.group("end"))
        body_start = match.end()
        body_end = matches[index + 1].start() if index + 1 < len(matches) else len(operation_text)
        body = operation_text[body_start:body_end].strip(" .:-\n")

        body = re.split(r"\bSiguiente\s*[:\-]", body, flags=re.IGNORECASE)[0]
        body = re.split(r"\bPrograma\s*[:\-]", body, flags=re.IGNORECASE)[0]
        body = clean_text(body)

        if body:
            activities.append(Activity(start=start, end=end, text=body))

    return normalize_activity_sequence(activities)


def build_operation_text(activities: List[Activity], fallback_text: str) -> str:
    if not activities:
        fallback = extract_between_markers(
            fallback_text,
            start_markers=[r"\bOPERACIONES\b", r"\bOPERACION\b", r"\bOPERACIÓN\b"],
            stop_markers=[
                r"\n\s*% Solubilidad\b",
                r"\n\s*Cromatograf[ií]a\b",
                r"\n\s*Lecturas promedio de gas\b",
                r"\n\s*An[aá]lisis del lodo\b",
                r"\n\s*Par[aá]metros de Perforaci[oó]n\b",
                r"\n\s*Estimaci[oó]n de presi[oó]n\b",
                r"\n\s*Datos de barrena\b",
                r"\n\s*Hidr[aá]ulica\b",
                r"\n\s*Observaciones\b",
            ],
        )
        return fallback[:8000]

    return "\n\n".join(f"{a.start}-{a.end} hrs. {a.text}" for a in activities)


def validate_hour_sequence(activities: List[Activity]) -> List[str]:
    warnings = []
    previous_end = None

    for activity in activities:
        if previous_end and activity.start != previous_end:
            if not (previous_end == "24:00" and activity.start == "00:00"):
                warnings.append(f"Posible salto de horario: termina {previous_end} y la siguiente inicia {activity.start}.")
        previous_end = activity.end

    return warnings


def extract_report(raw_text: str) -> Dict[str, str]:
    blob = clean_multiline_text(raw_text)
    activities = split_activity_text(blob)

    return {
        "cliente": find_value(blob, ["CLIENTE", "CLIENT"], "PEMEX EXPLORACION Y PRODUCCION"),
        "compania": find_value(blob, ["COMPAÑÍA", "COMPANIA", "COMPANY"], ""),
        "pozo": find_value(blob, ["NOMBRE DEL POZO", "POZO", "WELL NAME", "WELL"], ""),
        "ciudad": find_value(blob, ["CIUDAD", "CITY"], ""),
        "estado": find_value(blob, ["ESTADO", "STATE"], ""),
        "folio": find_value(blob, ["FOLIO"], ""),
        "fecha": find_value(blob, ["FECHA", "DATE"], datetime.now().strftime("%d/%m/%Y")),
        "hora": find_value(blob, ["HORA", "TIME"], "24:00 hrs"),
        "profundidad": extract_depth(blob),
        "operacion_actual": "",
        "operacion": build_operation_text(activities, blob),
        "siguiente": extract_following_value(blob, "Siguiente"),
        "programa": extract_following_value(blob, "Programa"),
        "_activities": activities,
    }


def send_email_with_attachment(to_email: str, attachment_bytes: bytes, attachment_name: str, mime_type: str):
    smtp_server = get_secret("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(get_secret("SMTP_PORT", 587))
    smtp_user = get_secret("SMTP_USER")
    smtp_pass = get_secret("SMTP_PASS")

    if not smtp_user or not smtp_pass:
        raise RuntimeError("Faltan credenciales SMTP en .streamlit/secrets.toml. Configura SMTP_USER y SMTP_PASS antes de enviar.")

    msg = EmailMessage()
    msg["Subject"] = f"Daily Report para Rogii Email Parsing - {attachment_name}"
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.set_content("Hola,\n\nAdjunto el Daily Report convertido a formato general para lectura de Rogii/BOSS Dashboard.\n\nSaludos.")

    maintype, subtype = mime_type.split("/", 1)
    msg.add_attachment(attachment_bytes, maintype=maintype, subtype=subtype, filename=attachment_name)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def split_long_blocks(text: str, max_chars: int = 1200) -> List[str]:
    blocks = [block.strip() for block in re.split(r"\n\s*\n+", text) if block.strip()]
    if not blocks:
        return ["-"]

    output = []
    for block in blocks:
        block = re.sub(r"\s+", " ", block).strip()
        while len(block) > max_chars:
            cut = block.rfind(". ", 0, max_chars)
            if cut < int(max_chars * 0.45):
                cut = block.rfind(" ", 0, max_chars)
            if cut <= 0:
                cut = max_chars
            output.append(block[:cut].strip())
            block = block[cut:].strip(" .")
        if block:
            output.append(block)
    return output


def make_pdf(report: Dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=12 * mm,
        leftMargin=12 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
    )

    styles = getSampleStyleSheet()
    normal = ParagraphStyle("normal", parent=styles["Normal"], fontName="Helvetica", fontSize=8, leading=10, alignment=TA_LEFT)
    bold = ParagraphStyle("bold", parent=normal, fontName="Helvetica-Bold")
    title = ParagraphStyle("title", parent=bold, fontSize=11, leading=13, alignment=TA_CENTER)
    section = ParagraphStyle("section", parent=bold, fontSize=9, leading=11, alignment=TA_LEFT)
    operation_style = ParagraphStyle(
        "operation",
        parent=normal,
        fontSize=8,
        leading=10,
        borderWidth=0.35,
        borderColor=BORDER,
        borderPadding=4,
        spaceAfter=4,
    )

    story = []

    header = Table([[Paragraph("REPORTE DIARIO DE OPERACIÓN", title)]], colWidths=[186 * mm])
    header.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
        ("BACKGROUND", (0, 0), (-1, -1), GREEN),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(header)
    story.append(Spacer(1, 8))

    metadata = Table([
        [Paragraph(f"<b>CLIENTE:</b> {xml_escape(report.get('cliente', ''))}", normal), Paragraph(f"<b>FOLIO:</b> {xml_escape(report.get('folio', ''))}", normal)],
        [Paragraph(f"<b>COMPAÑÍA:</b> {xml_escape(report.get('compania', ''))}", normal), Paragraph(f"<b>FECHA:</b> {xml_escape(report.get('fecha', ''))}", normal)],
        [Paragraph(f"<b>NOMBRE DEL POZO:</b> {xml_escape(report.get('pozo', ''))}", normal), Paragraph(f"<b>HORA:</b> {xml_escape(report.get('hora', ''))}", normal)],
        [Paragraph(f"<b>CIUDAD:</b> {xml_escape(report.get('ciudad', ''))}", normal), Paragraph(f"<b>ESTADO:</b> {xml_escape(report.get('estado', ''))}", normal)],
        [Paragraph(f"<b>PROFUNDIDAD ACTUAL (m):</b> {xml_escape(report.get('profundidad', ''))}", normal), Paragraph(f"<b>OPERACIÓN ACTUAL:</b> {xml_escape(report.get('operacion_actual', ''))}", normal)],
    ], colWidths=[120 * mm, 66 * mm])
    metadata.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.4, BORDER),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(metadata)
    story.append(Spacer(1, 8))

    op_header = Table([[Paragraph("OPERACIONES", section)]], colWidths=[186 * mm])
    op_header.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
        ("BACKGROUND", (0, 0), (-1, -1), GREEN),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(op_header)

    for block in split_long_blocks(report.get("operacion", "")):
        story.append(Paragraph(xml_escape(block), operation_style))

    if clean_text(report.get("siguiente", "")):
        story.append(Paragraph(f"<b>Siguiente:</b> {xml_escape(report.get('siguiente', ''))}", operation_style))

    if clean_text(report.get("programa", "")):
        story.append(Paragraph(f"<b>Programa:</b> {xml_escape(report.get('programa', ''))}", operation_style))

    doc.build(story)
    return buffer.getvalue()


st.markdown("""
<style>
.block-container { padding-top: 1.2rem; max-width: 1400px; }
.stDownloadButton button, .stButton button { border-radius: 12px; font-weight: 700; }
</style>
""", unsafe_allow_html=True)

st.title("Conversor de Daily Report a formato general para Rogii Email Parsing")
st.caption("Carga un Daily Report en Excel, CSV, TXT o PDF. La app extrae operaciones, elimina secciones no necesarias y genera un PDF simple para lectura por email parsing.")

with st.sidebar:
    st.header("Parsing Email")
    st.caption("Las credenciales SMTP se leen desde .streamlit/secrets.toml y no se muestran en la app.")
    sender_email = get_secret("SMTP_USER", "No configurado")
    st.text_input("From email", value=sender_email, disabled=True)
    to_email = st.text_input("To email parsing", value="solobox+pemex@rogii.com")

uploaded = st.file_uploader("Sube Daily Report", type=["xlsx", "xls", "csv", "txt", "pdf"])

if uploaded:
    try:
        sheets, file_type, raw_text = read_any_file(uploaded)
        report = extract_report(raw_text)
        st.success(f"Archivo leído como {file_type.upper()}")

        left, right = st.columns([1, 1])

        with left:
            st.subheader("Campos detectados / editables")
            for key in ["cliente", "compania", "pozo", "ciudad", "estado", "folio", "fecha", "hora", "profundidad"]:
                report[key] = st.text_input(key.replace("_", " ").title(), value=report.get(key, ""))

            report["operacion_actual"] = st.text_input("Operación Actual", value=report.get("operacion_actual", ""))
            report["siguiente"] = st.text_area("Siguiente", value=report.get("siguiente", ""), height=80)
            report["programa"] = st.text_area("Programa", value=report.get("programa", ""), height=100)

            well_for_file = sanitize_filename(report.get("pozo", ""))
            date_for_file = sanitize_filename(report.get("fecha", "").replace("/", "-"), datetime.now().strftime("%d-%m-%Y"))
            default_output_name = f"{well_for_file}_Daily_Report_{date_for_file}.pdf"

            st.markdown("### 📎 Nombre del archivo adjunto de salida")
            output_name = st.text_input(
                "Así quedará el nombre del PDF que se descargará o enviará por email:",
                value=default_output_name,
                help="Puedes editarlo antes de descargar o enviar. Se recomienda mantener el nombre del pozo en el archivo.",
            )
            output_name = sanitize_filename(output_name.replace(".pdf", "")) + ".pdf"

        with right:
            st.subheader("Operaciones normalizadas")
            st.info("Se eliminan cromatografía, lecturas de gas, lodo, parámetros, hidráulica y observaciones. Solo se envían metadatos + operaciones + siguiente/programa.")

            activities = split_activity_text(raw_text)
            warnings = validate_hour_sequence(activities)
            if warnings:
                st.warning("\n".join(warnings))

            report["operacion"] = st.text_area("Texto de operaciones", value=report.get("operacion", ""), height=520)

        pdf_bytes = make_pdf(report)

        c1, c2 = st.columns(2)
        with c1:
            st.download_button("⬇️ Descargar PDF", data=pdf_bytes, file_name=output_name, mime="application/pdf", use_container_width=True)

        with c2:
            if st.button("📧 Enviar por email", type="primary", use_container_width=True):
                if not to_email:
                    st.error("Completa el correo destino antes de enviar.")
                else:
                    send_email_with_attachment(to_email, pdf_bytes, output_name, "application/pdf")
                    st.success(f"PDF enviado correctamente a {to_email} con el archivo {output_name}")

        with st.expander("Vista previa del texto fuente detectado"):
            st.text(clean_multiline_text(raw_text)[:12000])

    except Exception as exc:
        st.exception(exc)

else:
    st.info("Esperando archivo. Ejemplo: Daily Report en Excel, CSV, TXT o PDF.")
