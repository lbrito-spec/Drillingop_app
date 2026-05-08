
import io
import re
import smtplib
import imaplib
import email
from email.header import decode_header
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from typing import Dict, List, Optional, Tuple

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



def normalize_date(value: str) -> str:
    """Convierte Apr-28-2026, 2026-04-28, 04/28/2026, etc. a dd/mm/yyyy."""
    value = clean_text(value)
    if not value:
        return datetime.now().strftime("%d/%m/%Y")

    value = re.split(
        r"\s+(?:WELLBORE|TARGET|FORMATION|LOCATION|STATE|OPERATOR|CONTRACTOR|RIG|REPORT|JOB)\b",
        value,
        flags=re.IGNORECASE,
    )[0].strip()

    value = value.replace(".", "-").replace("_", "-")

    for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%b-%d-%Y", "%B-%d-%Y", "%d-%b-%Y", "%d-%B-%Y", "%m/%d/%Y", "%m-%d-%Y"]:
        try:
            return datetime.strptime(value, fmt).strftime("%d/%m/%Y")
        except Exception:
            pass

    match = re.search(r"([A-Za-z]{3,9})[-/ ](\d{1,2})[-/ ](\d{4})", value)
    if match:
        candidate = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        for fmt in ["%b-%d-%Y", "%B-%d-%Y"]:
            try:
                return datetime.strptime(candidate, fmt).strftime("%d/%m/%Y")
            except Exception:
                pass

    return value


def clean_operation_text(text: str) -> str:
    """Quita encabezados OCR y limpia repeticiones obvias."""
    text = clean_multiline_text(text)

    text = re.sub(
        r"(?:Resumen operacional:\s*)?REPORTE DIARIO DE OPERACIÓN\s+CLIENTE/OPERADOR:.*?OPERACIONES\s*",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(
        r"CLIENTE/OPERADOR:.*?PROFUNDIDAD ACTUAL\s*\(m\):.*?OPERACIONES\s*",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    return clean_multiline_text(text)


def deduplicate_activities(activities: List[Activity]) -> List[Activity]:
    """Elimina actividades repetidas generadas por OCR y descarta 00:00-00:00."""
    seen = set()
    unique: List[Activity] = []

    for activity in activities:
        if activity.start == activity.end:
            continue

        normalized_text = re.sub(r"\s+", " ", clean_text(activity.text)).strip()
        key = (activity.start, activity.end, normalized_text[:180].lower())

        if key in seen:
            continue

        seen.add(key)
        unique.append(Activity(activity.start, activity.end, normalized_text))

    return unique


def dataframe_to_blob_text(sheets: Dict[str, pd.DataFrame]) -> str:
    chunks = []
    for sheet_name, df in sheets.items():
        chunks.append(f"\n--- SHEET: {sheet_name} ---\n")
        for _, row in df.iterrows():
            line = " ".join(clean_text(v) for v in row.tolist() if clean_text(v))
            if line:
                chunks.append(line)
    return "\n".join(chunks)


# ============================================================
# OCR fallback para PDFs tipo Baker Hughes escaneados/imagen
# ============================================================

def ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    """
    Fallback OCR para PDFs sin texto embebido.
    Requiere en requirements.txt:
      PyMuPDF
      rapidocr-onnxruntime
      pillow
      numpy
    """
    try:
        import fitz  # PyMuPDF
        import numpy as np
        from PIL import Image
        from rapidocr_onnxruntime import RapidOCR
    except Exception as exc:
        raise RuntimeError(
            f"OCR import error real: {type(exc).__name__}: {exc}\n\n"
            "El PDF no trae texto seleccionable y se necesita OCR. "
            "Verifica dependencias: PyMuPDF, rapidocr-onnxruntime, onnxruntime, Pillow, numpy. "
            "También confirma runtime.txt en la raíz con python-3.11."
        ) from exc

    engine = RapidOCR()
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages_text = []

    for page_index, page in enumerate(doc):
        # 2x da mejor lectura de tablas sin hacer el archivo demasiado pesado
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
        result, _ = engine(np.array(img))

        if not result:
            pages_text.append("")
            continue

        items = []
        for row in result:
            try:
                box, txt, score = row
                # Convertimos score a float por si viene como string u otro tipo
                f_score = float(score) if score is not None else 0.0
                if f_score < 0.35:
                    continue
            except (ValueError, TypeError):
                continue

            xs = [p[0] for p in box]
            ys = [p[1] for p in box]
            items.append((min(ys), min(xs), clean_text(txt)))

        items.sort(key=lambda x: (x[0], x[1]))

        # Agrupa palabras/frases OCR por línea aproximada
        lines = []
        current_y = None
        current = []
        for y, x, txt in items:
            if current_y is None or abs(y - current_y) <= 12:
                current.append((x, txt))
                current_y = y if current_y is None else (current_y * 0.7 + y * 0.3)
            else:
                current.sort(key=lambda t: t[0])
                lines.append(" ".join(t for _, t in current))
                current = [(x, txt)]
                current_y = y

        if current:
            current.sort(key=lambda t: t[0])
            lines.append(" ".join(t for _, t in current))

        pages_text.append(f"\n--- OCR PAGE {page_index + 1} ---\n" + "\n".join(lines))

    return "\n".join(pages_text)


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
        text = ""
        try:
            import pdfplumber
            pages = []
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                for page in pdf.pages:
                    pages.append(page.extract_text() or "")
            text = "\n".join(pages).strip()
        except Exception:
            text = ""

        # Si no hay texto útil, usa OCR
        if len(re.sub(r"\s+", "", text)) < 80:
            text = ocr_pdf_bytes(data)

        sheets = {"PDF": pd.DataFrame([[line] for line in text.splitlines()])}
        return sheets, "pdf", text

    raise ValueError("Formato no soportado. Usa Excel, CSV, TXT o PDF.")




@dataclass
class EmailReport:
    subject: str
    sender: str
    date: str
    filename: str
    raw_text: str
    source_type: str
    uid: str
    body_text: str = ""


class BytesUpload:
    """Adapter pequeño para reutilizar read_any_file() con adjuntos de correo."""
    def __init__(self, name: str, data: bytes):
        self.name = name
        self._data = data

    def getvalue(self) -> bytes:
        return self._data


def decode_mime_header(value: str) -> str:
    if not value:
        return ""
    parts = []
    for text, enc in decode_header(value):
        if isinstance(text, bytes):
            parts.append(text.decode(enc or "utf-8", errors="ignore"))
        else:
            parts.append(text)
    return clean_text("".join(parts))


def html_to_text(value: str) -> str:
    value = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", value)
    value = re.sub(r"(?i)<br\s*/?>", "\n", value)
    value = re.sub(r"(?i)</p>", "\n", value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = value.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return clean_multiline_text(value)


def extract_text_from_email_message(msg) -> str:
    chunks = []
    for part in msg.walk():
        content_type = part.get_content_type()
        disposition = str(part.get("Content-Disposition") or "").lower()
        if "attachment" in disposition:
            continue
        if content_type not in ("text/plain", "text/html"):
            continue
        payload = part.get_payload(decode=True)
        if not payload:
            continue
        charset = part.get_content_charset() or "utf-8"
        text = payload.decode(charset, errors="ignore")
        if content_type == "text/html":
            text = html_to_text(text)
        chunks.append(text)
    return clean_multiline_text("\n\n".join(chunks))


def extract_ultimas_24hrs_from_body(text: str) -> str:
    """
    Busca en el cuerpo del correo (texto/HTML ya plano) el párrafo bajo «Ultimas 24Hrs» / «Últimas 24 hrs».
    Devuelve solo el texto narrativo (sin repetir la etiqueta), sin adjuntos.
    Prioriza la etiqueta tipo tabla «Ultimas 24Hrs:» y la última aparición si el hilo trae citas arriba.
    """
    if not text or not str(text).strip():
        return ""

    normalized = str(text).replace("\r\n", "\n").replace("\r", "\n")

    def _last_match(pattern: str):
        found = list(re.finditer(pattern, normalized, re.IGNORECASE))
        return found[-1] if found else None

    # Orden: primero 24Hrs pegado (evita enganchar solo «Ultimas 24 Hrs.» antes del bloque real).
    m = (
        _last_match(r"(?:Últimas|Ultimas)\s+24Hrs\s*:")
        or _last_match(r"(?:Últimas|Ultimas)\s+24\s+Hrs\s*:")
        or _last_match(r"(?:Últimas|Ultimas)\s+24\s*(?:hrs|Hrs|HRS|horas)\s*:")
        or _last_match(r"(?:Últimas|Ultimas)\s+24\s*(?:hrs|Hrs|HRS|horas)\b\.?")
        or _last_match(r"Last\s+24\s*(?:hours|hrs)\s*:?")
    )
    if not m:
        return ""

    rest = normalized[m.end() :]
    rest = re.sub(r"^[\s\n]+", "", rest)
    # Por si quedó pegada otra vez la misma etiqueta (p. ej. «...24 Hrs.Ultimas 24Hrs:»)
    rest = re.sub(
        r"^[\s.]*(?:Últimas|Ultimas)\s+24(?:Hrs|\s*(?:hrs|Hrs|HRS|horas))\s*:?\s*",
        "",
        rest,
        count=1,
        flags=re.IGNORECASE,
    )

    # Fin de celda/fila: con salto de línea, al inicio de línea, o en la misma línea tras el párrafo (HTML colapsado).
    stop_re = re.compile(
        r"(?:"
        r"^\s*(?:Programa|Siguiente|Operaciones|OPERACIONES)\s*:|"
        r"\n\s*(?:Programa|Siguiente|Operaciones)\s*:|"
        r"\.\s+(?:Programa|Siguiente)\s*:|"
        r"\s{2,}(?:Programa|Siguiente)\s*:\s*|"
        r"(?:BIT|MOTOR)\s+DATA\b|"
        r"BHA\s+info\b|BHA\s+Configuration\b|"
        r"Daily Activity Summary\b|"
        r"%\s*Solubilidad\b|Cromatograf"
        r")",
        re.IGNORECASE | re.MULTILINE,
    )
    sm = stop_re.search(rest)
    if sm:
        rest = rest[: sm.start()]

    return rest.rstrip("\n \t")


def read_email_attachments_or_body(msg) -> Tuple[str, str, str]:
    """
    Devuelve filename, raw_text, source_type.
    Prioridad: adjuntos PDF/Excel/CSV/TXT. Si no existen, usa el cuerpo del correo.
    """
    allowed = (".xlsx", ".xls", ".csv", ".txt", ".pdf")
    for part in msg.walk():
        filename = decode_mime_header(part.get_filename() or "")
        if not filename or not filename.lower().endswith(allowed):
            continue
        payload = part.get_payload(decode=True)
        if not payload:
            continue
        sheets, source_type, raw_text = read_any_file(BytesUpload(filename, payload))
        return filename, raw_text, source_type

    body_text = extract_text_from_email_message(msg)
    return "email_body.txt", body_text, "email_body"


def latest_report_from_inbox(sender_filter: str = "", subject_filter: str = "", mailbox: str = "INBOX") -> Optional[EmailReport]:
    imap_server = get_secret("IMAP_SERVER", "imap.gmail.com")
    imap_port = int(get_secret("IMAP_PORT", 993))
    imap_user = get_secret("IMAP_USER", get_secret("SMTP_USER"))
    imap_pass = get_secret("IMAP_PASS", get_secret("SMTP_PASS"))

    if not imap_user or not imap_pass:
        raise RuntimeError("Faltan credenciales IMAP en .streamlit/secrets.toml. Configura IMAP_USER/IMAP_PASS o reutiliza SMTP_USER/SMTP_PASS.")

    criteria = ["UNSEEN"]
    if sender_filter:
        criteria += ["FROM", f'"{sender_filter}"']
    if subject_filter:
        criteria += ["SUBJECT", f'"{subject_filter}"']

    with imaplib.IMAP4_SSL(imap_server, imap_port) as imap:
        imap.login(imap_user, imap_pass)
        imap.select(mailbox)
        status, data = imap.uid("search", None, *criteria)
        if status != "OK" or not data or not data[0]:
            return None

        uid = data[0].split()[-1]
        status, msg_data = imap.uid("fetch", uid, "(RFC822)")
        if status != "OK":
            return None

        msg = email.message_from_bytes(msg_data[0][1])
        body_text = extract_text_from_email_message(msg)
        filename, raw_text, source_type = read_email_attachments_or_body(msg)
        if not clean_text(raw_text):
            return None

        # Marca como leído solo después de poder extraer texto útil.
        imap.uid("store", uid, "+FLAGS", "(\\Seen)")

        return EmailReport(
            subject=decode_mime_header(msg.get("Subject", "")),
            sender=decode_mime_header(msg.get("From", "")),
            date=decode_mime_header(msg.get("Date", "")),
            filename=filename,
            raw_text=raw_text,
            source_type=source_type,
            uid=uid.decode("ascii", errors="ignore"),
            body_text=body_text,
        )


def force_24h_operation_if_summary_only(report: Dict[str, str], raw_text: str) -> Dict[str, str]:
    """
    Para clientes que solo mandan resumen de 24 horas, crea una sola actividad 00:00-24:00.
    Así el parser de email/feed puede leer el bloque como operación de día completo.
    """
    if report.get("_operacion_cuerpo_correo"):
        return report

    activities = report.get("_activities") or []
    operation = clean_operation_text(report.get("operacion", ""))

    already_has_range = bool(re.search(r"\b\d{1,2}:\d{2}\s*-\s*(?:\d{1,2}:\d{2}|24:00)\b", operation))
    if activities or already_has_range:
        return report

    summary = extract_baker_operational_summary(raw_text) or operation or clean_operation_text(raw_text[:4000])
    summary = clean_operation_text(summary)
    if summary:
        report["operacion"] = f"00:00-24:00 hrs. {summary}"
        report["_activities"] = [Activity("00:00", "24:00", summary)]
    return report


# ============================================================
# Extracción de campos
# ============================================================

def find_value(text: str, labels: List[str], default: str = "") -> str:
    for label in labels:
        # 1) Formato normal: LABEL: valor
        pattern = rf"{label}\s*[:\-]?\s*([^\n\r]+)"
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            value = re.split(
                r"\s{2,}|\t| FOLIO:| FECHA:| HORA:| CLIENTE:| COMPAÑÍA:| COMPANIA:| OPERATOR:| WELLBORE:| REPORT DATE:| REPORT CREATED:",
                value,
                flags=re.IGNORECASE,
            )[0].strip()
            if value:
                return value

    return default


def find_value_after_label_in_same_line(text: str, label: str, default: str = "") -> str:
    """
    Ayuda para tablas OCR donde aparece:
      OPERATOR: Geopark JOB NO: 113...
    """
    pattern = rf"{label}\s*[:\-]?\s*([A-Za-z0-9ÁÉÍÓÚÜÑáéíóúüñ()./#_\- ]+?)(?=\s+[A-Z][A-Z /()]+:|\n|$)"
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return default
    value = clean_text(match.group(1))
    return value or default


def extract_depth(text: str) -> str:
    patterns = [
        r"expected\s+td\s*/?\s*depth\s*[:\-]?\s*(?:m\s*)?([0-9,]+(?:\.[0-9]+)?)",
        r"end\s+drilling\s*[:\-]?\s*(?:m\s*)?([0-9,]+(?:\.[0-9]+)?)",
        r"midnight\s+depth\s*(?:m)?\s*([0-9,]+(?:\.[0-9]+)?)",
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
    pattern = rf"{label}\s*[:\-]?\s*(.*?)(?=\n\s*(?:Siguiente|Programa|% Solubilidad|Cromatograf[ií]a|Lecturas promedio|An[aá]lisis del lodo|Par[aá]metros de Perforaci[oó]n|Daily Activity Summary|Latest Survey|Wellbore|BHA Configuration)\b|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return clean_text(match.group(1))


# ============================================================
# Actividades / horas
# ============================================================

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

    has_next_day_tail = any(a.start == "00:00" and index > 0 for index, a in enumerate(fixed))
    if not has_next_day_tail:
        fixed = sorted(fixed, key=lambda a: (time_to_minutes(a.start), time_to_minutes(a.end)))

    return fixed


def parse_standard_operations(text: str) -> List[Activity]:
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

    if not re.search(r"\bOPERACIONES\b|\bOPERACION\b|\bOPERACIÓN\b", text, re.IGNORECASE):
        return []

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


def parse_baker_daily_activity(text: str) -> List[Activity]:
    """
    Parser para Baker Hughes DDR.
    Busca la tabla 'Daily Activity Summary' que trae:
      START TIME | END TIME | DURATION | ACTIVITY | COMMENTS
    Funciona con texto OCR o texto extraído del PDF.
    """
    section = extract_between_markers(
        text,
        start_markers=[r"Daily Activity Summary", r"Daily Activity"],
        stop_markers=[r"\n\s*file:", r"\n\s*\d+\s*/\s*\d+\s*$"],
    )

    if not re.search(r"Daily Activity", text, re.IGNORECASE):
        return []

    # Une para que filas OCR partidas se puedan leer.
    compact = re.sub(r"[ \t]+", " ", section)
    compact = re.sub(r"\n+", " ", compact)

    # Encuentra todas las posiciones con par start/end.
    time_pair = re.compile(r"(?P<start>\b\d{2}:\d{2})\s+(?P<end>\d{2}:\d{2})")
    matches = list(time_pair.finditer(compact))

    activities: List[Activity] = []
    for i, match in enumerate(matches):
        start = normalize_time(match.group("start"))
        end = normalize_time(match.group("end"))
        body_start = match.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(compact)
        body = compact[body_start:body_end].strip(" .:-")

        # Quita columnas numéricas iniciales: start depth/end depth/duration.
        body = re.sub(r"^(?:\d+(?:\.\d+)?\s+){0,4}", "", body).strip()

        # Limpia encabezados que se pegan por OCR.
        body = re.sub(
            r"START TIME|END TIME|START DEPTH|END DEPTH|DURATION|ACTIVITY|COMMENTS",
            "",
            body,
            flags=re.IGNORECASE,
        )
        body = clean_text(body)

        if body and not re.fullmatch(r"\d+(?:\.\d+)?", body):
            activities.append(Activity(start=start, end=end, text=body))

    return normalize_activity_sequence(activities)


def split_activity_text(text: str) -> List[Activity]:
    activities = parse_standard_operations(text)
    if activities:
        return deduplicate_activities(activities)

    activities = parse_baker_daily_activity(text)
    if activities:
        return deduplicate_activities(activities)

    return []


def build_operation_text(activities: List[Activity], fallback_text: str) -> str:
    if not activities:
        fallback = extract_between_markers(
            fallback_text,
            start_markers=[
                r"\bOPERACIONES\b",
                r"\bOPERACION\b",
                r"\bOPERACIÓN\b",
                r"Operational Summary",
                r"Daily Activity Summary",
            ],
            stop_markers=[
                r"\n\s*% Solubilidad\b",
                r"\n\s*Cromatograf[ií]a\b",
                r"\n\s*Lecturas promedio de gas\b",
                r"\n\s*An[aá]lisis del lodo\b",
                r"\n\s*Par[aá]metros de Perforaci[oó]n\b",
                r"\n\s*Latest Survey",
                r"\n\s*Wellbore",
                r"\n\s*BHA Configuration",
            ],
        )
        return clean_operation_text(fallback[:8000])

    operation_text = "\n\n".join(f"{a.start}-{a.end} hrs. {a.text}" for a in activities)
    return clean_operation_text(operation_text)


def validate_hour_sequence(activities: List[Activity]) -> List[str]:
    warnings = []
    previous_end = None

    for activity in activities:
        if previous_end and activity.start != previous_end:
            if not (previous_end == "24:00" and activity.start == "00:00"):
                warnings.append(f"Posible salto de horario: termina {previous_end} y la siguiente inicia {activity.start}.")
        previous_end = activity.end

    return warnings


def extract_baker_operational_summary(text: str) -> str:
    if not re.search(r"Operational Summary", text, re.IGNORECASE):
        return ""

    section = extract_between_markers(
        text,
        start_markers=[r"Operational Summary"],
        stop_markers=[
            r"\n\s*24\s*Hr Tracking",
            r"\n\s*Drilling Parameters",
            r"\n\s*Fluid Parameters",
            r"\n\s*Latest Survey",
            r"\n\s*Daily Activity Summary",
        ],
    )
    section = re.sub(r"DAILY OPERATIONS", "", section, flags=re.IGNORECASE)
    section = re.sub(r"24 HOUR FORECAST.*", "", section, flags=re.IGNORECASE | re.DOTALL)
    section = clean_operation_text(section)

    if re.search(r"REPORTE DIARIO|CLIENTE/OPERADOR|NOMBRE DEL POZO|PROFUNDIDAD ACTUAL", section, re.IGNORECASE):
        return ""

    return clean_text(section)

def extract_report(raw_text: str, uploaded_name: str = "", email_body: Optional[str] = None) -> Dict[str, str]:
    blob = clean_multiline_text(raw_text)
    activities = split_activity_text(blob)
    operacion_cuerpo_correo = False

    pozo = (
        find_value_after_label_in_same_line(blob, "WELLBORE")
        or find_value(blob, ["NOMBRE DEL POZO", "POZO", "WELL NAME", "WELLBORE", "WELL"], "")
    )

    if not pozo and uploaded_name:
        # Ej: LJE-1031(h)_DDR 9_04_28_2026.pdf
        m = re.search(r"([A-Za-z]{2,5}-\d{3,5}\(?[A-Za-z]?\)?)", uploaded_name)
        if m:
            pozo = m.group(1)

    cliente = (
        find_value_after_label_in_same_line(blob, "OPERATOR")
        or find_value(blob, ["CLIENTE", "CLIENT", "OPERATOR"], "PEMEX EXPLORACION Y PRODUCCION")
    )

    compania = (
        find_value_after_label_in_same_line(blob, "CONTRACTOR")
        or find_value(blob, ["COMPAÑÍA", "COMPANIA", "COMPANY", "CONTRACTOR"], "")
    )

    fecha_raw = (
        find_value_after_label_in_same_line(blob, "REPORT DATE")
        or find_value_after_label_in_same_line(blob, "REPORT CREATED")
        or find_value(blob, ["FECHA", "DATE", "REPORT DATE", "REPORT CREATED"], "")
    )
    fecha = normalize_date(fecha_raw)

    ciudad_estado = (
        find_value_after_label_in_same_line(blob, "STATE/PROVINCE")
        or find_value(blob, ["ESTADO", "STATE/PROVINCE", "STATE"], "")
    )

    operational_summary = extract_baker_operational_summary(blob)

    if email_body:
        ultimas_24 = extract_ultimas_24hrs_from_body(email_body)
        if ultimas_24:
            operation_text = f"00:00 a 24:00\n\n{ultimas_24}"
            activities = []
            operacion_cuerpo_correo = True
        else:
            operation_text = build_operation_text(activities, blob)
    else:
        operation_text = build_operation_text(activities, blob)

    if not operacion_cuerpo_correo and operational_summary and operational_summary.lower() not in operation_text.lower():
        operation_text = f"Resumen operacional: {operational_summary}\n\n{operation_text}"

    if operacion_cuerpo_correo:
        operation_text = operation_text.rstrip("\n \t")
    else:
        operation_text = clean_operation_text(operation_text)

    return {
        "cliente": cliente,
        "compania": compania,
        "pozo": pozo,
        "ciudad": find_value(blob, ["CIUDAD", "CITY", "LOCATION"], ""),
        "estado": ciudad_estado,
        "folio": find_value_after_label_in_same_line(blob, "JOB NO") or find_value(blob, ["FOLIO", "JOB NO"], ""),
        "fecha": fecha,
        "hora": "24:00 hrs",
        "profundidad": extract_depth(blob),
        "operacion_actual": "",
        "operacion": operation_text,
        "siguiente": extract_following_value(blob, "Siguiente"),
        "programa": extract_following_value(blob, "Programa"),
        "_activities": activities,
        "_operacion_cuerpo_correo": operacion_cuerpo_correo,
    }


# ============================================================
# Email
# ============================================================

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


# ============================================================
# PDF general sin logo ni tablas extra
# ============================================================

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
    operation_style = ParagraphStyle("operation", parent=normal, fontSize=8, leading=10, borderWidth=0.35, borderColor=BORDER, borderPadding=4, spaceAfter=4)

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
        [Paragraph(f"<b>CLIENTE/OPERADOR:</b> {xml_escape(report.get('cliente', ''))}", normal), Paragraph(f"<b>FOLIO/JOB:</b> {xml_escape(report.get('folio', ''))}", normal)],
        [Paragraph(f"<b>COMPAÑÍA/CONTRATISTA:</b> {xml_escape(report.get('compania', ''))}", normal), Paragraph(f"<b>FECHA:</b> {xml_escape(report.get('fecha', ''))}", normal)],
        [Paragraph(f"<b>NOMBRE DEL POZO:</b> {xml_escape(report.get('pozo', ''))}", normal), Paragraph(f"<b>HORA:</b> {xml_escape(report.get('hora', ''))}", normal)],
        [Paragraph(f"<b>CIUDAD/LOCATION:</b> {xml_escape(report.get('ciudad', ''))}", normal), Paragraph(f"<b>ESTADO:</b> {xml_escape(report.get('estado', ''))}", normal)],
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


# ============================================================
# UI
# ============================================================

st.markdown("""
<style>
.block-container { padding-top: 1.2rem; max-width: 1400px; padding-bottom: 7.5rem; }
.stDownloadButton button, .stButton button { border-radius: 12px; font-weight: 700; }

.rogii-title-row {
  display: flex;
  align-items: center;
  gap: 0.65rem;
  margin: 0 0 0.35rem 0;
  flex-wrap: nowrap;
}
.rogii-title-flame {
  font-size: clamp(2.4rem, 6vw, 3.5rem);
  line-height: 1;
  flex-shrink: 0;
  filter: drop-shadow(0 0 10px rgba(255, 120, 40, 0.55));
}
.rogii-title-text {
  font-size: clamp(1.25rem, 2.4vw, 1.85rem);
  font-weight: 700;
  margin: 0;
  padding: 0;
  line-height: 1.2;
  letter-spacing: -0.02em;
  color: var(--text-color, inherit);
}

.rogii-signature-footer {
  position: fixed;
  bottom: 0.85rem;
  right: 1.1rem;
  z-index: 999999;
  text-align: right;
  font-family: "Source Sans Pro", ui-sans-serif, system-ui, sans-serif;
  pointer-events: none;
  max-width: min(22rem, 92vw);
}
.rogii-signature-footer .sig-line1 {
  color: var(--text-color, #f5f5f5);
  font-size: 0.95rem;
  font-weight: 500;
  margin: 0;
  line-height: 1.35;
}
.rogii-signature-footer .sig-line1 b { font-weight: 700; }
.rogii-signature-footer .sig-line2 {
  color: var(--text-color, #f5f5f5);
  opacity: 0.72;
  font-size: 0.68rem;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin: 0.15rem 0 0 0;
  line-height: 1.3;
}
.rogii-signature-footer .sig-sep {
  border: none;
  border-top: 1px solid rgba(140, 140, 150, 0.45);
  margin: 0.55rem 0 0.45rem 0;
  width: 100%;
}
.rogii-signature-footer .sig-brand {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 0.35rem;
  margin: 0;
}
.rogii-signature-footer .sig-brand-flame {
  font-size: 1.15rem;
  line-height: 1;
  filter: drop-shadow(0 0 6px rgba(255, 140, 60, 0.5));
}
.rogii-signature-footer .sig-brand-text {
  font-size: 1rem;
  font-weight: 800;
  letter-spacing: 0.04em;
  color: var(--text-color, #f5f5f5);
}
</style>
""", unsafe_allow_html=True)

st.markdown(
    """
<div class="rogii-signature-footer" aria-label="Firma">
  <p class="sig-line1">Elaborado por <b>Lenin Brito</b></p>
  <p class="sig-line2">DRILLING OPTIMIZATION LEAD</p>
  <hr class="sig-sep" />
  <div class="sig-brand">
    <span class="sig-brand-flame" aria-hidden="true">🔥</span>
    <span class="sig-brand-text">ROGII</span>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

st.markdown(
    """
<div class="rogii-title-row">
  <span class="rogii-title-flame" aria-hidden="true">🔥</span>
  <h1 class="rogii-title-text">Conversor de Daily Report a formato general para Rogii Email Parsing</h1>
</div>
""",
    unsafe_allow_html=True,
)
st.caption(
    "Carga un Daily Report en Excel, CSV, TXT o PDF. "
    "Incluye soporte OCR para PDFs tipo Baker Hughes que vienen como imagen."
)

with st.sidebar:
    st.header("Parsing Email")
    st.caption("Las credenciales SMTP se leen desde .streamlit/secrets.toml y no se muestran en la app.")
    sender_email = get_secret("SMTP_USER", "No configurado")
    st.text_input("From email", value=sender_email, disabled=True)
    to_email = st.text_input("To email parsing", value="solobox+pemex@rogii.com")

    st.divider()
    st.header("Monitoreo de correo del cliente")
    st.caption("Busca correos nuevos cada cierto intervalo y carga el reporte recibido en la app.")
    monitor_enabled = st.toggle("Buscar reportes por correo", value=False)
    poll_seconds = st.number_input("Intervalo de búsqueda (segundos)", min_value=30, max_value=3600, value=60, step=30)
    inbox_sender_filter = st.text_input("Filtrar remitente del cliente", value=get_secret("CLIENT_REPORT_FROM", ""))
    inbox_subject_filter = st.text_input("Filtrar asunto", value=get_secret("CLIENT_REPORT_SUBJECT", ""))
    check_now = st.button("Buscar ahora", use_container_width=True)

uploaded = st.file_uploader("Sube Daily Report", type=["xlsx", "xls", "csv", "txt", "pdf"])

if "email_report" not in st.session_state:
    st.session_state.email_report = None
if "email_report_status" not in st.session_state:
    st.session_state.email_report_status = ""

if monitor_enabled:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=int(poll_seconds) * 1000, key="email_report_autorefresh")
    except Exception:
        st.caption("Tip: instala streamlit-autorefresh para que el monitoreo se ejecute automáticamente sin recargar manualmente.")

if monitor_enabled or check_now:
    try:
        found = latest_report_from_inbox(inbox_sender_filter, inbox_subject_filter)
        if found:
            st.session_state.email_report = found
            st.session_state.email_report_status = f"Reporte recibido por correo: {found.subject or found.filename}"
        elif check_now:
            st.session_state.email_report_status = "No encontré correos nuevos no leídos con esos filtros."
    except Exception as exc:
        st.session_state.email_report_status = f"Error leyendo correo: {exc}"

if st.session_state.email_report_status:
    if st.session_state.email_report and st.session_state.email_report_status.startswith("Reporte recibido"):
        st.success(st.session_state.email_report_status)
    else:
        st.info(st.session_state.email_report_status)

if "manual_report_open" not in st.session_state:
    st.session_state.manual_report_open = False

if uploaded:
    st.session_state.manual_report_open = False
else:
    manual_label = "Ocultar captura manual" if st.session_state.manual_report_open else "Capturar reporte sin adjuntar archivo"
    if st.button(manual_label, type="secondary", use_container_width=True):
        st.session_state.manual_report_open = not st.session_state.manual_report_open

show_editor = uploaded is not None or st.session_state.manual_report_open or st.session_state.email_report is not None

if show_editor:
    try:
        if uploaded:
            sheets, file_type, raw_text = read_any_file(uploaded)
            report = force_24h_operation_if_summary_only(extract_report(raw_text, uploaded.name), raw_text)
            st.success(f"Archivo leído como {file_type.upper()}")
        elif st.session_state.email_report is not None:
            email_report = st.session_state.email_report
            raw_text = email_report.raw_text
            report = force_24h_operation_if_summary_only(
                extract_report(
                    raw_text,
                    email_report.filename,
                    email_body=getattr(email_report, "body_text", "") or "",
                ),
                raw_text,
            )
            st.success(f"Correo leído: {email_report.subject or email_report.filename}")
            st.caption(f"From: {email_report.sender} | Fecha: {email_report.date} | Fuente: {email_report.source_type}")
        else:
            raw_text = ""
            report = extract_report(raw_text)
            st.info("Captura manual activa. Completa los campos y escribe las operaciones sin adjuntar un reporte.")

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
            if raw_text:
                if report.get("_operacion_cuerpo_correo"):
                    st.info(
                        "Texto de operaciones tomado del cuerpo del correo: bloque «Últimas 24 hrs» "
                        "(copiado tal cual en el correo, sin usar el adjunto)."
                    )
                else:
                    st.info(
                        "Se extrae la tabla Daily Activity Summary cuando existe. "
                        "Se eliminan secciones no necesarias como BHA, bit, personal, costos, parámetros, lodo e hidráulica."
                    )

                activities = report.get("_activities") if report.get("_activities") is not None else split_activity_text(raw_text)
                warnings = validate_hour_sequence(activities)
                if warnings:
                    st.warning("\n".join(warnings))
            else:
                st.info("Escribe o pega aquí el texto de operaciones que llevará el PDF.")

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

        if raw_text:
            with st.expander("Vista previa del texto fuente detectado"):
                st.text(clean_multiline_text(raw_text)[:20000])

    except Exception as exc:
        st.exception(exc)

else:
    st.info("Esperando archivo. También puedes usar el botón para capturar un reporte manualmente.")
