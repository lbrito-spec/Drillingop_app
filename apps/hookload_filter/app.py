import io
import smtplib
from email.message import EmailMessage

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Filtrado de Hookload máximo por Bit depth",
    page_icon="📈",
    layout="wide",
)

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }
    div.stButton > button[kind="primary"] {
        width: 100%;
        border-radius: 14px;
        padding: 0.9rem 1.1rem;
        font-weight: 700;
        font-size: 1rem;
        border: 0;
        box-shadow: 0 10px 24px rgba(0,0,0,0.18);
    }
    div.stDownloadButton > button {
        width: 100%;
        border-radius: 14px;
        padding: 0.9rem 1.1rem;
        font-weight: 700;
        font-size: 1rem;
    }
    .small-note {
        opacity: 0.85;
        margin-top: -0.3rem;
        margin-bottom: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================
# Helpers
# =========================
def detect_timestamp_column(columns):
    preferred = ["Timestamp", "timestamp", "YYYY-MM-DDTHH:MM:SS"]
    for col in preferred:
        if col in columns:
            return col
    return None


def preserve_units_row(original_df: pd.DataFrame, filtered_df: pd.DataFrame) -> pd.DataFrame:
    """
    Conserva la primera fila del archivo original como fila de unidades,
    para que quede justo debajo del encabezado en el CSV final.
    """
    if original_df.empty:
        return filtered_df.copy()

    units_row = original_df.iloc[[0]].copy()
    units_row.columns = [c.strip() for c in units_row.columns]

    for col in filtered_df.columns:
        if col not in units_row.columns:
            units_row[col] = ""

    units_row = units_row[filtered_df.columns]
    return pd.concat([units_row, filtered_df], ignore_index=True)


def send_email_with_attachment(
    smtp_server: str,
    smtp_port: int,
    smtp_user: str,
    smtp_pass: str,
    from_email: str,
    to_email: str,
    attachment_bytes: bytes,
    attachment_name: str,
) -> None:
    msg = EmailMessage()
    msg["Subject"] = f"CSV filtrado - {attachment_name}"
    msg["From"] = from_email
    msg["To"] = to_email
    msg.set_content(
        "Hola,\n\n"
        "Adjunto el archivo CSV filtrado generado desde la app de Streamlit.\n\n"
        "Saludos."
    )

    msg.add_attachment(
        attachment_bytes,
        maintype="text",
        subtype="csv",
        filename=attachment_name,
    )

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


# =========================
# Sidebar
# =========================
with st.sidebar:
    st.header("Configuración de limpieza")
    timestamp_col_enabled = st.checkbox(
        "Procesar columna Timestamp si existe",
        value=True,
    )
    drop_nan_timestamp = st.checkbox(
        "Eliminar filas con Timestamp inválido",
        value=True,
    )

    st.divider()

    st.header("Configuración de email")
    smtp_server = st.text_input("SMTP server", value="smtp.gmail.com")
    smtp_port = st.number_input("SMTP port", min_value=1, max_value=65535, value=587, step=1)
    smtp_user = st.text_input("SMTP user", value="lenin.rogii@gmail.com")
    smtp_pass = st.text_input(
        "SMTP password / App password",
        value="rzqc ojjv osrq pexw",
        type="password",
    )
    from_email = st.text_input("From email", value=smtp_user)
    to_email = st.text_input("To email", value="solobox+pemex@rogii.com")


# =========================
# Header
# =========================
col_logo, col_title = st.columns([0.6, 8])

with col_logo:
    logo_loaded = False

    try:
        st.image("LogoDS.png", width=56)
        logo_loaded = True
    except Exception:
        pass

    if not logo_loaded:
        try:
            st.image(r"C:\Users\l.brito_rogii\Downloads\Tripping App\LogoDS.png", width=56)
        except Exception:
            pass

with col_title:
    st.title("Filtrado de Hookload máximo por Bit depth")

st.write(
    "Carga un archivo CSV, conserva la fila de unidades si existe, "
    "filtra el mayor Hookload por cada Bit depth y luego permite descargar "
    "o enviar el resultado por correo."
)

uploaded_file = st.file_uploader("Sube tu archivo CSV", type=["csv"])


# =========================
# Main flow
# =========================
if uploaded_file is not None:
    try:
        raw_bytes = uploaded_file.getvalue()
        original_df = pd.read_csv(io.BytesIO(raw_bytes))
        original_df.columns = [c.strip() for c in original_df.columns]

        st.subheader("Vista previa original")
        st.dataframe(original_df.head(), use_container_width=True)

        st.info(f"Columnas detectadas: {', '.join(original_df.columns.tolist())}")

        required_columns = ["Bit depth", "Hookload"]
        missing = [c for c in required_columns if c not in original_df.columns]
        if missing:
            st.error("Faltan columnas obligatorias: " + ", ".join(missing))
            st.stop()

        df = original_df.copy()

        # Limpieza numérica
        df["Bit depth"] = pd.to_numeric(df["Bit depth"], errors="coerce")
        df["Hookload"] = pd.to_numeric(df["Hookload"], errors="coerce")

        original_rows = len(df)
        df = df.dropna(subset=["Bit depth", "Hookload"]).copy()
        cleaned_rows = len(df)

        # Procesamiento de timestamp
        ts_col = detect_timestamp_column(df.columns)
        if timestamp_col_enabled and ts_col is not None:
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
            if drop_nan_timestamp:
                df = df.dropna(subset=[ts_col]).copy()

        st.subheader("Datos después de limpiar NaNs")
        st.dataframe(df.head(), use_container_width=True)

        if df.empty:
            st.warning("No quedaron filas válidas después de la limpieza.")
            st.stop()

        # Filtrado por máximo Hookload para cada Bit depth
        filtered_df = (
            df.loc[df.groupby("Bit depth")["Hookload"].idxmax()]
            .sort_values("Bit depth")
            .reset_index(drop=True)
        )

        final_df = preserve_units_row(original_df=original_df, filtered_df=filtered_df)

        st.subheader("Datos filtrados")
        st.dataframe(final_df.head(100), use_container_width=True)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Filas originales", original_rows)
        m2.metric("Filas válidas", cleaned_rows)
        m3.metric("Filas filtradas", len(filtered_df))
        m4.metric("Fila de unidades", "Sí" if len(original_df) >= 1 else "No")

        # =========================
        # Gráfico Hookload vs Depth
        # =========================
        st.subheader("Gráfico Hookload vs Depth en vivo")

        chart_df = filtered_df.copy()

        if not chart_df.empty:
            chart = (
                alt.Chart(chart_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("Bit depth:Q", title="Bit depth"),
                    y=alt.Y("Hookload:Q", title="Hookload"),
                    tooltip=["Bit depth", "Hookload"],
                )
                .properties(height=420)
                .interactive()
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No hay datos suficientes para graficar.")

        csv_bytes = final_df.to_csv(index=False).encode("utf-8")
        output_name = uploaded_file.name.replace(".csv", "_filtrado.csv")

        c1, c2 = st.columns(2)

        with c1:
            st.download_button(
                label="⬇️ Descargar CSV filtrado",
                data=csv_bytes,
                file_name=output_name,
                mime="text/csv",
                use_container_width=True,
            )

        with c2:
            if st.button("🚀 Enviar a Parsing Email", type="primary", use_container_width=True):
                try:
                    send_email_with_attachment(
                        smtp_server=smtp_server,
                        smtp_port=int(smtp_port),
                        smtp_user=smtp_user,
                        smtp_pass=smtp_pass,
                        from_email=from_email,
                        to_email=to_email,
                        attachment_bytes=csv_bytes,
                        attachment_name=output_name,
                    )
                    st.success(f"Archivo enviado correctamente a {to_email}")
                except Exception as email_error:
                    st.error(f"No se pudo enviar el correo: {email_error}")

    except Exception as e:
        st.exception(e)
else:
    st.caption("Esperando que subas un archivo CSV.")
