
import html
import textwrap
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


st.set_page_config(
    page_title="Vibe Coding Pro · Apps Técnicas para Rogii",
    layout="wide",
    initial_sidebar_state="expanded",
)

PRIMARY = "#2563eb"
SECONDARY = "#7c3aed"
SUCCESS = "#16a34a"
WARNING = "#d97706"
DANGER = "#db2777"
DARK_BG = "#0e1117"
PANEL_BG = "rgba(15,23,42,0.72)"
GRID = "rgba(148,163,184,0.14)"
AXIS = "rgba(148,163,184,0.40)"
TEXT = "#e2e8f0"

PYTHON_DL = "https://www.python.org/downloads/"
PIP_GUIDE = "https://packaging.python.org/en/latest/tutorials/installing-packages/"
PIP_VENV = "https://packaging.python.org/guides/installing-using-pip-and-virtualenv/"
STREAMLIT_DEPLOY = "https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/deploy"
STREAMLIT_OVERVIEW = "https://docs.streamlit.io/deploy/streamlit-community-cloud"
STREAMLIT_FILEORG = "https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/file-organization"
CURSOR_GETTING_STARTED = "https://docs.cursor.com/getting-started"
CURSOR_INSTALL = "https://docs.cursor.com/get-started/installation"
CURSOR_INTRODUCTION = "https://docs.cursor.com/get-started/introduction"
CURSOR_MODELS = "https://docs.cursor.com/models"
CURSOR_ACCOUNT_PRICING = "https://docs.cursor.com/account/pricing"
CURSOR_CHANGELOG = "https://cursor.com/changelog"
CLAUDE_CODE_OVERVIEW = "https://docs.anthropic.com/en/docs/claude-code/overview"
GITHUB_COPILOT_GETTING_STARTED = "https://docs.github.com/copilot/get-started"

st.markdown(
    f"""
    <style>
    .main-title {{
        font-size: 3.35rem;
        font-weight: 900;
        text-align: center;
        margin-top: 0.25rem;
        margin-bottom: 0.3rem;
        letter-spacing: -0.03em;
    }}
    .subtitle {{
        text-align: center;
        color: #94a3b8;
        font-size: 1.12rem;
        margin-bottom: 1.5rem;
    }}
    .section-title {{
        font-size: 1.72rem;
        font-weight: 800;
        margin-top: 1.05rem;
        margin-bottom: 0.6rem;
        letter-spacing: -0.02em;
    }}
    .breadcrumb {{
        font-size: 0.88rem;
        color: #94a3b8;
        margin-bottom: 0;
    }}
    .time-badge {{
        display: inline-block;
        font-size: 0.72rem;
        font-weight: 800;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: #93c5fd;
        border: 1px solid rgba(147, 197, 253, 0.45);
        padding: 0.28rem 0.65rem;
        border-radius: 999px;
        background: rgba(15, 23, 42, 0.55);
    }}
    .head-row {{
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 0.45rem 0.8rem;
        margin: 0.1rem 0 0.9rem 0;
    }}
    .topic-card {{
        background: linear-gradient(145deg, rgba(15,23,42,0.98) 0%, rgba(30,41,59,0.94) 100%);
        border: 1px solid rgba(148,163,184,0.22);
        border-radius: 18px;
        padding: 1rem 1rem 0.95rem 1rem;
        min-height: 150px;
        color: #e2e8f0;
        box-shadow: 0 10px 26px rgba(2, 6, 23, 0.22);
    }}
    .topic-title {{
        font-size: 1.08rem;
        font-weight: 800;
        color: #f8fafc;
        margin-bottom: 0.3rem;
    }}
    .metric-card {{
        background: linear-gradient(145deg, rgba(15,23,42,0.95) 0%, rgba(30,41,59,0.90) 100%);
        border: 1px solid rgba(148,163,184,0.20);
        border-radius: 16px;
        padding: 0.85rem 0.95rem;
        color: #e2e8f0;
    }}
    .metric-title {{
        font-size: 0.78rem;
        color: #94a3b8;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 0.15rem;
    }}
    .metric-value {{
        font-size: 1.3rem;
        font-weight: 900;
        color: #f8fafc;
    }}
    .info-box {{
        background: #eff6ff;
        border-left: 5px solid {PRIMARY};
        padding: 1rem;
        border-radius: 12px;
        margin: 0.8rem 0;
        color: #0f172a;
    }}
    .ok-box {{
        background: #f0fdf4;
        border-left: 5px solid {SUCCESS};
        padding: 1rem;
        border-radius: 12px;
        margin: 0.8rem 0;
        color: #052e16;
    }}
    .warn-box {{
        background: #fff7ed;
        border-left: 5px solid {WARNING};
        padding: 1rem;
        border-radius: 12px;
        margin: 0.8rem 0;
        color: #431407;
    }}
    .exercise-box {{
        background: #fefce8;
        border-left: 5px solid {WARNING};
        padding: 1rem;
        border-radius: 12px;
        margin: 0.8rem 0;
        color: #422006;
    }}
    .objective-box {{
        background: linear-gradient(145deg, rgba(6,78,59,0.52) 0%, rgba(15,23,42,0.95) 100%);
        border: 1px solid rgba(34,197,94,0.3);
        border-left: 5px solid #22c55e;
        padding: 1rem 1.25rem;
        border-radius: 14px;
        margin: 0.8rem 0 1rem 0;
        color: #ecfdf5;
    }}
    .prereq-box {{
        background: linear-gradient(145deg, rgba(120,53,15,0.42) 0%, rgba(15,23,42,0.95) 100%);
        border: 1px solid rgba(251,146,60,0.28);
        border-left: 5px solid #fb923c;
        padding: 1rem 1.25rem;
        border-radius: 14px;
        margin: 0.8rem 0 1rem 0;
        color: #ffedd5;
    }}
    .flow-wrap {{
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 0.4rem 0.55rem;
        margin: 0.55rem 0 1rem 0;
        padding: 1rem 1.1rem;
        background: rgba(15, 23, 42, 0.55);
        border: 1px solid rgba(148, 163, 184, 0.22);
        border-radius: 12px;
    }}
    .flow-step {{
        background: linear-gradient(145deg, rgba(37,99,235,0.38) 0%, rgba(67,56,202,0.48) 100%);
        color: #f1f5f9;
        padding: 0.5rem 0.85rem;
        border-radius: 8px;
        font-size: 0.82rem;
        font-weight: 700;
        line-height: 1.35;
        border: 1px solid rgba(129, 140, 248, 0.55);
    }}
    .flow-arrow {{
        color: #94a3b8;
        font-size: 1.2rem;
        font-weight: 900;
        padding: 0 0.1rem;
        user-select: none;
    }}
    .chip-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.45rem;
        margin: 0.3rem 0 0.8rem 0;
    }}
    .chip {{
        display: inline-block;
        font-size: 0.72rem;
        font-weight: 800;
        letter-spacing: 0.07em;
        text-transform: uppercase;
        padding: 0.38rem 0.92rem;
        border-radius: 999px;
        border: 1px solid rgba(147,197,253,0.55);
        color: #93c5fd;
    }}
    .pro-link-box {{
        background: linear-gradient(145deg, rgba(15,23,42,0.96) 0%, rgba(30,41,59,0.92) 100%);
        border: 1px solid rgba(148,163,184,0.22);
        border-radius: 16px;
        padding: 1rem;
        margin: 0.5rem 0 0.85rem 0;
    }}
    .code-caption {{
        color: #94a3b8;
        font-size: 0.92rem;
        margin-top: -0.25rem;
        margin-bottom: 0.6rem;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

LESSONS = [
    "1. Bienvenida y mapa pro",
    "2. Teoría de programación",
    "3. Pensamiento computacional",
    "4. Python desde cero",
    "5. Frameworks, librerías y stack",
    "6. Arquitectura de software para apps Rogii",
    "7. Datos, unidades y validación",
    "8. Cursor: teoría de uso",
    "9. Prompt engineering para coding",
    "10. Entorno local: instalar Python y pip",
    "11. Librerías principales con pip",
    "12. Caso Rogii: Roadmap",
    "13. Caso Rogii: Torque & Drag",
    "14. Caso Rogii: BHA parser",
    "15. Enfoque para geólogos: star steering",
    "16. Ejercicios para geólogos con vibe coding",
    "17. Tops y markers para geólogos",
    "18. Correlación simple entre pozos",
    "19. Alertas above / below target",
    "20. Steering training simulator",
    "21. AI vs Machine Learning",
    "22. Cursor, Claude Code y otros asistentes",
    "23. Cómo usar Cursor y Claude Code",
    "24. Laboratorio de Python interactivo",
    "25. Live coding studio pro",
    "26. Caso Jusset Peña · Darcy + Python + prompts",
    "27. P10, P90, box plots y estadística (vibe coding)",
    "28. Refactorización guiada",
    "29. Deploy de la app paso a paso",
    "30. Proyecto final y checklist",
    "31. Ejercicio para Angela · Gatito Galileo (vibe coding)",
]


def section_title(text: str):
    st.markdown(f'<div class="section-title">{text}</div>', unsafe_allow_html=True)


def lesson_header(breadcrumb: str, minutes: int | None = None):
    inner = f'<div class="breadcrumb">{html.escape(breadcrumb)}</div>'
    if minutes is not None:
        inner += f'<span class="time-badge">~{minutes} min</span>'
    st.markdown(f'<div class="head-row">{inner}</div>', unsafe_allow_html=True)


def box(text: str, kind: str = "info"):
    cls = {"info": "info-box", "ok": "ok-box", "warn": "warn-box", "exercise": "exercise-box"}.get(kind, "info-box")
    st.markdown(f'<div class="{cls}">{text}</div>', unsafe_allow_html=True)


def objective_box(title: str, items: list[str], kind: str = "objective"):
    lis = "".join(f"<li>{html.escape(i)}</li>" for i in items)
    cls = "objective-box" if kind == "objective" else "prereq-box"
    st.markdown(f'<div class="{cls}"><b>{html.escape(title)}</b><ul>{lis}</ul></div>', unsafe_allow_html=True)


def chips(*labels: str):
    markup = "".join(f'<span class="chip">{html.escape(x)}</span>' for x in labels)
    st.markdown(f'<div class="chip-row">{markup}</div>', unsafe_allow_html=True)


def flow(*steps: str):
    parts = []
    for i, s in enumerate(steps):
        parts.append(f'<span class="flow-step">{html.escape(s)}</span>')
        if i < len(steps) - 1:
            parts.append('<span class="flow-arrow">→</span>')
    st.markdown(f'<div class="flow-wrap">{"".join(parts)}</div>', unsafe_allow_html=True)


def dark_layout(fig):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=DARK_BG,
        plot_bgcolor=DARK_BG,
        font=dict(color=TEXT),
    )
    fig.update_xaxes(gridcolor=GRID, linecolor=AXIS, showline=True)
    fig.update_yaxes(gridcolor=GRID, linecolor=AXIS, showline=True)


def metric_card(title: str, value: str):
    st.markdown(
        f'<div class="metric-card"><div class="metric-title">{html.escape(title)}</div><div class="metric-value">{html.escape(value)}</div></div>',
        unsafe_allow_html=True,
    )


def link_box(title: str, links: list[tuple[str, str]]):
    inner = "".join(f"<li><a href='{url}' target='_blank'>{html.escape(label)}</a></li>" for label, url in links)
    st.markdown(f"<div class='pro-link-box'><b>{html.escape(title)}</b><ul>{inner}</ul></div>", unsafe_allow_html=True)


def generate_roadmap_demo(offset_n=2):
    depth = np.linspace(9500, 11000, 140)
    rng = np.random.default_rng(7)
    df = pd.DataFrame({"Depth": depth})
    for name, base in [("Roadmap", 55), ("Active Well", 48)] + [(f"Offset {i+1}", 46 - i * 2) for i in range(offset_n)]:
        df[f"{name}_ROP"] = base + 6 * np.sin(depth / 220) + rng.normal(0, 1.2, len(depth))
        df[f"{name}_WOB"] = 18 + 2 * np.cos(depth / 260 + len(name)) + rng.normal(0, 0.35, len(depth))
        df[f"{name}_Surface RPM"] = 120 + 8 * np.sin(depth / 280 + len(name)) + rng.normal(0, 0.9, len(depth))
    return df


def intro_page():
    st.markdown('<div class="main-title">Vibe Coding Pro · Apps Técnicas para Rogii</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="subtitle">Versión completa y profesional del curso: teoría de programación, Python, arquitectura, Cursor, librerías, deploy, prompts en vivo y laboratorios interactivos aplicados a casos Rogii.</div>',
        unsafe_allow_html=True,
    )
    lesson_header("Inicio › Bienvenida › Mapa pro", 12)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown('<div class="topic-card"><div class="topic-title">Base conceptual</div>Qué es programar, cómo pensar problemas, variables, flujo lógico, funciones y errores.</div>', unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="topic-card"><div class="topic-title">Stack técnico</div>Python, pip, Pandas, NumPy, Plotly, Streamlit, validación y arquitectura limpia.</div>', unsafe_allow_html=True)
    with c3:
        st.markdown('<div class="topic-card"><div class="topic-title">Cursor y prompts</div>Cómo usar Cursor con intención, restricciones, criterio de éxito y control técnico.</div>', unsafe_allow_html=True)
    with c4:
        st.markdown('<div class="topic-card"><div class="topic-title">Deploy y producto</div>Cómo pasar de app local a app compartible con repositorio, requirements y deploy.</div>', unsafe_allow_html=True)

    flow("Problema", "Datos + unidades", "Lógica + arquitectura", "UI + visualización", "Validación", "Deploy")
    chips("Python", "Streamlit", "Cursor", "Deploy", "Casos Rogii", "Live coding")

    roadmap = pd.DataFrame(
        {
            "Etapa": ["Programación", "Python", "Frameworks", "Arquitectura", "Cursor", "Casos Rogii", "Live coding", "Deploy"],
            "Peso": [8, 10, 8, 10, 10, 10, 9, 8],
        }
    )
    # Barras horizontales desde 0: el funnel de Plotly centra cada tramo y parece un "rombo";
    # además, los números son PESO (1–10), no el número de lección del menú lateral.
    fig = px.bar(
        roadmap,
        x="Peso",
        y="Etapa",
        color="Etapa",
        orientation="h",
        height=540,
        title="Peso relativo por tema (escala 1–10; no es el nº de lección del curso)",
        text="Peso",
    )
    fig.update_traces(textposition="inside", cliponaxis=False)
    fig.update_layout(
        showlegend=False,
        xaxis=dict(title="Peso relativo", range=[0, 11], dtick=1),
        yaxis=dict(categoryorder="total ascending"),
    )
    dark_layout(fig)
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Los valores 8, 9 o 10 miden énfasis relativo en el mapa (tiempo/complejidad), "
        "no confundir con las lecciones «8. Cursor…» o «9. Prompt…» en la barra lateral."
    )

    box(
        "<b>Meta del curso:</b> que puedas describir, construir, depurar, refactorizar y desplegar una app técnica útil, sin perder control sobre datos, unidades, arquitectura y criterio de negocio.",
        "info",
    )


def programming_theory_page():
    section_title("Teoría de programación")
    lesson_header("Inicio › Fundamentos › Teoría de programación", 22)
    objective_box(
        "Objetivos",
        [
            "Entender un programa como transformación precisa de entradas a salidas.",
            "Reconocer reglas, estado, flujo, validación y modularidad.",
            "Relacionar teoría general con apps técnicas de drilling y operaciones.",
        ],
    )
    objective_box(
        "Requisitos previos",
        [
            "Lógica básica.",
            "No se necesita experiencia previa programando.",
        ],
        kind="prereq",
    )
    chips("Entradas", "Procesos", "Salidas", "Reglas", "Estado", "Trazabilidad")

    st.markdown(
        """
        Programar es diseñar una secuencia precisa de acciones que una computadora pueda ejecutar sin ambigüedad.
        La computadora no intuye el contexto; tú debes declarar qué entra, qué se valida, qué se calcula y qué sale.
        En una app técnica, el valor no está solo en “hacer cuentas”, sino en encapsular reglas de negocio, convertir unidades,
        protegerse ante errores y mostrar resultados interpretables.
        """
    )

    theory = pd.DataFrame(
        {
            "Concepto": ["Entrada", "Proceso", "Salida", "Estado", "Validación", "Módulo"],
            "Qué significa": [
                "Dato inicial que recibe el programa",
                "Transformación lógica o cálculo",
                "Resultado visible o reusable",
                "Información que se conserva entre pasos",
                "Chequeo para evitar basura o errores",
                "Bloque de código con responsabilidad clara",
            ],
            "Ejemplo Rogii": [
                "CSV, Excel, parámetros de pozo",
                "Normalizar torque, calcular delta, interpolar FF",
                "Dashboard, tabla, alerta, resumen BHA",
                "Archivo cargado, filtros activos, pozo seleccionado",
                "Falta de columnas, unidades inconsistentes",
                "roadmap.py, units.py, validators.py",
            ],
        }
    )
    st.dataframe(theory, use_container_width=True)

    code = """
    depth_ft = 10350
    rop_ft_hr = 47.2

    if rop_ft_hr < 30:
        status = "revisar desempeño"
    else:
        status = "dentro del rango"

    print(status)
    """
    st.code(textwrap.dedent(code), language="python")
    rop = st.slider("Simula la variable ROP", 5.0, 80.0, 47.2, 0.5, key="prog_theory_rop")
    st.success("La regla devuelve: dentro del rango" if rop >= 30 else "La regla devuelve: revisar desempeño")

    box(
        "<b>Idea clave:</b> una app profesional no es un bloque enorme de código. Es un sistema donde cada parte tiene una función clara y verificable.",
        "ok",
    )


def computational_thinking_page():
    section_title("Pensamiento computacional")
    lesson_header("Inicio › Fundamentos › Pensamiento computacional", 16)
    chips("Descomposición", "Patrones", "Abstracción", "Algoritmo")
    st.markdown("Antes de escribir código o pedirle algo a Cursor, conviene estructurar el problema con cuatro lentes mentales.")
    grid = pd.DataFrame(
        {
            "Lente": ["Descomposición", "Patrones", "Abstracción", "Algoritmo"],
            "Pregunta guía": [
                "¿Qué piezas pequeñas tiene el problema?",
                "¿Qué se repite de un caso a otro?",
                "¿Qué puedo encapsular y ocultar por ahora?",
                "¿Cuál es el orden reproducible de pasos?",
            ],
            "Ejemplo Rogii": [
                "Separar carga, normalización, comparación y visualización.",
                "Cada export necesita validación de columnas y unidades.",
                "Pensar 'cargar roadmap' como una función completa.",
                "Leer → limpiar → convertir → calcular → graficar.",
            ],
        }
    )
    st.dataframe(grid, use_container_width=True)

    selected = st.selectbox("Aplica estos lentes a un caso", ["Roadmap comparativo", "Torque & Drag", "BHA parser"])
    if selected == "Roadmap comparativo":
        flow("Upload export", "Parse doble header", "Detectar well activo", "Elegir offsets", "Graficar panel", "Calcular delta %")
    elif selected == "Torque & Drag":
        flow("Cargar modelo", "Seleccionar FF", "Interpolar curvas", "Construir corredor", "Mostrar incertidumbre", "Validar lectura")
    else:
        flow("Subir BHA", "Parsear tabla", "Normalizar campos", "Resumir", "Persistir", "Reusar en modelo")

    box("<b>Consejo:</b> si tu prompt no expresa bien piezas, patrones, abstracciones y orden, la IA tenderá a generar código más frágil.", "warn")


def python_basics_page():
    section_title("Python desde cero")
    lesson_header("Inicio › Python › Sintaxis, datos y funciones", 28)
    tab1, tab2, tab3 = st.tabs(["Variables y tipos", "Flujo lógico", "Funciones y errores"])

    with tab1:
        code = """
        pozo = "GeoPark-LJE-1030"
        depth_ft = 10350
        rop_ft_hr = 47.2
        run_ids = [1, 2, 3]
        well_cfg = {"unit_system": "field", "client": "GeoPark"}
        """
        st.code(textwrap.dedent(code), language="python")
        df = pd.DataFrame(
            {
                "Nombre": ["pozo", "depth_ft", "rop_ft_hr", "run_ids", "well_cfg"],
                "Tipo": ["str", "int", "float", "list", "dict"],
                "Uso": ["Identificador", "Profundidad", "Velocidad", "Secuencia", "Configuración"],
            }
        )
        st.dataframe(df, use_container_width=True)

    with tab2:
        wob = st.slider("WOB", 5.0, 45.0, 24.0, 0.5, key="py_wob")
        vibration = st.slider("Vibración", 0.1, 3.0, 1.1, 0.05, key="py_vib")
        code = f"""
        wob = {wob}
        vibration = {vibration}

        if vibration > 1.5 and wob > 30:
            action = "bajar agresividad"
        else:
            action = "seguir monitoreando"
        """
        st.code(textwrap.dedent(code), language="python")
        if vibration > 1.5 and wob > 30:
            st.error("La regla dispara: bajar agresividad")
        else:
            st.success("La regla indica: seguir monitoreando")

    with tab3:
        code = """
        import pandas as pd

        def convert_torque_from_surface(series, unit_mode):
            s = pd.to_numeric(series, errors="coerce")
            s_lbf_ft = s * 1000.0
            if unit_mode == "metric":
                return s_lbf_ft * 1.3558179483314
            return s_lbf_ft
        """
        st.code(textwrap.dedent(code), language="python")
        val = st.number_input("Torque en klbf·ft", 0.0, 100.0, 12.0, 0.5)
        unit = st.radio("Unidad objetivo", ["field", "metric"], horizontal=True, key="torque_unit")
        result = val * 1000.0 if unit == "field" else val * 1000.0 * 1.3558179483314
        st.metric("Resultado", f"{result:,.2f} {'lbf·ft' if unit == 'field' else 'N·m'}")

    box("<b>Buenas prácticas:</b> nombres claros, funciones pequeñas, validación temprana y manejo de errores con mensajes útiles.", "info")


def frameworks_page():
    section_title("Frameworks, librerías y stack")
    lesson_header("Inicio › Stack › Frameworks y librerías", 22)
    st.markdown(
        """
        **Si no has programado antes:** programar es escribir **instrucciones claras** para que la computadora las ejecute paso a paso.
        En este curso no necesitas saberlo todo de memoria; sí conviene entender **qué es cada tipo de pieza** para no mezclar conceptos.
        """
    )

    with st.expander("¿Qué es un **lenguaje** de programación (p. ej. Python)?", expanded=True):
        st.markdown(
            """
            Es el **idioma** en el que escribes el programa: reglas de sintaxis, palabras reservadas y forma de expresar lógica (condiciones, bucles, funciones).
            **Analogía:** como el idioma humano con el que redactas un procedimiento; la máquina solo entiende lo que está escrito en ese lenguaje.
            """
        )
    with st.expander("¿Qué es una **librería**?"):
        st.markdown(
            """
            Es **código ya hecho** (por la comunidad o por terceros) que **importas** en tu proyecto para reutilizar funciones: tablas, gráficos, matemática, etc.
            Tú **decides cuándo** llamarla; no impone toda la forma de tu aplicación.
            **Analogía:** una caja de herramientas que sacas cuando la necesitas; no es la casa entera.
            """
        )
    with st.expander("¿Qué es un **framework**?"):
        st.markdown(
            """
            Es un **marco** que define **cómo debe organizarse** parte de tu aplicación: qué archivo arranca, cómo se construyen pantallas, ciclo de ejecución, etc.
            Te ahorra decidir todo desde cero; a cambio **sigues convenciones** del framework.
            **Analogía:** el esqueleto de un stand o un manual de montaje: encaja piezas en un orden concreto.
            """
        )
    with st.expander("¿Qué es el **stack**?"):
        st.markdown(
            """
            Es el **conjunto de tecnologías** que usas juntas en un proyecto: lenguaje + framework + librerías.
            No es una sola cosa nueva; es **la receta** de lo que convive en tu app (por ejemplo Python + Streamlit + Pandas + Plotly).
            """
        )

    st.markdown("### Piezas que usamos en el curso (resumen)")
    table = pd.DataFrame(
        {
            "Pieza": ["Python", "Pandas", "NumPy", "Plotly", "Streamlit", "SciPy", "Pydantic"],
            "Tipo": ["Lenguaje", "Librería", "Librería", "Librería", "Framework", "Librería", "Librería"],
            "Si vienes de cero": [
                "Donde escribes toda la lógica del programa.",
                "Tablas y datos como en Excel, pero en código.",
                "Números y vectores; operaciones matemáticas rápidas.",
                "Gráficos interactivos en la pantalla del navegador.",
                "La app web: botones, páginas y despliegue sin montar un servidor a mano.",
                "Herramientas numéricas extra (p. ej. interpolar curvas).",
                "Comprobar que columnas y tipos de datos son los esperados.",
            ],
            "Qué resuelve (técnico)": [
                "Base sintáctica y lógica",
                "Tablas, limpieza y joins",
                "Cálculo vectorizado",
                "Visualización interactiva",
                "Interfaz, estado y despliegue rápido",
                "Interpolación y herramientas numéricas",
                "Validación de esquemas y datos",
            ],
        }
    )
    st.dataframe(table, use_container_width=True)

    fig = px.sunburst(
        names=["App técnica Rogii", "Lenguaje", "Framework", "Data", "Visualización", "Validación", "Python", "Streamlit", "Pandas", "NumPy", "Plotly", "SciPy", "Pydantic"],
        parents=["", "App técnica Rogii", "App técnica Rogii", "App técnica Rogii", "App técnica Rogii", "App técnica Rogii", "Lenguaje", "Framework", "Data", "Data", "Visualización", "Data", "Validación"],
        values=[24, 5, 5, 6, 4, 4, 5, 5, 3, 3, 4, 2, 4],
    )
    fig.update_layout(height=680, title="Cómo se apilan lenguaje, framework y librerías")
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Diagrama de conjunto: en el centro está tu app; alrededor, el tipo de rol (lenguaje, framework, datos, etc.) "
        "y en el anillo exterior las herramientas concretas. Los tamaños son ilustrativos, no unidades reales."
    )

    box(
        "<b>Idea práctica:</b> Python es el idioma; Streamlit da la estructura de la app web; Pandas y Plotly son librerías que "
        "tú llamas desde Python. No compiten entre sí: se combinan.",
        "ok",
    )


def architecture_page():
    section_title("Arquitectura de software para apps Rogii")
    lesson_header("Inicio › Arquitectura › Diseño mantenible", 24)
    objective_box(
        "Objetivos",
        [
            "Separar ingestión, validación, lógica, visualización y estado.",
            "Evitar archivos monolíticos y duplicación.",
            "Preparar la app para crecer sin romperse.",
        ],
    )
    flow("Cargar", "Validar", "Normalizar", "Calcular", "Construir figura", "Renderizar UI")

    code = """
    app.py
    modules/
        roadmap.py
        torque_drag.py
        bha.py
    utils/
        loaders.py
        validators.py
        units.py
        plots.py
        prompts.py
    assets/
        logo.png
    requirements.txt
    """
    st.code(textwrap.dedent(code), language="bash")

    principles = pd.DataFrame(
        {
            "Principio": [
                "Separación de responsabilidades",
                "Funciones puras cuando sea posible",
                "Session state solo para experiencia de usuario",
                "Validación cerca de la ingestión",
                "Visualización separada del cálculo",
                "Prompts guardados como activos del proyecto",
            ],
            "Por qué importa": [
                "Reduce caos y acoplamiento",
                "Facilita pruebas y debugging",
                "Evita estado global confuso",
                "Captura problemas temprano",
                "Permite reusar lógica",
                "Hace reproducible el trabajo con IA",
            ],
        }
    )
    st.dataframe(principles, use_container_width=True)

    tab1, tab2 = st.tabs(["Patrón recomendado", "Antipatrón"])
    with tab1:
        st.code(
            textwrap.dedent(
                """
                def load_and_normalize(path):
                    raw = pd.read_excel(path)
                    clean = normalize_units(raw)
                    validate_schema(clean)
                    return clean

                def build_visuals(df):
                    fig = build_plot(df)
                    return fig

                def render_ui(fig):
                    st.plotly_chart(fig, use_container_width=True)
                """
            ),
            language="python",
        )
    with tab2:
        st.code(
            textwrap.dedent(
                """
                def do_everything(file):
                    # lee, valida, corrige, calcula, grafica y maneja estado
                    # todo mezclado en una sola función gigante
                    ...
                """
            ),
            language="python",
        )

    box("<b>Regla de diseño:</b> primero obtén un DataFrame correcto; después calcula; al final diseña la experiencia visual.", "warn")


def data_validation_page():
    section_title("Datos, unidades y validación")
    lesson_header("Inicio › Datos › Calidad antes de calcular", 22)
    st.markdown(
        """
        En apps operativas, gran parte del valor está en manejar bien datos defectuosos. Unidades inconsistentes, columnas variables,
        nulos ocultos, centinelas y merges defectuosos pueden romper una app sin que el error se vea a simple vista.
        """
    )
    issues = pd.DataFrame(
        {
            "Problema": ["Unidades inconsistentes", "Valores centinela", "Columnas con nombres variables", "Series desalineadas", "Duplicados"],
            "Impacto": ["Curvas engañosas", "Nulos invisibles", "Errores silenciosos", "Comparaciones inválidas", "KPIs falsos"],
            "Mitigación": ["Normalizar", "Convertir a NaN", "Mapeo robusto", "Reindexar / interpolar", "Validación y deduplicado"],
        }
    )
    st.dataframe(issues, use_container_width=True)

    raw = pd.DataFrame(
        {
            "Bit depth": np.linspace(10000, 10120, 8),
            "Surface Torque": [12.1, 12.5, -999.25, 13.2, 13.0, 12.9, 13.3, 13.6],
            "RPM": [110, 112, 111, None, 115, 114, 113, 116],
        }
    )
    st.markdown("### Muestra de datos problemáticos")
    st.dataframe(raw, use_container_width=True)

    cleaned = raw.replace(-999.25, np.nan).copy()
    cleaned["Surface Torque_lbf_ft"] = cleaned["Surface Torque"] * 1000.0
    st.markdown("### Después de limpieza mínima")
    st.dataframe(cleaned, use_container_width=True)

    box("<b>Secuencia recomendada:</b> leer → renombrar → detectar unidades → validar nulos y esquema → calcular → graficar.", "info")


def cursor_theory_page():
    section_title("Cursor: teoría de uso")
    lesson_header("Inicio › Cursor › Cómo usarlo bien", 24)
    st.markdown(
        """
        **Cursor** es un **editor con IA** y un **agente de programación**: puedes usarlo para **entender tu base de código**,
        **planificar y desarrollar** funcionalidades, **corregir errores**, **revisar cambios** y **conectar** el flujo con las
        herramientas que ya usas (Git, revisión de código, etc.).

        En la práctica, combina lo que esperas de un IDE con asistencia que lee archivos y propone ediciones; **usarlo bien** no es
        “pedir cualquier cosa”, sino dar **contexto técnico**, **restricciones**, **criterios de éxito** y **iterar** con pedidos acotados.
        """
    )

    link_box(
        "Documentación oficial de Cursor",
        [
            ("Introducción · Bienvenido a Cursor", CURSOR_INTRODUCTION),
            ("Comenzar · primeros pasos", CURSOR_GETTING_STARTED),
            ("Instalación", CURSOR_INSTALL),
            ("Modelos (contexto, capacidades)", CURSOR_MODELS),
            ("Precios y planes de cuenta", CURSOR_ACCOUNT_PRICING),
            ("Registro de cambios (changelog)", CURSOR_CHANGELOG),
        ],
    )

    st.markdown("### Lo que puedes hacer con Cursor")
    cursor_caps = pd.DataFrame(
        {
            "Área": [
                "Entender tu código",
                "Planificar y desarrollar funcionalidades",
                "Encontrar y corregir errores",
                "Revisar cambios",
                "Personalizar Cursor",
                "Conectar tu flujo de trabajo",
            ],
            "Qué implica": [
                "Ver cómo encaja un repositorio y dónde conviene empezar.",
                "Definir alcance, usar modo Plan y abordar trabajos grandes por partes.",
                "Reproducir el problema, acotar la causa y validar el arreglo.",
                "Inspeccionar diffs, correr comprobaciones y detectar riesgos antes de fusionar.",
                "Reglas, skills e indicaciones alineadas al equipo o al proyecto.",
                "Integración con GitHub, GitLab, JetBrains, Slack, Linear y otras herramientas habituales.",
            ],
        }
    )
    st.dataframe(cursor_caps, use_container_width=True, hide_index=True)

    st.markdown("### Modelos (referencia; consulta la doc para valores actualizados)")
    cursor_models_ref = pd.DataFrame(
        {
            "Proveedor": [
                "Anthropic",
                "Anthropic",
                "Cursor",
                "Google",
                "OpenAI",
                "OpenAI",
                "xAI",
            ],
            "Modelo": [
                "Claude 4.6 Sonnet",
                "Claude 4.7 Opus",
                "Composer 2",
                "Gemini 3.1 Pro",
                "GPT-5.3 Codex",
                "GPT-5.4",
                "Grok 4.20",
            ],
            "Contexto predeterminado": ["200k", "200k", "200k", "200k", "272k", "272k", "200k"],
            "Modo máximo": ["1M", "1M", "—", "1M", "—", "1M", "2M"],
        }
    )
    st.dataframe(cursor_models_ref, use_container_width=True, hide_index=True)
    st.caption(
        "Los límites de contexto, planes y nombres de modelo cambian con el tiempo. "
        f"Atributos completos y lista actualizada: [Modelos y precios en docs.cursor.com]({CURSOR_MODELS})."
    )

    chips("Contexto", "Restricciones", "Criterio de éxito", "Iteración", "Validación", "Refactorización")
    flow("Definir problema", "Dar contexto", "Pedir cambio", "Leer respuesta", "Validar", "Iterar")

    rubric_cols = st.columns(4)
    with rubric_cols[0]:
        clarity = st.slider("Problema claro", 0, 10, 8)
    with rubric_cols[1]:
        context = st.slider("Contexto suficiente", 0, 10, 8)
    with rubric_cols[2]:
        constraints = st.slider("Restricciones explícitas", 0, 10, 7)
    with rubric_cols[3]:
        success = st.slider("Criterio de éxito", 0, 10, 9)
    quality = np.mean([clarity, context, constraints, success])
    st.metric("Calidad estimada del pedido", f"{quality:.1f}/10")

    prompt = """
    Quiero una app en Streamlit para comparar Roadmap vs offset wells.

    Contexto:
    - El archivo es un export de DrillSpot con doble encabezado.
    - Debe detectar el pozo activo y los offsets.
    - Necesito panel horizontal con ROP, WOB y RPM.
    - Quiero tabla derecha con delta % por offset.
    - No quiero Azimuth.

    Criterio de éxito:
    - Debe cargar sin romperse si cambia el nombre del well activo.
    - Las unidades deben quedar normalizadas.
    """
    st.code(textwrap.dedent(prompt), language="markdown")
    box("<b>Consejo:</b> usa Cursor como colaborador técnico. Pídele crear, explicar, revisar, depurar y refactorizar, no solo “escribir todo”.", "ok")


def prompt_engineering_page():
    section_title("Prompt engineering para coding")
    lesson_header("Inicio › Cursor › Prompts útiles", 24)
    tab1, tab2, tab3, tab4 = st.tabs(["Crear", "Depurar", "Refactorizar", "Prompt builder en vivo"])

    with tab1:
        st.code(
            textwrap.dedent(
                """
                Add a KPI module that:
                - uploads a DrillSpot roadmap export
                - normalizes units
                - compares active well vs offsets
                - renders a horizontal panel
                - shows a right-side delta table
                """
            ),
            language="markdown",
        )

    with tab2:
        st.code(
            textwrap.dedent(
                """
                The plot is wrong because Surface Torque in the CSV is in klbf.ft,
                but the chart axis is ft·lbf.
                Review the conversion path and patch only the relevant function.
                """
            ),
            language="markdown",
        )

    with tab3:
        st.code(
            textwrap.dedent(
                """
                Refactor this module into:
                - loaders.py
                - validators.py
                - plots.py
                - ui.py
                Preserve behavior, remove duplication and keep the public API stable.
                """
            ),
            language="markdown",
        )

    with tab4:
        feature = st.selectbox("Qué quieres pedir", ["Módulo de carga", "Conversión de unidades", "Roadmap panel", "Corredor FF", "Parser BHA", "Deploy checklist"])
        symptoms = st.text_input("Síntoma / necesidad")
        restrictions = st.text_area("Restricciones / contexto")
        success_crit = st.text_area("Criterio de éxito")
        generated = f"""Task: {feature}

Context:
{restrictions if restrictions else '- add context here'}

Symptom or goal:
{symptoms if symptoms else '- describe the goal or bug'}

Success criteria:
{success_crit if success_crit else '- define how to validate success'}

Please return:
- code
- short explanation
- validation checklist
"""
        st.code(generated, language="markdown")

    box("<b>Fórmula útil:</b> síntoma o meta + contexto real + restricciones + criterio de éxito + forma esperada de la respuesta.", "info")


def install_python_page():
    section_title("Entorno local: instalar Python y pip")
    lesson_header("Inicio › Setup › Python y pip", 18)
    link_box(
        "Enlaces oficiales",
        [
            ("Descargar Python", PYTHON_DL),
            ("Guía oficial para instalar paquetes", PIP_GUIDE),
            ("Guía oficial de venv + pip", PIP_VENV),
        ],
    )

    st.markdown(
        """
        Flujo recomendado para empezar localmente:

        1. Instalar Python desde el sitio oficial.  
        2. Verificar que Python y pip estén disponibles en terminal.  
        3. Crear un entorno virtual.  
        4. Activarlo.  
        5. Instalar librerías con pip.
        """
    )

    tab1, tab2 = st.tabs(["macOS / Linux", "Windows"])
    with tab1:
        st.code(
            textwrap.dedent(
                """
                python3 --version
                python3 -m pip --version

                python3 -m venv .venv
                source .venv/bin/activate

                python3 -m pip install --upgrade pip setuptools wheel
                """
            ),
            language="bash",
        )
    with tab2:
        st.code(
            textwrap.dedent(
                r"""
                py --version
                py -m pip --version

                py -m venv .venv
                .venv\Scripts\activate

                py -m pip install --upgrade pip setuptools wheel
                """
            ),
            language="powershell",
        )

    box("<b>Recomendación:</b> trabaja dentro de un entorno virtual para no mezclar librerías de proyectos distintos.", "ok")


def pip_libraries_page():
    section_title("Librerías principales con pip")
    lesson_header("Inicio › Setup › Instalar el stack", 18)
    link_box(
        "Documentación oficial",
        [
            ("Instalar paquetes con pip", PIP_GUIDE),
            ("Crear entorno virtual con venv", PIP_VENV),
        ],
    )

    libs = pd.DataFrame(
        {
            "Librería": ["streamlit", "pandas", "numpy", "plotly", "scipy", "pydantic", "openpyxl"],
            "Para qué sirve": [
                "Framework de la app",
                "Datos tabulares",
                "Cálculo numérico",
                "Gráficas interactivas",
                "Métodos numéricos",
                "Validación de datos",
                "Leer / escribir Excel",
            ],
        }
    )
    st.dataframe(libs, use_container_width=True)

    st.code(
        textwrap.dedent(
            """
            python -m pip install streamlit pandas numpy plotly scipy pydantic openpyxl
            """
        ),
        language="bash",
    )
    st.caption("En Windows puedes usar `py -m pip install ...`; en macOS/Linux suele usarse `python3 -m pip install ...`.")

    st.markdown("### Archivo requirements.txt recomendado")
    st.code(
        textwrap.dedent(
            """
            streamlit
            pandas
            numpy
            plotly
            scipy
            pydantic
            openpyxl
            """
        ),
        language="text",
    )

    box("<b>Consejo:</b> para enseñar o desplegar, usa `requirements.txt` y evita depender solo de lo que tienes instalado en tu máquina.", "warn")


def roadmap_case_page():
    section_title("Caso Rogii · Roadmap comparativo")
    lesson_header("Inicio › Casos Rogii › Roadmap", 20)
    st.markdown("Caso típico: comparar pozo activo contra roadmap y offsets con panel claro y tabla de deltas.")
    metrics = ["ROP", "WOB", "Surface RPM"]
    selected = st.multiselect("Métricas visibles", metrics, default=metrics)
    offset_n = st.slider("Número de offsets", 1, 4, 2)
    df = generate_roadmap_demo(offset_n)
    wells = ["Roadmap", "Active Well"] + [f"Offset {i+1}" for i in range(offset_n)]
    rows = max(1, len(selected))
    fig = make_subplots(rows=1, cols=rows, shared_yaxes=True, subplot_titles=selected)
    palette = px.colors.qualitative.Set2
    for c, metric in enumerate(selected, start=1):
        for i, well in enumerate(wells):
            fig.add_trace(
                go.Scatter(
                    x=df[f"{well}_{metric}"],
                    y=df["Depth"],
                    mode="lines",
                    name=f"{well} · {metric}",
                    line=dict(width=2, color=palette[i % len(palette)]),
                    showlegend=(c == 1),
                ),
                row=1,
                col=c,
            )
    fig.update_yaxes(autorange="reversed", title_text="Depth")
    fig.update_layout(height=600, title="Panel comparativo tipo Roadmap vs offsets")
    dark_layout(fig)
    st.plotly_chart(fig, use_container_width=True)

    summary = []
    for off in [f"Offset {i+1}" for i in range(offset_n)]:
        summary.append(
            {
                "Offset": off,
                "Δ ROP % vs Active": 100 * (df[f"{off}_ROP"].mean() - df["Active Well_ROP"].mean()) / df["Active Well_ROP"].mean(),
                "Δ WOB % vs Active": 100 * (df[f"{off}_WOB"].mean() - df["Active Well_WOB"].mean()) / df["Active Well_WOB"].mean(),
                "Δ RPM % vs Active": 100 * (df[f"{off}_Surface RPM"].mean() - df["Active Well_Surface RPM"].mean()) / df["Active Well_Surface RPM"].mean(),
            }
        )
    st.dataframe(pd.DataFrame(summary).round(2), use_container_width=True)

    st.code(
        textwrap.dedent(
            """
            roadmap_cmp = load_drillspot_roadmap_comparison_export(file)
            selected_metrics = ["ROP", "WOB", "Surface RPM"]
            selected_wells = ["Roadmap", active_well, *offsets]

            fig = build_roadmap_offset_comparison_figure(
                roadmap_cmp,
                selected_metrics=selected_metrics,
                selected_wells=selected_wells,
                palette_name="DrillSpot Pro",
            )
            """
        ),
        language="python",
    )
    box("<b>Aprendizaje:</b> este caso mezcla parsing, layout visual, comparación operativa y reglas de negocio.", "info")


def torque_drag_case_page():
    section_title("Caso Rogii · Torque & Drag")
    lesson_header("Inicio › Casos Rogii › Torque & Drag", 20)
    ff_min, ff_max = st.slider("Rango del corredor FF", 0.10, 0.70, (0.30, 0.45), 0.01)
    show_corridor = st.checkbox("Mostrar corredor sombreado", value=True)

    depth = np.linspace(0, 16000, 220)
    curve_lo = 40 + 0.004 * depth + 8 * np.sin(depth / 1800) * ff_min
    curve_hi = 40 + 0.004 * depth + 8 * np.sin(depth / 1800) * ff_max
    measured = 42 + 0.0042 * depth + 3 * np.sin(depth / 1700) + np.cos(depth / 1200)

    fig = go.Figure()
    if show_corridor:
        fig.add_trace(go.Scatter(x=curve_lo, y=depth, mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
        fig.add_trace(
            go.Scatter(
                x=curve_hi,
                y=depth,
                mode="lines",
                fill="tonextx",
                name=f"Corredor FF {ff_min:.2f}-{ff_max:.2f}",
                line=dict(width=0),
                fillcolor="rgba(96,165,250,0.18)",
            )
        )
    fig.add_trace(go.Scatter(x=curve_lo, y=depth, mode="lines", name=f"Modelo FF {ff_min:.2f}", line=dict(color="#38bdf8", width=2)))
    fig.add_trace(go.Scatter(x=curve_hi, y=depth, mode="lines", name=f"Modelo FF {ff_max:.2f}", line=dict(color="#f59e0b", width=2)))
    fig.add_trace(go.Scatter(x=measured, y=depth, mode="lines", name="Curva medida", line=dict(color="#f472b6", width=2.5, dash="dash")))
    fig.update_yaxes(autorange="reversed", title_text="Depth")
    fig.update_layout(height=620, title="Torque & Drag con corredor de factor de fricción")
    dark_layout(fig)
    st.plotly_chart(fig, use_container_width=True)

    sections = pd.DataFrame(
        {
            "Sección": ["Surface", "Intermediate", "Production"],
            "FF mínimo": [ff_min, min(ff_min + 0.02, 1.0), min(ff_min + 0.04, 1.0)],
            "FF máximo": [ff_max, min(ff_max + 0.03, 1.0), min(ff_max + 0.05, 1.0)],
        }
    )
    st.dataframe(sections.round(2), use_container_width=True)

    st.code(
        textwrap.dedent(
            """
            if show_corridor and ff_range:
                ff_lo, ff_hi = min(ff_range), max(ff_range)
                curve_lo = interp_ff_curve(model_df, fam_map, "PU", ff_lo)
                curve_hi = interp_ff_curve(model_df, fam_map, "PU", ff_hi)
                _trip_td_add_corridor_band(fig, curve_lo, curve_hi, depth, f"Corredor FF {ff_lo:.2f}-{ff_hi:.2f}")
            """
        ),
        language="python",
    )

    box("<b>Mensaje pedagógico:</b> un corredor comunica incertidumbre y sensibilidad; una curva única puede dar falsa certeza.", "warn")


def bha_case_page():
    section_title("Caso Rogii · BHA parser")
    lesson_header("Inicio › Casos Rogii › BHA", 18)

    default = pd.DataFrame(
        {
            "Component": ["Bit", "Motor", "Stabilizer", "HWDP", "Drill Collar"],
            "Length_ft": [1.2, 29.5, 3.0, 120.0, 180.0],
            "OD_in": [8.5, 6.75, 6.75, 5.0, 6.5],
            "ID_in": [0.8, 2.25, 2.5, 2.75, 2.81],
            "Weight_lbft": [120, 165, 140, 49, 110],
        }
    )
    st.dataframe(default, use_container_width=True)

    total_len = default["Length_ft"].sum()
    avg_od = np.average(default["OD_in"], weights=default["Length_ft"])
    avg_id = np.average(default["ID_in"], weights=default["Length_ft"])
    total_weight = np.sum(default["Length_ft"] * default["Weight_lbft"])

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Longitud total", f"{total_len:.1f} ft")
    with c2:
        st.metric("OD promedio", f"{avg_od:.2f} in")
    with c3:
        st.metric("ID promedio", f"{avg_id:.2f} in")
    with c4:
        st.metric("Peso total", f"{total_weight:,.0f} lb")

    st.code(
        textwrap.dedent(
            """
            if bha_file is not None:
                bha_raw, err = read_bha_upload_to_table(bha_file)
                bha_df, bha_summary, err2 = bha_table_to_summary(bha_raw)
                st.session_state["tadp_bha_uploaded_df"] = bha_df
                st.session_state["tadp_bha_uploaded_summary"] = bha_summary
            """
        ),
        language="python",
    )
    box("<b>Buena UX:</b> mostrar tabla y resumen inmediatamente aumenta confianza y reduce riesgo de usar parámetros mal interpretados.", "ok")


def python_lab_page():
    section_title("Laboratorio de Python interactivo")
    lesson_header("Inicio › Laboratorio › Ejercicios didácticos", 30)
    tabs = st.tabs(["Quiz base", "Conversión de unidades", "Datos tabulares", "Caso aplicado"])

    with tabs[0]:
        q1 = st.radio("1) Programar es...", ["Memorizar sintaxis", "Definir reglas y transformaciones", "Solo graficar"], key="quiz1")
        q2 = st.radio("2) Un diccionario sirve para...", ["Guardar pares clave-valor", "Solo números", "Solo listas"], key="quiz2")
        q3 = st.radio("3) Validar datos significa...", ["Ignorar faltantes", "Verificar que el input tenga forma y valores esperados", "Cambiar el color de una gráfica"], key="quiz3")
        if st.button("Revisar quiz"):
            score = 0
            score += int(q1 == "Definir reglas y transformaciones")
            score += int(q2 == "Guardar pares clave-valor")
            score += int(q3 == "Verificar que el input tenga forma y valores esperados")
            st.success(f"Resultado: {score}/3")

    with tabs[1]:
        val = st.slider("Torque en klbf·ft", 1.0, 30.0, 12.0, 0.5)
        mode = st.radio("Unidad objetivo", ["field", "metric"], horizontal=True, key="lab_unit")
        res = val * 1000.0 if mode == "field" else val * 1000.0 * 1.3558179483314
        st.metric("Torque convertido", f"{res:,.2f} {'lbf·ft' if mode == 'field' else 'N·m'}")
        st.code(
            textwrap.dedent(
                """
                def convert_torque_from_surface(series, unit_mode="metric"):
                    s = pd.to_numeric(series, errors="coerce")
                    s_lbf_ft = s * 1000.0
                    return s_lbf_ft if unit_mode == "field" else s_lbf_ft * 1.3558179483314
                """
            ),
            language="python",
        )

    with tabs[2]:
        demo = pd.DataFrame(
            {
                "Bit depth": np.linspace(10200, 10290, 7),
                "Hookload": [182, 184, 183, 185, 187, 186, 188],
                "Surface Torque": [12.0, 12.5, 12.1, 12.8, 13.0, 12.9, 13.2],
                "RPM": [118, 119, 120, 121, 120, 122, 123],
            }
        )
        selected = st.multiselect("Columnas visibles", demo.columns.tolist(), default=["Bit depth", "Hookload", "Surface Torque"])
        st.dataframe(demo[selected], use_container_width=True)

    with tabs[3]:
        wob = st.slider("WOB", 5.0, 45.0, 20.0, 0.5, key="lab_wob")
        rpm = st.slider("RPM", 60.0, 220.0, 120.0, 1.0, key="lab_rpm")
        flow_rate = st.slider("Flow rate", 250.0, 900.0, 500.0, 10.0, key="lab_flow")
        rop_est = 2.4 * np.sqrt(max(wob, 1)) + 0.09 * rpm + 0.004 * flow_rate - 0.45
        vib_est = 0.10 + 0.010 * wob + 0.003 * rpm - 0.0007 * flow_rate
        c1, c2 = st.columns(2)
        with c1:
            st.metric("ROP estimada", f"{rop_est:.2f}")
        with c2:
            st.metric("Vibración estimada", f"{vib_est:.2f}")
        answer = st.radio("¿Qué interpretación es mejor?", ["Maximizar ROP a cualquier costo", "Balancear desempeño y estabilidad", "Ignorar vibración"], key="lab_ans")
        if st.button("Revisar caso"):
            if answer == "Balancear desempeño y estabilidad":
                st.success("Correcto. La optimización técnica real balancea productividad y restricciones.")
            else:
                st.error("No. La decisión técnica debe balancear velocidad con estabilidad y riesgo.")

    box("<b>Enfoque didáctico:</b> teoría breve, interacción, feedback inmediato y conexión con decisiones de ingeniería.", "exercise")


def live_coding_page():
    section_title("Live coding studio pro")
    lesson_header("Inicio › Live coding › Prompt + código + checklist", 32)
    st.markdown(
        """
        Esta sección simula una sesión real de live coding en clase.
        Definimos la tarea, la interfaz deseada, el nivel de validación y el estilo de arquitectura, y la app genera
        un prompt pro, un esqueleto de código y un checklist para revisar el resultado.
        """
    )

    task = st.selectbox("Tipo de feature", ["Carga y preview", "Conversión de unidades", "Roadmap panel", "Corredor FF", "Parser BHA", "Deploy helper"])
    ui_style = st.selectbox("Estilo de interfaz", ["Minimal", "Technical dashboard", "Training app", "Operations app"])
    validation = st.selectbox("Nivel de validación", ["Ligera", "Media", "Estrica"])
    preserve_api = st.checkbox("Pedir preservar API pública", value=True)
    include_tests = st.checkbox("Pedir mini checklist de pruebas", value=True)

    prompts = {
        "Carga y preview": """
            import pandas as pd
            import streamlit as st

            uploaded = st.file_uploader("Sube un archivo", type=["csv", "xlsx"])
            if uploaded is not None:
                df = pd.read_csv(uploaded)
                st.dataframe(df.head(20), use_container_width=True)
        """,
        "Conversión de unidades": """
            import pandas as pd

            def convert_torque_from_surface(series, unit_mode="metric"):
                s = pd.to_numeric(series, errors="coerce")
                s_lbf_ft = s * 1000.0
                return s_lbf_ft if unit_mode == "field" else s_lbf_ft * 1.3558179483314
        """,
        "Roadmap panel": """
            def build_roadmap_panel(df, selected_metrics, selected_wells):
                validate_schema(df)
                df = normalize_units(df)
                fig = build_tracks(df, selected_metrics, selected_wells)
                delta_df = compute_deltas(df, selected_wells)
                return fig, delta_df
        """,
        "Corredor FF": """
            def add_corridor_band(fig, curve_lo, curve_hi, depth, label):
                fig.add_trace(...)
                fig.add_trace(...)
                return fig
        """,
        "Parser BHA": """
            def bha_table_to_summary(raw_bha):
                # parse table
                # normalize fields
                # compute summary
                return bha_df, bha_summary, None
        """,
        "Deploy helper": """
            def write_requirements_file():
                packages = ["streamlit", "pandas", "numpy", "plotly", "scipy", "pydantic", "openpyxl"]
                return "\\n".join(packages)
        """,
    }

    prompt = f"""
Build a {ui_style.lower()} feature for: {task}.

Context:
- This is for Rogii-style technical apps.
- The code should be readable for teaching and maintenance.
- Use modular architecture.
- Validation level: {validation}.
- {'Preserve the public API.' if preserve_api else 'You may redesign the API if needed.'}
- {'Return a short testing checklist.' if include_tests else 'Testing checklist is optional.'}

Deliver:
- implementation
- brief explanation
- validation notes
"""
    st.markdown("### Prompt sugerido")
    st.code(textwrap.dedent(prompt), language="markdown")

    st.markdown("### Esqueleto inicial")
    st.code(textwrap.dedent(prompts[task]), language="python")

    checklist = pd.DataFrame(
        {
            "Chequeo": [
                "¿Carga sin romperse?",
                "¿Valida entradas?",
                "¿Las unidades son correctas?",
                "¿La UI es legible?",
                "¿La lógica está separada?",
                "¿El usuario puede validar lo que ve?",
            ],
            "Estado": ["Pendiente"] * 6,
        }
    )
    st.dataframe(checklist, use_container_width=True)

    box("<b>Uso docente:</b> el instructor puede editar estos campos en vivo y mostrar cómo cambia la calidad del prompt y de la arquitectura resultante.", "info")


def darcy_jusset_pena_page():
    section_title("Caso Jusset Peña · Darcy + Python + prompts")
    lesson_header("Inicio › Casos especiales › Jusset Peña · vibe coding", 28)
    objective_box(
        "Objetivos (programación y vibe coding)",
        [
            "Usar la ecuación de Darcy como caso concreto para practicar descomposición en código (variables, funciones, unidades).",
            "Traducir física + restricciones + criterio de éxito en prompts útiles para Cursor.",
            "Generar y revisar prompts listos para pegar en el asistente, no solo mirar números.",
        ],
    )
    chips("Vibe coding", "Python", "Prompts", "Cursor", "Unidades", "Iteración")

    st.markdown(
        """
        **Caso dedicado a Jusset Peña.** La física es el *pretexto*: lo importante es **cómo planteas el problema como programa**
        y **cómo se lo pides a la IA**. La ley de Darcy en 1D para flujo lineal incompresible es:
        """
    )
    st.latex(r"Q = \frac{k \, A \, \Delta P}{\mu \, L}")
    flow("Física", "Variables y unidades", "Función Python", "UI Streamlit (opcional)", "Prompt claro", "Revisar código generado")

    box(
        "<b>Vibe coding:</b> la IA no adivina tu intención. Define entrada, salida, reglas y validación; el prompt empaqueta eso "
        "para obtener código mantenible.",
        "info",
    )

    tab_py, tab_prompt, tab_demo = st.tabs(
        [
            "1 · Python: del modelo al código",
            "2 · Laboratorio de prompts (Cursor)",
            "3 · Demo numérica (referencia)",
        ]
    )

    with tab_py:
        st.markdown(
            """
            **Mapa mental.** Separa: (a) conversiones de unidades, (b) núcleo matemático en SI, (c) presentación.
            Así reduces errores (mezclar psi con Pa, mD con m²) y tu prompt puede pedir esa estructura.
            """
        )
        st.code(
            textwrap.dedent(
                """
                M2_PER_MD = 9.869233e-16   # 1 mD → m²
                PA_PER_PSI = 6894.757293168
                CP_TO_PA_S = 0.001

                def darcy_q_m3s(*, k_md: float, mu_cp: float, delta_psi: float, area_m2: float, L_m: float) -> float:
                    \"\"\"Flujo lineal 1D. Retorna Q en m³/s. Entradas en unidades de campo + SI donde se indica.\"\"\"
                    if L_m <= 0:
                        raise ValueError("L_m debe ser > 0")
                    k_m2 = k_md * M2_PER_MD
                    mu_pa_s = mu_cp * CP_TO_PA_S
                    dP_pa = delta_psi * PA_PER_PSI
                    return (k_m2 * area_m2 * dP_pa) / (mu_pa_s * L_m)
                """
            ),
            language="python",
        )
        st.caption("Pídele a Cursor que extraiga constantes, añada type hints y pruebas; el prompt debe nombrar unidades y el contrato de la función.")

        st.markdown("### Mini check (conceptos de código)")
        q = st.radio(
            "En la función anterior, si olvidas convertir `k_md` a m² y usas el número en mD directamente en la fórmula, ¿qué ocurre?",
            [
                "El resultado tiene escala incorrecta (órdenes de magnitud mal)",
                "Python siempre corrige las unidades solo",
                "No pasa nada si los sliders “se ven bien”",
            ],
            key="darcy_quiz_code",
        )
        if st.button("Comprobar", key="darcy_check_code"):
            if q.startswith("El resultado"):
                st.success("Correcto: las unidades no son magia; hay que codificarlas explícitamente.")
            else:
                st.error("La computadora ejecuta lo que escribes; la coherencia dimensional es tu responsabilidad en el código.")

    with tab_prompt:
        st.markdown(
            """
            **Objetivo:** mismo patrón que en el curso: *tarea + contexto + restricciones + criterio de éxito + formato de respuesta*.
            """
        )
        goal = st.selectbox(
            "Qué quieres que genere la IA",
            [
                "Función pura `darcy_q_*` con conversiones y validación",
                "Página Streamlit con sliders y gráfica Plotly de sensibilidad",
                "Refactor: separar constants.py / darcy.py / ui.py",
                "Mini tests (pytest) para el cálculo de Darcy",
            ],
            key="darcy_prompt_goal",
        )
        ctx = st.text_area(
            "Contexto (datos, supuestos)",
            "Flujo lineal 1D, medio homogéneo, fluido incompresible. Entradas típicas: k en mD, μ en cP, ΔP en psi, A en m², L en m.",
            key="darcy_prompt_ctx",
        )
        constraints = st.text_area(
            "Restricciones",
            "Python 3.10+, sin dependencias nuevas salvo streamlit/plotly si pide UI. Comentarios breves. Nombres en inglés para código.",
            key="darcy_prompt_constraints",
        )
        success = st.text_area(
            "Criterio de éxito (cómo validar)",
            "Los resultados coinciden con una calculadora manual en un caso; falla con mensaje claro si L<=0 o μ<=0.",
            key="darcy_prompt_success",
        )
        tone = st.radio("Idioma del prompt generado", ["Inglés (recomendado para modelos)", "Español"], horizontal=True, key="darcy_prompt_lang")

        if tone.startswith("Inglés"):
            generated = f"""Task: {goal}

Context:
{ctx}

Constraints:
{constraints}

Success criteria:
{success}

Please return:
1) the code
2) a short note on units (SI path)
3) a 3-item manual validation checklist
"""
        else:
            generated = f"""Tarea: {goal}

Contexto:
{ctx}

Restricciones:
{constraints}

Criterio de éxito:
{success}

Por favor devuelve:
1) el código
2) una nota breve sobre unidades (camino SI)
3) un mini checklist de validación manual (3 ítems)
"""

        st.markdown("### Prompt generado (cópialo y pégalo)")
        st.code(generated, language="markdown")

        with st.expander("Ejemplos de prompts base (ajústalos)"):
            st.code(
                textwrap.dedent(
                    """
                    Create a pure Python function for 1D linear Darcy flow Q = k*A*dP/(mu*L).
                    Inputs: k in mD, mu in cP, delta_P in psi, area in m^2, length in m.
                    Convert to SI inside the function, return Q in m^3/s. Raise ValueError for invalid L or mu.

                    Add a Streamlit sidebar with sliders and show Q in m^3/d and bbl/d. Use Plotly for Q vs k sensitivity.
                    """
                ),
                language="markdown",
            )
            st.code(
                textwrap.dedent(
                    """
                    Refactor my single-file Darcy script into:
                    - constants.py (conversion factors only)
                    - darcy.py (compute functions)
                    - app.py (Streamlit UI)
                    Preserve behavior. No behavior change.
                    """
                ),
                language="markdown",
            )

        box(
            "<b>Consejo:</b> cuanto más precisas sean las unidades y el contrato de la función en el prompt, menos errores de escala obtendrás.",
            "warn",
        )

    M2_PER_MD = 9.869233e-16
    PA_PER_PSI = 6894.757293168
    CP_TO_PA_S = 0.001
    M3_PER_BBL = 0.158987304

    with tab_demo:
        st.markdown(
            """
            **Demo de referencia.** Si tu código o el de Cursor reproduce magnitudes similares para los mismos inputs, vas bien.
            Si no, revisa conversiones antes de culpar al modelo.
            """
        )
        c1, c2 = st.columns(2)
        with c1:
            k_md = st.slider("Permeabilidad k (mD)", 0.1, 500.0, 50.0, 0.1, key="darcy_k")
            mu_cp = st.slider("Viscosidad μ (cP)", 0.2, 50.0, 1.0, 0.1, key="darcy_mu")
            delta_psi = st.slider("Caída de presión ΔP (psi)", 1.0, 5000.0, 500.0, 10.0, key="darcy_dp")
        with c2:
            area_m2 = st.slider("Área transversal A (m²)", 1e-4, 0.05, 0.01, 1e-4, format="%.4f", key="darcy_a")
            L_m = st.slider("Longitud del tramo L (m)", 0.1, 200.0, 10.0, 0.5, key="darcy_L")

        k_m2 = k_md * M2_PER_MD
        mu_pa_s = mu_cp * CP_TO_PA_S
        dP_pa = delta_psi * PA_PER_PSI

        if L_m <= 0 or mu_pa_s <= 0:
            st.error("L y μ deben ser mayores que cero.")
        else:
            Q_m3_s = (k_m2 * area_m2 * dP_pa) / (mu_pa_s * L_m)
            Q_m3_d = Q_m3_s * 86400.0
            Q_bbl_d = Q_m3_d / M3_PER_BBL

            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Caudal Q", f"{Q_m3_d:,.4f} m³/d")
            with m2:
                st.metric("Caudal (campo)", f"{Q_bbl_d:,.2f} bbl/d")
            with m3:
                st.metric("k en SI", f"{k_m2:.3e} m²")

            fig = go.Figure()
            k_sweep = np.linspace(max(0.1, k_md * 0.05), k_md * 3, 80)
            k_m2_s = k_sweep * M2_PER_MD
            Q_sweep = (k_m2_s * area_m2 * dP_pa) / (mu_pa_s * L_m) * 86400.0
            fig.add_trace(go.Scatter(x=k_sweep, y=Q_sweep, mode="lines", name="Q vs k", line=dict(color="#38bdf8", width=2.5)))
            fig.add_vline(x=k_md, line_dash="dash", line_color="#f97316", annotation_text="k actual")
            fig.update_layout(height=420, title="Sensibilidad del caudal a la permeabilidad (resto fijo)")
            fig.update_xaxes(title_text="k (mD)")
            fig.update_yaxes(title_text="Q (m³/d)")
            dark_layout(fig)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Pregunta física rápida")
        q_phys = st.radio(
            "Si aumentas solo la viscosidad y mantienes k, A, ΔP y L constantes, el caudal volumétrico…",
            ["Aumenta", "Disminuye", "No cambia"],
            key="darcy_quiz",
        )
        if st.button("Comprobar respuesta", key="darcy_check"):
            if q_phys == "Disminuye":
                st.success("Correcto: Q ∝ 1/μ; en el código μ va en el denominador.")
            else:
                st.error("Revisa la ecuación: al programarla, μ va en el denominador.")

        st.code(
            textwrap.dedent(
                """
                # Núcleo (SI): Q_m3_s = (k_m2 * area_m2 * dP_pa) / (mu_pa_s * L_m)
                """
            ),
            language="python",
        )

    box(
        "<b>Nota:</b> supuestos de medio homogéneo e incompresible; en campo añadirías más física. En vibe coding, deja extensiones como "
        "«siguiente iteración» explícita en tu prompt.",
        "info",
    )


def statistics_p10_p90_boxplots_page():
    section_title("P10, P90, box plots y estadística (vibe coding)")
    lesson_header("Inicio › Datos › Percentiles, box plots y prompts", 26)
    objective_box(
        "Objetivos",
        [
            "Entender P10, P50 y P90 como resúmenes de una distribución (no son “promedios” arbitrarios).",
            "Leer un box plot: mediana, cuartiles, bigotes y valores atípicos típicos en Plotly.",
            "Escribir prompts para que Cursor genere tablas, percentiles y gráficos en Streamlit con Pandas/NumPy/Plotly.",
        ],
    )
    chips("Percentiles", "Box plot", "Pandas", "Plotly", "Prompts", "QC de datos")

    st.markdown(
        """
        En ingeniería y datos de pozo, **P10 / P50 / P90** suelen usarse como escenarios: valores que dejan por debajo el 10 %, 50 % o 90 %
        de los datos (definición depende del convenio del equipo: a veces “P90” es el pesimista u optimista; **alinea el criterio antes de calcular**).

        - **P50** ≈ **mediana**: mitad de los valores por encima y mitad por debajo (si la distribución es simétrica, coincide con la media; si es sesgada, no).
        - **P10** y **P90** acotan colas: sirven para rangos de incertidumbre y comparar pozos o campañas.

        Un **box plot** resume la distribución: caja = cuartiles Q1–Q3 (rango intercuartílico), línea central = mediana, bigotes = extensión de los datos
        (en Plotly/Matplotlib por defecto suelen seguir reglas tipo 1.5×IQR para marcar *outliers*).
        """
    )

    conc = pd.DataFrame(
        {
            "Medida": ["P10", "P50 (mediana)", "P90"],
            "Idea operativa": [
                "Cola inferior (pocos valores más bajos que esto).",
                "Centro robusto ante valores extremos.",
                "Cola superior (pocos valores más altos que esto).",
            ],
            "En Python (serie numérica `s`)": [
                "`np.percentile(s, 10)` o `s.quantile(0.10)`",
                "`np.percentile(s, 50)` o `s.median()`",
                "`np.percentile(s, 90)` o `s.quantile(0.90)`",
            ],
        }
    )
    st.dataframe(conc, use_container_width=True, hide_index=True)

    tab_demo, tab_prompts = st.tabs(["Demo interactiva", "Prompts ejemplo (Cursor)"])

    with tab_demo:
        st.markdown("### Datos sintéticos tipo métrica de pozo (sesgados)")
        rng = np.random.default_rng(7)
        n = st.slider("Número de puntos", 50, 800, 200, 10, key="stat_n")
        skew = st.slider("Sesgo (log-normal)", 0.15, 0.8, 0.35, 0.05, key="stat_skew")
        base = st.slider("Valor base (ej. ROP)", 10.0, 80.0, 42.0, 0.5, key="stat_base")
        s = pd.Series(base * 0.12 * rng.lognormal(mean=1.0, sigma=skew, size=n) + base * 0.88)

        p10, p50, p90 = np.percentile(s, [10, 50, 90])
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("P10", f"{p10:.2f}")
        with c2:
            st.metric("P50", f"{p50:.2f}")
        with c3:
            st.metric("P90", f"{p90:.2f}")
        with c4:
            st.metric("Media", f"{float(s.mean()):.2f}")

        fig = go.Figure()
        fig.add_trace(go.Box(y=s, name="Métrica", marker_color="#38bdf8", boxmean="sd"))
        fig.add_hline(y=p10, line_dash="dot", line_color="#f97316", annotation_text="P10")
        fig.add_hline(y=p50, line_dash="solid", line_color="#22c55e", annotation_text="P50")
        fig.add_hline(y=p90, line_dash="dot", line_color="#a78bfa", annotation_text="P90")
        fig.update_layout(height=480, title="Box plot + líneas P10 / P50 / P90 (referencia)")
        fig.update_yaxes(title_text="Valor")
        dark_layout(fig)
        st.plotly_chart(fig, use_container_width=True)

        st.code(
            textwrap.dedent(
                """
                import numpy as np
                import pandas as pd

                p10, p50, p90 = np.percentile(series.dropna(), [10, 50, 90])
                # o con pandas:
                q = series.quantile([0.10, 0.50, 0.90])
                """
            ),
            language="python",
        )

    with tab_prompts:
        st.markdown(
            """
            Pide a Cursor **contexto de negocio** (qué es cada columna), **definición de P10/P90** del equipo y **salidas esperadas** (tabla + figura).
            """
        )
        st.code(
            textwrap.dedent(
                """
                Build a Streamlit page that:
                - loads a CSV with a numeric column "ROP_ft_hr" (may have NaNs)
                - computes P10, P50, P90 with pandas/numpy and shows them as metrics
                - plots a Plotly box plot of the column
                - adds a short markdown note explaining P10/P50/P90 in plain language for operations staff
                Use Python 3.10+, streamlit, pandas, plotly. No seaborn.
                """
            ),
            language="markdown",
        )
        st.code(
            textwrap.dedent(
                """
                Add a function percentile_summary(df: pd.DataFrame, col: str) -> pd.Series that returns P10,P50,P90,count,mean,std.
                Add unit tests with a tiny fixed DataFrame. Then wire it into my Streamlit sidebar.
                """
            ),
            language="markdown",
        )
        st.code(
            textwrap.dedent(
                """
                Create side-by-side Plotly box plots for columns "ROP" and "WOB" grouped by "well_name" from a pandas DataFrame.
                Highlight outliers explain briefly in captions (what Plotly considers outlier points).
                """
            ),
            language="markdown",
        )

    box(
        "<b>Vibe coding:</b> especifica columna, manejo de nulos, si los percentiles son por pozo o globales, y el tipo de gráfico; "
        "pide validación cruzada con un cálculo manual en un ejemplo pequeño.",
        "ok",
    )


def refactor_page():
    section_title("Refactorización guiada")
    lesson_header("Inicio › Refactorización › Mejorar sin romper", 24)
    before_code = """
    import pandas as pd
    import streamlit as st

    def run_everything(file):
        df = pd.read_excel(file)
        df["Surface Torque"] = pd.to_numeric(df["Surface Torque"], errors="coerce") * 1000.0
        if "Depth" not in df.columns:
            st.error("Falta Depth")
            return
        fig = ...
        st.plotly_chart(fig)
        return df
    """
    after_code = """
    # loaders.py
    def read_input(file):
        return pd.read_excel(file)

    # validators.py
    def validate_schema(df):
        required = ["Depth", "Surface Torque"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")

    # units.py
    def normalize_surface_torque(series):
        s = pd.to_numeric(series, errors="coerce")
        return s * 1000.0

    # ui.py
    def render_panel(df):
        fig = ...
        st.plotly_chart(fig)
    """
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Antes")
        st.code(textwrap.dedent(before_code), language="python")
    with c2:
        st.markdown("### Después")
        st.code(textwrap.dedent(after_code), language="python")

    score = st.slider("¿Qué tanto mejoró la mantenibilidad?", 0, 10, 8)
    st.progress(score / 10)
    box("<b>Refactorizar</b> no es reescribir por gusto: es separar responsabilidades, reducir duplicación y hacer el sistema más estable para crecer.", "warn")


def deploy_page():
    section_title("Deploy de la app paso a paso")
    lesson_header("Inicio › Deploy › De local a compartible", 22)
    link_box(
        "Enlaces oficiales de Streamlit",
        [
            ("Resumen de Community Cloud", STREAMLIT_OVERVIEW),
            ("Deploy paso a paso", STREAMLIT_DEPLOY),
            ("Organización de archivos", STREAMLIT_FILEORG),
        ],
    )

    st.markdown(
        """
        Flujo recomendado para desplegar una app Streamlit:

        1. Tener la app funcionando localmente.  
        2. Guardar el código en un repositorio GitHub.  
        3. Crear `requirements.txt`.  
        4. Verificar que el archivo de entrada esté claro (`app.py` o similar).  
        5. Entrar a Streamlit Community Cloud y crear la app.  
        6. Seleccionar repositorio, rama y archivo de entrada.  
        7. Configurar Python version y secretos si hace falta.  
        8. Desplegar y revisar logs.
        """
    )
    flow("App local", "Repo GitHub", "requirements.txt", "Entrypoint", "Community Cloud", "Deploy", "Logs", "Compartir URL")

    st.markdown("### Estructura mínima del repo")
    st.code(
        textwrap.dedent(
            """
            my-app/
            ├─ app.py
            ├─ requirements.txt
            ├─ modules/
            ├─ utils/
            └─ assets/
            """
        ),
        language="text",
    )

    st.markdown("### requirements.txt de ejemplo")
    st.code(
        textwrap.dedent(
            """
            streamlit
            pandas
            numpy
            plotly
            scipy
            pydantic
            openpyxl
            """
        ),
        language="text",
    )

    readiness = st.multiselect(
        "Marca lo que ya tienes listo",
        ["App corre local", "Repo en GitHub", "requirements.txt", "Entrypoint claro", "Secrets definidos si aplica"],
    )
    st.metric("Nivel de preparación", f"{len(readiness)}/5")
    st.progress(len(readiness) / 5)

    box("<b>Consejo:</b> si funciona solo en tu laptop pero no está documentado ni tiene dependencies declaradas, todavía no está listo para deploy.", "ok")


def final_project_page():
    section_title("Proyecto final y checklist")
    lesson_header("Inicio › Proyecto final › Entrega pro", 20)
    project = pd.DataFrame(
        {
            "Módulo": ["Roadmap", "Torque & Drag", "BHA", "Validación", "UI", "Prompts", "Deploy"],
            "Qué debe incluir": [
                "Comparación pozo activo vs offsets",
                "Selector FF + corredor sombreado",
                "Tabla + resumen técnico",
                "Chequeo de columnas y unidades",
                "Interfaz entendible para operaciones",
                "Prompts reutilizables en Cursor",
                "Repo + requirements + app desplegable",
            ],
        }
    )
    st.dataframe(project, use_container_width=True)

    checks = [
        "¿Las columnas críticas se validan antes de calcular?",
        "¿Las unidades se convierten correctamente?",
        "¿Los gráficos cuentan la historia correcta?",
        "¿El usuario ve qué se cargó?",
        "¿La arquitectura permite crecer sin duplicación?",
        "¿Los prompts quedaron guardados y reutilizables?",
        "¿Existe requirements.txt?",
        "¿El repo está listo para deploy?",
    ]
    done = 0
    for i, item in enumerate(checks):
        if st.checkbox(item, key=f"project_chk_{i}"):
            done += 1
    st.metric("Checklist completado", f"{done}/{len(checks)}")
    st.progress(done / len(checks))

    box("<b>Resultado esperado:</b> una app técnica que se pueda enseñar, iterar, mantener y compartir, no solo una demo aislada.", "ok")



def geology_star_steering_page():
    section_title("Enfoque para geólogos · star steering")
    lesson_header("Inicio › Geología › Star steering aplicado", 22)
    objective_box(
        "Objetivos",
        [
            "Conectar vibe coding con necesidades típicas de geología operacional y geosteering.",
            "Pensar una app para star steering como producto técnico: datos, lógica, visualización y validación.",
            "Traducir problemas geológicos a prompts concretos para Cursor.",
        ],
    )
    objective_box(
        "Qué debe saber el alumno",
        [
            "Lectura básica de trayectorias, TVD/MD e interpretación operacional.",
            "Concepto general de ventanas objetivo, tops y distancia a target.",
        ],
        kind="prereq",
    )
    chips("Geología operacional", "Star steering", "TVD / MD", "Target window", "Apps técnicas", "Cursor")

    st.markdown(
        """
        Además de drilling apps clásicas, este curso también puede servir a **geólogos** que trabajan con **star steering / geosteering**.
        Aquí la lógica cambia un poco: el centro del problema ya no es solo torque, WOB o ROP, sino la **posición relativa del pozo**
        respecto al objetivo geológico, la interpretación de tops, la cercanía a la ventana objetivo y la necesidad de comunicar
        decisiones de steering con claridad.

        Una app útil para geólogos puede ayudar a:
        - visualizar la **trayectoria planeada vs trayectoria actual**,
        - estimar **distancia vertical al target**,
        - mostrar **alertas** cuando el pozo sale de ventana,
        - comparar escenarios de corrección,
        - documentar decisiones y supuestos geológicos.
        """
    )
    flow("Cargar survey / picks", "Validar columnas", "Calcular posición relativa", "Evaluar target window", "Visualizar", "Soportar decisión")

    c1, c2, c3 = st.columns(3)
    with c1:
        metric_card("Uso típico", "Monitoreo geológico")
    with c2:
        metric_card("Dato crítico", "Distancia a target")
    with c3:
        metric_card("Salida clave", "Recomendación visual")

    md = np.linspace(10000, 12500, 180)
    planned_tvd = 8600 + 0.35 * (md - md.min()) + 18 * np.sin(md / 340)
    actual_tvd = planned_tvd + 14 * np.sin(md / 180) - 8 * np.cos(md / 260)
    target_center = 9025 + 0.12 * np.sin(md / 210)
    target_top = target_center + 18
    target_base = target_center - 18
    delta = actual_tvd - target_center

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=md, y=planned_tvd, mode="lines", name="Trayectoria planeada", line=dict(width=2.5, color="#38bdf8")))
    fig.add_trace(go.Scatter(x=md, y=actual_tvd, mode="lines", name="Trayectoria actual", line=dict(width=2.5, color="#f97316")))
    fig.add_trace(go.Scatter(x=md, y=target_top, mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(
        go.Scatter(
            x=md, y=target_base, mode="lines", fill="tonexty",
            name="Target window", line=dict(width=0),
            fillcolor="rgba(34,197,94,0.16)"
        )
    )
    fig.update_layout(height=540, title="Star steering — trayectoria vs ventana objetivo")
    fig.update_xaxes(title_text="MD")
    fig.update_yaxes(title_text="TVD")
    dark_layout(fig)
    st.plotly_chart(fig, use_container_width=True)

    eval_df = pd.DataFrame(
        {
            "Métrica": [
                "Distancia media al centro del target",
                "Máxima salida por arriba",
                "Máxima salida por abajo",
                "Porcentaje de puntos dentro de ventana",
            ],
            "Valor": [
                f"{np.mean(np.abs(delta)):.2f}",
                f"{max(0, np.max(actual_tvd - target_top)):.2f}",
                f"{max(0, np.max(target_base - actual_tvd)):.2f}",
                f"{100*np.mean((actual_tvd <= target_top) & (actual_tvd >= target_base)):.1f}%",
            ],
        }
    )
    st.dataframe(eval_df, use_container_width=True)

    st.markdown("### ¿Qué tipo de app podría pedir un geólogo a Cursor?")
    st.code(
        textwrap.dedent(
            """
            Build a Streamlit app for geosteering / star steering that:
            - loads survey points and target window data
            - validates MD, TVD and target columns
            - plots actual vs planned trajectory
            - highlights when the well exits the target window
            - computes distance to target center
            - shows a simple decision panel for steering interpretation
            """
        ),
        language="markdown",
    )

    box(
        "<b>Idea clave:</b> para geólogos, vibe coding no es solo escribir código; es transformar interpretación geológica y reglas operativas en una app clara, trazable y útil para toma de decisión.",
        "info",
    )


def geology_exercises_page():
    section_title("Ejercicios para geólogos con vibe coding")
    lesson_header("Inicio › Geología › Ejercicios y prompts", 28)
    chips("Survey", "Target window", "Alertas", "Prompting", "Star steering", "Geosteering")

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Ejercicio 1 · Visualización básica",
            "Ejercicio 2 · Salida de ventana",
            "Ejercicio 3 · Prompt builder geológico",
            "Ejercicio 4 · Mini laboratorio",
        ]
    )

    with tab1:
        st.markdown(
            """
            **Objetivo:** construir una vista simple de trayectoria planeada, trayectoria actual y ventana objetivo.
            **Criterio de éxito:** el usuario puede ver claramente si el pozo va centrado, alto o bajo respecto al target.
            """
        )
        st.code(
            textwrap.dedent(
                """
                import streamlit as st
                import plotly.graph_objects as go
                import pandas as pd

                df = pd.read_csv("survey.csv")
                # columnas esperadas: MD, TVD_actual, TVD_plan, TargetTop, TargetBase
                """
            ),
            language="python",
        )
        st.markdown("**Prompt sugerido para Cursor**")
        st.code(
            textwrap.dedent(
                """
                Create a Streamlit page for geologists that plots:
                - actual TVD vs MD
                - planned TVD vs MD
                - shaded target window
                Use clear labels and a professional technical layout.
                """
            ),
            language="markdown",
        )

    with tab2:
        st.markdown(
            """
            **Objetivo:** crear una regla que marque cuando el pozo sale de la ventana objetivo.
            """
        )
        dev = st.slider("Desviación respecto al centro del target", -40.0, 40.0, 12.0, 1.0, key="geo_dev")
        half_window = st.slider("Semi-espesor de ventana", 5.0, 30.0, 18.0, 1.0, key="geo_hw")
        if dev > half_window:
            st.error("Interpretación: el pozo está por arriba de la ventana objetivo.")
        elif dev < -half_window:
            st.error("Interpretación: el pozo está por debajo de la ventana objetivo.")
        else:
            st.success("Interpretación: el pozo está dentro de la ventana objetivo.")

        st.code(
            textwrap.dedent(
                """
                if actual_tvd > target_top:
                    status = "above target window"
                elif actual_tvd < target_base:
                    status = "below target window"
                else:
                    status = "inside target window"
                """
            ),
            language="python",
        )

    with tab3:
        st.markdown("### Generador de prompt para geólogos")
        goal = st.selectbox(
            "Qué quieres construir",
            [
                "Plot de trayectoria y ventana objetivo",
                "Alerta de salida de target",
                "Comparador de escenarios de steering",
                "Resumen geológico operativo",
                "Dashboard de geosteering",
            ],
            key="geo_goal",
        )
        data_ctx = st.text_area(
            "Contexto de datos",
            "Tengo survey con MD, TVD actual, trayectoria planeada y límites de target window.",
            key="geo_ctx",
        )
        success = st.text_area(
            "Criterio de éxito",
            "La app debe mostrar claramente si el pozo está dentro o fuera de la ventana y cuánto se desvía.",
            key="geo_success",
        )
        prompt = f"""Build a Streamlit app for geologists focused on {goal.lower()}.

Context:
{data_ctx}

Requirements:
- keep the UI clear and technical
- validate required columns
- explain the geological interpretation in plain language
- use Plotly for charts

Success criteria:
{success}
"""
        st.code(prompt, language="markdown")

    with tab4:
        st.markdown("### Mini laboratorio de interpretación")
        md = np.linspace(10400, 11000, 60)
        target_center = 9000 + 0.10 * np.sin(md / 120)
        actual = target_center + 10 * np.sin(md / 80)
        top = target_center + 15
        base = target_center - 15
        inside = (actual <= top) & (actual >= base)

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=md, y=top, mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
        fig2.add_trace(go.Scatter(x=md, y=base, mode="lines", fill="tonexty", name="Ventana objetivo", line=dict(width=0), fillcolor="rgba(34,197,94,0.16)"))
        fig2.add_trace(go.Scatter(x=md, y=actual, mode="lines+markers", name="Trayectoria actual", marker=dict(size=6), line=dict(width=2.2)))
        fig2.update_layout(height=440, title="Ejercicio visual — interpretar permanencia en ventana")
        fig2.update_xaxes(title_text="MD")
        fig2.update_yaxes(title_text="TVD")
        dark_layout(fig2)
        st.plotly_chart(fig2, use_container_width=True)

        pct_inside = 100 * np.mean(inside)
        st.metric("% de trayectoria dentro de la ventana", f"{pct_inside:.1f}%")

        interpretation = st.radio(
            "¿Cuál conclusión es mejor?",
            [
                "La trayectoria está perfectamente centrada todo el tiempo",
                "La trayectoria entra y sale de la ventana, por lo que conviene revisar steering",
                "La ventana objetivo no importa si la gráfica se ve bien",
            ],
            key="geo_interp",
        )
        if st.button("Revisar interpretación geológica"):
            if interpretation == "La trayectoria entra y sale de la ventana, por lo que conviene revisar steering":
                st.success("Correcto. La app debe ayudar a detectar y comunicar esas salidas de ventana.")
            else:
                st.error("No. La lectura correcta debe enfocarse en permanencia relativa al target.")

    box(
        "<b>Valor agregado:</b> estos ejercicios permiten usar vibe coding también para perfiles geológicos, no solo para drilling optimization o torque & drag.",
        "exercise",
    )



def geology_tops_markers_page():
    section_title("Tops y markers para geólogos")
    lesson_header("Inicio › Geología › Tops y markers", 22)
    chips("Formation tops", "Markers", "Interpretación", "Ventana objetivo", "Visual QC")
    st.markdown(
        """
        En geosteering, **tops** y **markers** ayudan a contextualizar la posición del pozo respecto a unidades o referencias geológicas.
        Una app útil no solo dibuja la trayectoria: también muestra **eventos geológicos relevantes** para que el usuario compare la posición actual
        con el modelo esperado y detecte si la narrativa geológica sigue siendo consistente.
        """
    )

    md = np.linspace(9800, 11800, 140)
    tvd = 8850 + 0.42 * (md - md.min()) + 10 * np.sin(md / 250)
    top_a = 9000 + 0.05 * np.sin(md / 170)
    top_b = 9080 + 0.06 * np.cos(md / 180)
    top_c = 9160 + 0.05 * np.sin(md / 210 + 1.2)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=md, y=tvd, mode="lines", name="Trayectoria actual", line=dict(width=2.6, color="#f97316")))
    fig.add_trace(go.Scatter(x=md, y=top_a, mode="lines", name="Top A", line=dict(width=2, dash="dash", color="#38bdf8")))
    fig.add_trace(go.Scatter(x=md, y=top_b, mode="lines", name="Top B", line=dict(width=2, dash="dot", color="#22c55e")))
    fig.add_trace(go.Scatter(x=md, y=top_c, mode="lines", name="Marker C", line=dict(width=2, dash="dashdot", color="#a78bfa")))
    fig.update_layout(height=520, title="Trayectoria con tops y markers de referencia")
    fig.update_xaxes(title_text="MD")
    fig.update_yaxes(title_text="TVD")
    dark_layout(fig)
    st.plotly_chart(fig, use_container_width=True)

    tops_df = pd.DataFrame(
        {
            "Referencia": ["Top A", "Top B", "Marker C"],
            "Uso típico": [
                "Límite superior de unidad",
                "Referencia intermedia para correlación",
                "Marker operativo para control fino",
            ],
            "Lectura práctica": [
                "Comparar si el pozo entra antes o después de lo esperado",
                "Medir separación relativa entre trayectoria y referencia",
                "Apoyar decisiones de steering y narrativa geológica",
            ],
        }
    )
    st.dataframe(tops_df, use_container_width=True)

    st.code(
        textwrap.dedent(
            """
            Build a Streamlit page for geologists that:
            - plots actual trajectory
            - overlays formation tops and markers
            - allows toggling each marker on/off
            - explains the geological meaning in plain language
            """
        ),
        language="markdown",
    )
    box("<b>Valor:</b> tops y markers convierten la gráfica en una herramienta de interpretación, no solo de dibujo.", "info")


def geology_correlation_page():
    section_title("Correlación simple entre pozos")
    lesson_header("Inicio › Geología › Correlación entre pozos", 24)
    chips("Well A", "Well B", "Shift", "Markers", "Interpretación lateral")
    st.markdown(
        """
        Una correlación simple entre pozos ayuda a comparar cómo cambian tops o markers entre un pozo de referencia y otro.
        En una app de entrenamiento, esto puede enseñarse con curvas sintéticas y desplazamientos simples antes de pasar a datos reales.
        """
    )

    tvd = np.linspace(8800, 9300, 180)
    gamma_a = 80 + 18 * np.sin(tvd / 22) + 7 * np.cos(tvd / 9)
    shift = st.slider("Desplazamiento del pozo B", -35.0, 35.0, 12.0, 1.0)
    gamma_b = 78 + 17 * np.sin((tvd - shift) / 22) + 6 * np.cos((tvd - shift) / 9)

    fig = make_subplots(rows=1, cols=2, shared_yaxes=True, subplot_titles=("Pozo A", "Pozo B"))
    fig.add_trace(go.Scatter(x=gamma_a, y=tvd, mode="lines", name="Well A", line=dict(width=2.5, color="#38bdf8")), row=1, col=1)
    fig.add_trace(go.Scatter(x=gamma_b, y=tvd, mode="lines", name="Well B", line=dict(width=2.5, color="#f97316")), row=1, col=2)
    fig.update_yaxes(autorange="reversed", title_text="TVD")
    fig.update_xaxes(title_text="Gamma")
    fig.update_layout(height=560, title="Correlación visual simple entre dos pozos")
    dark_layout(fig)
    st.plotly_chart(fig, use_container_width=True)

    similarity = max(0.0, 100 - abs(shift) * 2.2)
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Shift aplicado", f"{shift:.1f}")
    with c2:
        st.metric("Similitud visual estimada", f"{similarity:.1f}%")

    st.code(
        textwrap.dedent(
            """
            Create a teaching app for geologists that:
            - compares two synthetic wells side by side
            - allows shifting one well relative to the other
            - helps visualize simple marker correlation
            - keeps the explanation intuitive for training
            """
        ),
        language="markdown",
    )
    box("<b>Idea didáctica:</b> primero se aprende la lógica de alineación y correlación con ejemplos simples; luego se lleva a datos reales.", "ok")


def geology_alerts_page():
    section_title("Alertas above / below target")
    lesson_header("Inicio › Geología › Alertas operativas", 18)
    chips("Inside", "Above", "Below", "Thresholds", "Alerting")
    st.markdown(
        """
        Un patrón muy útil en geosteering es traducir la posición relativa del pozo a estados simples:
        **inside target**, **above target** o **below target**.
        Eso permite construir alertas visuales, paneles de estado y reglas operativas fáciles de entender.
        """
    )

    dev = st.slider("Desviación actual respecto al centro del target", -40.0, 40.0, 8.0, 1.0)
    half_window = st.slider("Semi-espesor de target window", 5.0, 25.0, 15.0, 1.0)

    if dev > half_window:
        status = "above target"
        st.error("Estado: ABOVE TARGET")
    elif dev < -half_window:
        status = "below target"
        st.error("Estado: BELOW TARGET")
    else:
        status = "inside target"
        st.success("Estado: INSIDE TARGET")

    gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=dev,
        title={"text": "Desviación relativa al centro del target"},
        gauge={
            "axis": {"range": [-40, 40]},
            "steps": [
                {"range": [-40, -half_window], "color": "rgba(244,114,182,0.35)"},
                {"range": [-half_window, half_window], "color": "rgba(34,197,94,0.28)"},
                {"range": [half_window, 40], "color": "rgba(248,113,113,0.35)"},
            ],
        },
    ))
    gauge.update_layout(height=350, paper_bgcolor=DARK_BG, font=dict(color=TEXT))
    st.plotly_chart(gauge, use_container_width=True)

    st.code(
        textwrap.dedent(
            """
            if actual_tvd > target_top:
                status = "above target"
            elif actual_tvd < target_base:
                status = "below target"
            else:
                status = "inside target"
            """
        ),
        language="python",
    )
    st.caption(f"Estado calculado en este ejemplo: {status}")
    box("<b>Aplicación:</b> estas alertas simplifican la comunicación entre interpretación geológica y acción operacional.", "warn")


def geology_steering_simulator_page():
    section_title("Steering training simulator")
    lesson_header("Inicio › Geología › Simulador de recomendación", 30)
    chips("Simulator", "Decision support", "Above / below", "Correction", "Training")
    st.markdown(
        """
        Este simulador no reemplaza criterio experto. Sirve como módulo de **entrenamiento** para practicar cómo una app puede sugerir
        una recomendación básica de steering a partir de una posición relativa al target.
        """
    )

    md = np.linspace(10500, 11200, 80)
    target_center = 9020 + 0.08 * np.sin(md / 120)
    top = target_center + 14
    base = target_center - 14

    offset = st.slider("Offset actual respecto al centro del target", -30.0, 30.0, 11.0, 1.0)
    aggressiveness = st.slider("Nivel de corrección sugerida", 1, 5, 3, 1)
    actual = target_center + offset + 5 * np.sin(md / 75)

    mean_dev = float(np.mean(actual - target_center))

    if mean_dev > 14:
        recommendation = f"Recommend steering downward with correction level {aggressiveness}/5."
        label = "above target"
    elif mean_dev < -14:
        recommendation = f"Recommend steering upward with correction level {aggressiveness}/5."
        label = "below target"
    else:
        recommendation = "Hold course with close monitoring; trajectory remains inside target window."
        label = "inside target"

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=md, y=top, mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=md, y=base, mode="lines", fill="tonexty", name="Target window", line=dict(width=0), fillcolor="rgba(34,197,94,0.16)"))
    fig.add_trace(go.Scatter(x=md, y=target_center, mode="lines", name="Target center", line=dict(width=2, dash="dot", color="#22c55e")))
    fig.add_trace(go.Scatter(x=md, y=actual, mode="lines", name="Current trajectory", line=dict(width=2.6, color="#f97316")))
    fig.update_layout(height=500, title="Simulador básico de steering")
    fig.update_xaxes(title_text="MD")
    fig.update_yaxes(title_text="TVD")
    dark_layout(fig)
    st.plotly_chart(fig, use_container_width=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Mean deviation", f"{mean_dev:.2f}")
    with c2:
        st.metric("State", label)
    with c3:
        st.metric("Correction level", f"{aggressiveness}/5")

    st.markdown("### Recomendación didáctica")
    st.code(recommendation, language="text")

    st.markdown("### Prompt para construir un simulador más avanzado")
    st.code(
        textwrap.dedent(
            """
            Build a Streamlit training simulator for geologists that:
            - simulates actual trajectory vs target window
            - classifies the state as inside / above / below target
            - provides a simple steering recommendation
            - exposes sliders to practice different scenarios
            - clearly states that the tool is educational and not an autonomous decision system
            """
        ),
        language="markdown",
    )
    box("<b>Valor pedagógico:</b> el alumno practica cómo traducir interpretación geológica en lógica de producto y UI interactiva.", "exercise")



def ai_vs_ml_page():
    section_title("AI vs Machine Learning")
    lesson_header("Inicio › Fundamentos › AI vs ML", 18)
    chips("AI", "Machine Learning", "Subconjuntos", "Datos", "Modelos", "Automatización")

    st.markdown(
        """
        **Artificial Intelligence (AI)** es el paraguas amplio: sistemas diseñados para realizar tareas que normalmente asociamos con razonamiento,
        percepción, lenguaje, planificación o toma de decisiones. **Machine Learning (ML)** es una subárea dentro de AI donde el sistema aprende
        patrones desde datos en vez de seguir únicamente reglas codificadas a mano.

        Dicho de forma simple:
        - **AI** = campo general.
        - **ML** = una manera específica de construir sistemas de AI usando datos.
        """
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        metric_card("AI", "Campo general")
    with c2:
        metric_card("ML", "Subárea de AI")
    with c3:
        metric_card("Idea clave", "AI ⊃ ML")

    st.markdown("### Comparación rápida")
    comp = pd.DataFrame(
        {
            "Tema": ["AI", "Machine Learning"],
            "Qué es": [
                "Campo amplio para construir sistemas que realizan tareas inteligentes",
                "Subcampo de AI que aprende patrones desde datos",
            ],
            "Cómo funciona": [
                "Puede usar reglas, búsqueda, lógica, modelos probabilísticos o ML",
                "Usa ejemplos/datos para ajustar un modelo",
            ],
            "Ejemplo útil en este curso": [
                "Un asistente que te ayuda a diseñar una app y explicar opciones",
                "Un modelo que estima ROP o clasifica riesgo de dysfunction",
            ],
        }
    )
    st.dataframe(comp, use_container_width=True)

    fig = go.Figure(
        go.Sunburst(
            labels=["AI", "Machine Learning", "Reglas", "Búsqueda", "Deep Learning", "Supervisado", "No supervisado"],
            parents=["", "AI", "AI", "AI", "Machine Learning", "Machine Learning", "Machine Learning"],
            values=[20, 10, 4, 3, 5, 3, 2],
        )
    )
    fig.update_layout(height=620, title="Relación conceptual: AI contiene a Machine Learning")
    st.plotly_chart(fig, use_container_width=True)

    box(
        "<b>Aplicación al curso:</b> cuando usas Cursor o Claude Code estás usando herramientas de AI. Cuando una app aprende desde históricos de datos para predecir o clasificar, eso ya entra en Machine Learning.",
        "info",
    )


def coding_assistants_overview_page():
    section_title("Cursor, Claude Code y otros asistentes")
    lesson_header("Inicio › Assistants › Panorama general", 22)
    chips("Cursor", "Claude Code", "GitHub Copilot", "Editor", "CLI", "Flujos de trabajo")

    link_box(
        "Documentación oficial",
        [
            ("Cursor · introducción", CURSOR_INTRODUCTION),
            ("Cursor · comenzar", CURSOR_GETTING_STARTED),
            ("Cursor · modelos", CURSOR_MODELS),
            ("Claude Code overview", CLAUDE_CODE_OVERVIEW),
            ("GitHub Copilot get started", GITHUB_COPILOT_GETTING_STARTED),
        ],
    )

    st.markdown(
        """
        Hoy existen varias herramientas de AI para desarrollo, pero no todas trabajan igual. Algunas viven **dentro de un editor**,
        otras en **terminal**, y otras se centran más en **autocompletar** que en ejecutar flujos largos sobre un proyecto.
        """
    )

    compare = pd.DataFrame(
        {
            "Herramienta": ["Cursor", "Claude Code", "GitHub Copilot"],
            "Forma principal": ["Editor de código con AI integrada", "Herramienta agentic en terminal", "Asistente de coding integrado en GitHub / IDEs"],
            "Uso típico": [
                "Editar archivos, chatear con el codebase, generar y refactorizar dentro del editor",
                "Trabajar desde terminal sobre un repo, pedir features, debugging y navegación del proyecto",
                "Autocompletar, sugerencias y asistencia general en el flujo de desarrollo",
            ],
            "Cuándo destaca": [
                "Cuando quieres experiencia tipo editor + contexto del proyecto",
                "Cuando te acomoda una experiencia más orientada a terminal y automatización",
                "Cuando ya trabajas fuerte en el ecosistema GitHub y quieres soporte en IDE",
            ],
        }
    )
    st.dataframe(compare, use_container_width=True)

    box(
        "<b>Diferencia práctica:</b> Cursor se siente como un editor AI-first, Claude Code como un agente de coding en terminal, y Copilot como un asistente muy integrado al flujo de desarrollo en GitHub/IDE.",
        "ok",
    )


def cursor_claude_howto_page():
    section_title("Cómo usar Cursor y Claude Code")
    lesson_header("Inicio › Assistants › Uso práctico", 28)
    tabs = st.tabs(["Cursor", "Claude Code", "Cuándo usar cuál", "Prompts de ejemplo"])

    with tabs[0]:
        link_box(
            "Cursor oficial",
            [
                ("Introducción", CURSOR_INTRODUCTION),
                ("Instalación", CURSOR_INSTALL),
                ("Comenzar (primeros pasos)", CURSOR_GETTING_STARTED),
                ("Modelos", CURSOR_MODELS),
                ("Precios y planes", CURSOR_ACCOUNT_PRICING),
                ("Registro de cambios", CURSOR_CHANGELOG),
            ],
        )
        st.markdown(
            """
            **Cursor** es un editor con IA y agente de programación: entiende el codebase, ayuda a planificar y desarrollar
            funcionalidades, depurar, revisar diffs y enlazar con tu flujo (GitHub, Slack, etc.). La documentación oficial
            cubre instalación, modelos, precios y novedades.

            Flujo recomendado para usarlo bien:
            1. Abrir el proyecto o repo.
            2. Dar contexto claro del problema.
            3. Pedir un cambio específico.
            4. Revisar el diff o los archivos propuestos.
            5. Validar localmente.
            6. Iterar con instrucciones más finas.
            """
        )
        st.code(
            textwrap.dedent(
                """
                Build a Streamlit module that:
                - loads a roadmap export
                - validates required columns
                - normalizes units
                - plots a horizontal comparison panel
                - returns a delta summary table
                Keep the code modular and readable.
                """
            ),
            language="markdown",
        )

    with tabs[1]:
        link_box(
            "Claude Code oficial",
            [
                ("Claude Code overview", CLAUDE_CODE_OVERVIEW),
            ],
        )
        st.markdown(
            """
            **Claude Code** se describe oficialmente como una herramienta de coding agentic que vive en tu terminal.
            La documentación indica como requisitos **Node.js 18+** y una cuenta Claude.ai o Anthropic Console, y el arranque rápido usa `npm install -g @anthropic-ai/claude-code` seguido de ejecutar `claude` dentro del proyecto.

            Flujo recomendado:
            1. Entrar al repo desde terminal.
            2. Lanzar Claude Code.
            3. Pedir una tarea concreta sobre el proyecto.
            4. Revisar el plan y los cambios.
            5. Validar archivos y resultados.
            """
        )
        st.code(
            textwrap.dedent(
                """
                npm install -g @anthropic-ai/claude-code
                cd your-project
                claude
                """
            ),
            language="bash",
        )
        st.code(
            textwrap.dedent(
                """
                Analyze this Streamlit app and refactor the data loading path into:
                - loaders.py
                - validators.py
                - units.py
                Preserve behavior and explain the changes.
                """
            ),
            language="markdown",
        )

    with tabs[2]:
        st.markdown(
            """
            **Usa Cursor cuando:**
            - quieras trabajar visualmente dentro del editor,
            - necesites editar varios archivos con contexto de IDE,
            - quieras una experiencia cercana a VS Code con AI integrada.

            **Usa Claude Code cuando:**
            - prefieras trabajar desde terminal,
            - quieras una experiencia más agentic sobre un repo,
            - te sientas cómodo lanzando tareas directamente desde CLI.

            **Usa Copilot cuando:**
            - tu foco principal sea autocompletado y asistencia dentro de tu IDE,
            - ya estés muy integrado al flujo GitHub.
            """
        )

    with tabs[3]:
        prompt_type = st.selectbox(
            "Escoge un caso de uso",
            ["Crear módulo", "Depurar bug", "Refactorizar", "Explicar arquitectura"],
            key="assist_prompt_type",
        )
        if prompt_type == "Crear módulo":
            prompt = """Create a new Streamlit page for geologists that:
- loads survey and target window data
- validates required columns
- plots actual vs planned trajectory
- highlights inside / above / below target
- uses Plotly and clean modular code"""
        elif prompt_type == "Depurar bug":
            prompt = """The chart is incorrect because TVD and MD are being mixed in the plotting path.
Review the geosteering module, patch only the relevant logic, and explain the bug clearly."""
        elif prompt_type == "Refactorizar":
            prompt = """Refactor this app into:
- loaders.py
- validators.py
- plots.py
- ui.py
- prompts.py
Preserve current behavior and remove duplication."""
        else:
            prompt = """Explain this Streamlit app architecture in plain language:
- what each module does
- where validation should live
- where visualization should live
- what should stay in session_state and what should not"""
        st.code(prompt, language="markdown")

    box(
        "<b>Consejo transversal:</b> tanto en Cursor como en Claude Code conviene pedir cambios concretos, incluir restricciones, y validar siempre el resultado en vez de aceptar todo automáticamente.",
        "warn",
    )


def angela_gatito_galileo_page():
    section_title("Ejercicio para Angela · Gatito Galileo (vibe coding)")
    lesson_header("Inicio › Vibe coding divertido › Angela · Gatito Galileo", 22)
    objective_box(
        "Qué se practica (en serio, pero con humor)",
        [
            "Nombrar variables claras y separar *datos* (parámetros del gato y del cielo) de la *figura* (Plotly).",
            "Pedir a la IA cambios concretos: qué mover, qué no romper, cómo validar (¿se ve la luna?).",
            "Entender que “generar una imagen” en código = construir geometría + estilos, no magia.",
        ],
    )
    chips("Variables", "Funciones", "Plotly", "Prompts", "Angela", "Galileo")

    st.markdown(
        """
        **Para Angela.** Galileo miró las lunas de Júpiter; **Gatito Galileo** mira la Luna… y las variables.
        Este módulo es un mini laboratorio *ligero*: mismas ideas que en apps técnicas (parámetros → función → visual),
        pero con bigotes y órbitas. Si te ríes un poco y aprendes a enunciar prompts, cumplió su misión.
        """
    )

    st.markdown("##### Parámetros (alimentan la figura en todas las pestañas)")
    c1, c2 = st.columns(2)
    with c1:
        nombre_gato = st.text_input("Nombre del gato astrónomo", value="Galileo", key="angela_nombre")
        nivel_curiosidad = st.slider("Nivel de curiosidad (escala de orejas)", 0.3, 1.8, 1.0, 0.05, key="angela_cur")
        fase_lunar = st.slider("Fase lunar iluminada (0 = nueva, 1 = llena)", 0.0, 1.0, 0.65, 0.01, key="angela_luna")
    with c2:
        longitud_bigotes = st.slider("Longitud de bigotes (px lógicos)", 0.4, 2.2, 1.0, 0.05, key="angela_big")
        ang_orbita = st.slider("Ángulo de las lunas galileanas (°)", 0, 360, 42, 2, key="angela_orb")

    tab_viz, tab_code, tab_prompt = st.tabs(["1 · Escena interactiva", "2 · Código didáctico", "3 · Prompts para Cursor"])

    with tab_viz:
        fig = build_gatito_galileo_figure(
            nombre_gato=nombre_gato,
            orejas=nivel_curiosidad,
            fase=fase_lunar,
            bigotes=longitud_bigotes,
            ang_rad=np.deg2rad(ang_orbita),
        )
        dark_layout(fig)
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            "La “imagen” es un gráfico vectorial (Plotly). Para **exportar PNG** en local: "
            "`pip install kaleido` y luego `fig.write_image('gatito_galileo.png')`."
        )

    with tab_code:
        st.markdown("### Variables que alimentan la escena")
        st.code(
            textwrap.dedent(
                f"""
                nombre_gato = "{nombre_gato}"          # str: identidad del personaje
                nivel_curiosidad = {nivel_curiosidad:.2f}   # float: escala orejas / actitud
                fase_lunar = {fase_lunar:.2f}          # float en [0, 1]: fracción iluminada
                longitud_bigotes = {longitud_bigotes:.2f}   # float: longitud relativa
                ang_orbita_deg = {ang_orbita}          # int: posición de las lunas de Júpiter

                # Idea vibe coding: una función pura construye la figura desde parámetros.
                fig = build_gatito_galileo_figure(...)
                """
            ),
            language="python",
        )
        st.markdown(
            """
            **Por qué sirve:** cuando pidas ayuda a Cursor, nombra estas variables y qué deben controlar (orejas vs órbita).
            Así el modelo no mezcla “luna llena” con “orejas de conejo” sin querer.
            """
        )

    with tab_prompt:
        st.markdown("### Copia, pega y ajusta en Cursor")
        p1 = textwrap.dedent(
            """
            Add a small Python module `galileo_cat.py` with a pure function `build_gatito_galileo_figure(...)` that returns a Plotly figure.
            Parameters: cat name (for title only), ear_scale, moon_phase_0_1, whisker_scale, moon_orbit_angle_rad.
            Draw: (1) a simple cat face using filled polygons/lines, (2) a moon with illuminated fraction approximated by overlaying circles,
            (3) Jupiter as a large marker and 4 smaller “Galilean moon” markers on a ring. No external images; only Plotly.
            Include a short docstring explaining the educational purpose (vibe coding exercise for Angela).
            """
        )
        p2 = textwrap.dedent(
            """
            Refactor the Streamlit page so sliders live in `sidebar` and the figure updates with `@st.cache_data` on a hash of parameters.
            Keep behavior identical. Explain what you cached and why.
            """
        )
        st.code(p1, language="markdown")
        st.code(p2, language="markdown")

    box(
        "<b>Regla de oro:</b> si el prompt suena a cuento (“haz un gato bonito”), la IA improvisa; si suena a contrato "
        "(parámetros, figura Plotly, validación visual), improvisa menos.",
        "exercise",
    )


def build_gatito_galileo_figure(
    *,
    nombre_gato: str,
    orejas: float,
    fase: float,
    bigotes: float,
    ang_rad: float,
) -> go.Figure:
    """Escena 2D minimalista: gatito, Luna con fase, Júpiter + 4 lunas (solo Plotly)."""
    fig = go.Figure()
    fase = float(np.clip(fase, 0.0, 1.0))

    # --- Cara: contorno ---
    th = np.linspace(0, 2 * np.pi, 72)
    r = 1.0
    hx = r * np.cos(th)
    hy = r * np.sin(th)
    fig.add_trace(
        go.Scatter(
            x=hx, y=hy, mode="lines", fill="toself", fillcolor="rgba(148,163,184,0.35)",
            line=dict(color="#94a3b8", width=2), name="Cara", hoverinfo="skip",
        )
    )

    # Orejas (triángulos)
    ear_l = np.column_stack([[-0.55, -0.15, 0.0], [0.75, 1.15 * orejas, 0.55]])
    ear_r = np.column_stack([[0.55, 0.15, 0.0], [0.75, 1.15 * orejas, 0.55]])
    for ex, ey, nm in [
        (ear_l[:, 0], ear_l[:, 1], "Oreja L"),
        (ear_r[:, 0], ear_r[:, 1], "Oreja R"),
    ]:
        fig.add_trace(
            go.Scatter(
                x=np.append(ex, ex[0]), y=np.append(ey, ey[0]), mode="lines", fill="toself",
                fillcolor="rgba(251,146,60,0.55)", line=dict(color="#fb923c", width=1), name=nm, hoverinfo="skip",
            )
        )

    # Ojos
    fig.add_trace(go.Scatter(x=[-0.38, 0.38], y=[0.15, 0.15], mode="markers", marker=dict(size=14, color="#0f172a"), name="Ojos", hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=[-0.38, 0.38], y=[0.15, 0.15], mode="markers", marker=dict(size=5, color="#f8fafc"), name="Brillo", hoverinfo="skip"))

    # Bigotes
    b = bigotes * 0.85
    for y0, sgn in [(0.05, -1), (-0.12, -1), (0.05, 1), (-0.12, 1)]:
        fig.add_trace(
            go.Scatter(
                x=[sgn * 0.95, sgn * (0.95 + b)], y=[y0, y0 + 0.04 * sgn],
                mode="lines", line=dict(color="#e2e8f0", width=2), name="Bigotes", showlegend=False, hoverinfo="skip",
            )
        )

    # Luna (fase): disco + máscara aproximada con segundo disco desplazado
    mx, my = 2.35, 1.1
    rm = 0.45
    moon_t = np.linspace(0, 2 * np.pi, 64)
    fig.add_trace(
        go.Scatter(
            x=mx + rm * np.cos(moon_t), y=my + rm * np.sin(moon_t), mode="lines", fill="toself",
            fillcolor="rgba(226,232,240,0.95)", line=dict(color="#cbd5e1", width=1), name="Luna", hoverinfo="skip",
        )
    )
    off = (1.0 - fase) * 2.0 * rm
    fig.add_trace(
        go.Scatter(
            x=mx + off + rm * np.cos(moon_t), y=my + rm * np.sin(moon_t), mode="lines", fill="toself",
            fillcolor=DARK_BG, line=dict(width=0), name="Sombra luna", hoverinfo="skip", showlegend=False,
        )
    )

    # Júpiter + 4 lunas galileanas
    jx, jy = -2.2, -0.3
    fig.add_trace(
        go.Scatter(
            x=[jx], y=[jy], mode="markers", name="Júpiter",
            marker=dict(size=46, color="#d97706", line=dict(color="#fdba74", width=2)),
            hoverinfo="skip",
        )
    )
    radii = [0.65, 0.82, 1.05, 1.28]
    cols = ["#38bdf8", "#f472b6", "#a78bfa", "#4ade80"]
    for i, (rad, col) in enumerate(zip(radii, cols)):
        a = ang_rad + i * 0.65
        fig.add_trace(
            go.Scatter(
                x=[jx + rad * np.cos(a)], y=[jy + rad * np.sin(a)], mode="markers",
                name=f"Luna {i+1}",
                marker=dict(size=9, color=col, line=dict(color="#f8fafc", width=1)),
                hoverinfo="skip",
            )
        )

    fig.update_layout(
        title=dict(text=f"{html.escape(nombre_gato)} · observador felino · fase lunar ≈ {fase:.0%}", font=dict(size=16)),
        xaxis=dict(visible=False, range=[-3.6, 3.6], scaleanchor="y", scaleratio=1),
        yaxis=dict(visible=False, range=[-2.2, 2.2]),
        height=520,
        margin=dict(l=20, r=20, t=60, b=20),
        showlegend=False,
    )
    return fig


PAGES = {
    "1. Bienvenida y mapa pro": intro_page,
    "2. Teoría de programación": programming_theory_page,
    "3. Pensamiento computacional": computational_thinking_page,
    "4. Python desde cero": python_basics_page,
    "5. Frameworks, librerías y stack": frameworks_page,
    "6. Arquitectura de software para apps Rogii": architecture_page,
    "7. Datos, unidades y validación": data_validation_page,
    "8. Cursor: teoría de uso": cursor_theory_page,
    "9. Prompt engineering para coding": prompt_engineering_page,
    "10. Entorno local: instalar Python y pip": install_python_page,
    "11. Librerías principales con pip": pip_libraries_page,
    "12. Caso Rogii: Roadmap": roadmap_case_page,
    "13. Caso Rogii: Torque & Drag": torque_drag_case_page,
    "14. Caso Rogii: BHA parser": bha_case_page,
    "15. Enfoque para geólogos: star steering": geology_star_steering_page,
    "16. Ejercicios para geólogos con vibe coding": geology_exercises_page,
    "17. Tops y markers para geólogos": geology_tops_markers_page,
    "18. Correlación simple entre pozos": geology_correlation_page,
    "19. Alertas above / below target": geology_alerts_page,
    "20. Steering training simulator": geology_steering_simulator_page,
    "21. AI vs Machine Learning": ai_vs_ml_page,
    "22. Cursor, Claude Code y otros asistentes": coding_assistants_overview_page,
    "23. Cómo usar Cursor y Claude Code": cursor_claude_howto_page,
    "24. Laboratorio de Python interactivo": python_lab_page,
    "25. Live coding studio pro": live_coding_page,
    "26. Caso Jusset Peña · Darcy + Python + prompts": darcy_jusset_pena_page,
    "27. P10, P90, box plots y estadística (vibe coding)": statistics_p10_p90_boxplots_page,
    "28. Refactorización guiada": refactor_page,
    "29. Deploy de la app paso a paso": deploy_page,
    "30. Proyecto final y checklist": final_project_page,
    "31. Ejercicio para Angela · Gatito Galileo (vibe coding)": angela_gatito_galileo_page,
}

st.sidebar.title("Curso Pro")
page = st.sidebar.radio("Selecciona un módulo", LESSONS)
st.sidebar.markdown("---")
st.sidebar.caption("Curso pro de vibe coding para apps técnicas de Rogii.")

PAGES[page]()
