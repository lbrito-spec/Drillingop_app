# Vibe Coding Pro · Apps Tecnicas para Rogii

Aplicacion Streamlit lista para deploy en Streamlit Community Cloud.

## Archivos
- `app.py`: punto de entrada de la app
- `requirements.txt`: dependencias Python

## Ejecutar localmente
```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate  # Windows
pip install -r requirements.txt
streamlit run app.py
```

## Deploy en Streamlit Community Cloud
1. Sube esta carpeta a un repositorio de GitHub.
2. En Streamlit Community Cloud, crea una app nueva.
3. Selecciona el repositorio, rama y archivo de entrada: `app.py`.
4. Si te lo pide, define la version de Python en Advanced settings.
5. Deploy.

## Notas
- Esta app no usa secretos ni variables de entorno.
- Si mas adelante agregas librerias del sistema, crea `packages.txt`.
