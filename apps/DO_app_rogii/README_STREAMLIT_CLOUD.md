# Streamlit Community Cloud - listo para subir

## Archivos
- `app.py`
- `requirements.txt`
- `packages.txt`
- `.streamlit/config.toml`

## Cómo desplegar
1. Sube esta carpeta a un repositorio en GitHub.
2. En Streamlit Community Cloud, crea una nueva app.
3. Selecciona ese repositorio.
4. En **Main file path**, usa: `app.py`
5. En **Python version**, usa 3.11 si está disponible.

## Secrets / Variables de entorno
En Streamlit Cloud > App settings > Secrets, agrega lo que necesites:
- `SOLO_BASE_URL`
- `SOLO_ACCESS_TOKEN`
- `MUD_IMAP_SERVER`
- `MUD_IMAP_USER`
- `MUD_IMAP_PASS`

## Importante
- Funciones Windows-specific como `pyautogui`, capturas regionales y algunas exportaciones automáticas a PDF/PPT pueden no funcionar en Streamlit Cloud.
- La app principal sí puede correr, pero esas funciones deben dejarse para entorno local Windows.
