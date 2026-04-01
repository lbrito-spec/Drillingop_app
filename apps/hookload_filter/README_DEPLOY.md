# Deploy de una segunda app Streamlit en el mismo repo

Esta carpeta esta lista para agregar una segunda app a tu repo existente.

## Estructura recomendada

```text
your-repo/
├─ .streamlit/
│  └─ secrets.toml
├─ app.py                  # tu app actual
├─ requirements.txt        # tu archivo actual o compartido
└─ apps/
   └─ hookload_filter/
      ├─ app.py
      └─ assets/
         └─ LogoDS.png
```

## Archivos incluidos aqui

- `apps/hookload_filter/app.py`
- `apps/hookload_filter/requirements.txt`
- `.streamlit/secrets.toml.example`

## Opcion recomendada: segunda app dentro del mismo repo

Streamlit Community Cloud permite desplegar multiples apps desde un mismo repositorio.
La app puede vivir en un subdirectorio y al crear el deploy eliges ese archivo como entrypoint.
La configuracion `.streamlit/config.toml` es compartida en la raiz del repo, y los paths se resuelven desde la raiz del repositorio.
Por eso el logo se busca con rutas relativas al repo. Esto esta documentado por Streamlit. 

## Pasos en GitHub

1. Crea la carpeta `apps/hookload_filter/`
2. Sube `app.py` dentro de esa carpeta.
3. Crea `apps/hookload_filter/assets/`
4. Sube ahi `LogoDS.png`
5. Si tu repo ya usa un `requirements.txt` en la raiz, agrega:
   - `streamlit`
   - `pandas`
   - `altair`
6. Si prefieres dependencias separadas para esta app, deja `apps/hookload_filter/requirements.txt` junto al entrypoint.

## Secrets para email

No subas la password a GitHub.
En Streamlit Community Cloud abre la app > Settings > Secrets y pega algo como esto:

```toml
smtp_server = "smtp.gmail.com"
smtp_port = "587"
smtp_user = "lenin.rogii@gmail.com"
smtp_pass = "TU_APP_PASSWORD"
from_email = "lenin.rogii@gmail.com"
to_email = "solobox+pemex@rogii.com"
```

## Crear el deploy en Streamlit Community Cloud

1. Entra a tu workspace.
2. Haz clic en **Create app**.
3. Selecciona tu repo `lbrito-spec/Drillingop_app`.
4. Branch: `main`
5. Main file path: `apps/hookload_filter/app.py`
6. Deploy

## Si ya tienes otra app corriendo

No pasa nada. Puedes crear otra app nueva apuntando al mismo repo, pero con otro `Main file path`.
Cada app tendra su propia URL y sus propios Secrets.

## Nota de dependencias

Streamlit Community Cloud permite compartir dependencias o definirlas por app colocando cada entrypoint dentro de su propia carpeta con su propio archivo de dependencias.
