# TNPI Deep3 (Streamlit)

## Run locally
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
# source .venv/bin/activate

pip install -r requirements.txt
streamlit run app.py
```

## Deploy (Streamlit Community Cloud)
1. Push this repo to GitHub.
2. In Streamlit Community Cloud, click **New app**.
3. Select your repo + branch.
4. Set **Main file path** to `app.py`.
5. Click **Deploy**.

If the app needs credentials later, add them in **App settings → Secrets** (Streamlit Cloud) as `secrets.toml` entries.
