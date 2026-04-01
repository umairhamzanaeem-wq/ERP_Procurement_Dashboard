# ERP Procurement Dashboard

Streamlit analytics app for procurement / material cost Excel files.

## Run locally (recommended)

```bash
cd ERP_Dashboard
python3 -m pip install -r requirements.txt
python3 -m streamlit run app.py --server.port 8501
```

If you see `command not found: streamlit`, your shell does not have the `streamlit` script on `PATH`. Always use **`python3 -m streamlit`** instead of plain `streamlit` (same Python that has the packages).

Then open **http://localhost:8501** in your browser — not a Vercel URL.

## Why you see `404 DEPLOYMENT_NOT_FOUND` on Vercel

This repository is **not** set up for Vercel. Streamlit needs a long-running Python process; Vercel’s default hosting is for static sites and serverless functions. If you open an old Vercel project link, or a deployment was deleted, Vercel shows that error.

**Do not use a `*.vercel.app` link for this app** unless you intentionally deploy something else there.

## Deploy online (Streamlit Cloud)

To share the app on the web, use [Streamlit Community Cloud](https://streamlit.io/cloud):

1. Push this repo to GitHub.
2. Sign in at [share.streamlit.io](https://share.streamlit.io) with GitHub.
3. **New app** → pick the repo, main file: `app.py`, branch: `main`.
4. Deploy; Cloud gives you a `*.streamlit.app` URL.

## Requirements

See `requirements.txt` (Streamlit, pandas, plotly, **openpyxl**, **xlrd** for `.xls`, etc.). Run:

```bash
python3 -m pip install -r requirements.txt
```

If Excel **fails to open**, install/update deps, save the file as **.xlsx** (not old `.xls` unless `xlrd` is installed), and remove **password protection** from the workbook.

## Many files at once (performance)

You can upload **many** Materialkosten workbooks in one go. The first load may take **one to several minutes** (parsing + combining rows). Supplier **fuzzy matching** is automatically limited so the app stays responsive. If the browser looks idle, wait for the green “Loaded … rows” message. Prefer **≤ ~20 large files** per session; for more, split into two uploads or filter files by year first.
