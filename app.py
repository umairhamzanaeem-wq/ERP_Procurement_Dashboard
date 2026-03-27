import io
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st

CURRENT_YEAR = 2025

# ---------------------------------------------------------------------------
# Column‑name mapping helpers
# ---------------------------------------------------------------------------

TRANSACTION_COL_MAP = {
    "periode": "Period",
    "period": "Period",
    "datum": "Period",
    "date": "Period",
    "lieferant nummer": "SupplierID",
    "lieferantnummer": "SupplierID",
    "kreditoren nr": "SupplierID",
    "kreditorennr": "SupplierID",
    "supplier id": "SupplierID",
    "supplierid": "SupplierID",
    "lieferant name": "SupplierName",
    "lieferantname": "SupplierName",
    "supplier name": "SupplierName",
    "suppliername": "SupplierName",
    "kreditorenname": "SupplierName",
    "soll": "Debit",
    "debit": "Debit",
    "haben": "Credit",
    "credit": "Credit",
    "sachkonto": "GLAccount",
    "glaccount": "GLAccount",
    "gl account": "GLAccount",
    "konto": "GLAccount",
    "dim_glaccount": "GLAccount",
    "beschreibung": "Description",
    "description": "Description",
    "text": "Description",
    "buchungstext": "Description",
}


def _normalise_columns(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    """Lowercase + strip column names, then map to canonical names.

    When multiple raw columns would map to the same canonical name, only the
    first match is kept; later duplicates are dropped entirely.
    """
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    used_targets: set[str] = set()
    final_names: list[str] = []
    keep_mask: list[bool] = []
    for col in df.columns:
        target = col_map.get(col, col)
        if target in used_targets:
            keep_mask.append(False)
            final_names.append(target)
        else:
            keep_mask.append(True)
            final_names.append(target)
            used_targets.add(target)

    df.columns = final_names
    df = df.loc[:, keep_mask]
    return df


# ---------------------------------------------------------------------------
# Data cleaning
# ---------------------------------------------------------------------------

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Drop empty rows, deduplicate, strip strings."""
    df = df.dropna(how="all").drop_duplicates().reset_index(drop=True)
    for col in df.select_dtypes(include="object").columns:
        try:
            df[col] = df[col].str.strip()
        except (AttributeError, TypeError):
            pass
    return df


def _to_numeric_safe(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", ".").str.replace(" ", ""),
        errors="coerce",
    ).fillna(0.0)


def _extract_company(name_series: pd.Series) -> pd.Series:
    """Extract text after the first '-' in strings like '1001 - Company Name'."""
    return name_series.apply(
        lambda v: v.split("-", 1)[1].strip()
        if isinstance(v, str) and "-" in v
        else (v if isinstance(v, str) else "")
    )


# ---------------------------------------------------------------------------
# Structuring
# ---------------------------------------------------------------------------

def _drop_descriptor_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop leading rows that are column-type descriptors, not real data.

    The Materialkonten sheet has a first data row like
    'Ganzzahl (Int)', 'Fließkommazahl (Double)', 'VarChar' etc.
    """
    if df.empty:
        return df
    first = df.iloc[0].astype(str).str.lower()
    type_keywords = {"varchar", "int", "double", "string", "float", "text",
                     "ganzzahl (int)", "fließkommazahl (double)"}
    if first.apply(lambda v: any(kw in v for kw in type_keywords)).any():
        df = df.iloc[1:].reset_index(drop=True)
    return df


def build_suppliers(raw: pd.DataFrame) -> pd.DataFrame:
    """Build a clean suppliers table.

    The Kreditoren sheet may have no header row. We detect this by checking
    whether the pandas-inferred column names look like data values (integers,
    company names) rather than descriptive headers.
    """
    df = raw.copy()

    has_real_headers = False
    str_cols = [str(c).strip().lower() for c in df.columns]
    header_keywords = {"nr", "nummer", "name", "bezeichnung", "kreditoren",
                       "supplier", "lieferant", "id"}
    for c in str_cols:
        if c.startswith("unnamed"):
            continue
        tokens = set(c.replace("-", " ").replace("_", " ").split())
        if tokens & header_keywords:
            has_real_headers = True
            break

    if not has_real_headers:
        df = pd.read_excel(
            io.BytesIO(_xlsx_bytes_cache),
            sheet_name=_kreditoren_sheet_name_cache,
            header=None,
        )
        id_col = None
        name_col = None
        company_col = None
        for idx in range(min(df.shape[1], 10)):
            sample = df[idx].dropna()
            if sample.empty:
                continue
            is_numeric = pd.to_numeric(sample, errors="coerce").notna().all()
            if id_col is None and is_numeric and sample.astype(float).mean() > 10000:
                id_col = idx
                continue
            if company_col is None and sample.astype(str).str.contains(" - ").any():
                company_col = idx
                continue
            if name_col is None and not is_numeric and sample.dtype == object:
                name_col = idx
                continue

        if id_col is None:
            id_col = 3
        if name_col is None:
            name_col = 4
        if company_col is None:
            company_col = 2

        result = pd.DataFrame({
            "SupplierID": pd.to_numeric(df[id_col], errors="coerce"),
            "SupplierName": df[name_col].astype(str).str.strip(),
            "Company": df[company_col].apply(
                lambda v: str(v).split("-", 1)[1].strip()
                if isinstance(v, str) and "-" in str(v)
                else str(v)
            ),
        })
        result = result.dropna(subset=["SupplierID"])
        result["SupplierID"] = result["SupplierID"].astype(int)
        result["SupplierName"] = result["SupplierName"].replace("nan", "Unknown")
        return result.drop_duplicates().reset_index(drop=True)

    df.columns = [str(c).strip().lower() for c in df.columns]
    col_map = {
        "kreditoren nr": "SupplierID", "kreditoren nummer": "SupplierID",
        "kreditorennr": "SupplierID", "kreditorennummer": "SupplierID",
        "supplier id": "SupplierID", "supplierid": "SupplierID",
        "lieferant nummer": "SupplierID", "lieferantnummer": "SupplierID",
        "nr": "SupplierID", "nummer": "SupplierID",
        "name": "SupplierName", "kreditoren name": "SupplierName",
        "kreditorenname": "SupplierName", "supplier name": "SupplierName",
        "suppliername": "SupplierName", "bezeichnung": "SupplierName",
    }
    used: set[str] = set()
    new_names, mask = [], []
    for c in df.columns:
        t = col_map.get(c, c)
        if t in used:
            new_names.append(t)
            mask.append(False)
        else:
            new_names.append(t)
            mask.append(True)
            used.add(t)
    df.columns = new_names
    df = df.loc[:, mask]
    df = clean_dataframe(df)

    if "SupplierID" not in df.columns:
        df = df.rename(columns={df.columns[0]: "SupplierID"})
    if "SupplierName" not in df.columns:
        for c in df.columns:
            if c != "SupplierID":
                df = df.rename(columns={c: "SupplierName"})
                break
    if "SupplierName" not in df.columns:
        df["SupplierName"] = "Unknown"

    df["Company"] = _extract_company(df["SupplierName"])
    df["SupplierID"] = pd.to_numeric(df["SupplierID"], errors="coerce")
    df = df.dropna(subset=["SupplierID"])
    df["SupplierID"] = df["SupplierID"].astype(int)
    return df[["SupplierID", "SupplierName", "Company"]].reset_index(drop=True)


def build_transactions(raw: pd.DataFrame) -> pd.DataFrame:
    df = _normalise_columns(raw.copy(), TRANSACTION_COL_MAP)
    df = _drop_descriptor_rows(df)
    df = clean_dataframe(df)

    if "Period" in df.columns:
        period = df["Period"]
        numeric_period = pd.to_numeric(period, errors="coerce")
        if numeric_period.notna().sum() > len(period) * 0.5:
            df["Date"] = numeric_period.apply(
                lambda m: pd.Timestamp(year=CURRENT_YEAR, month=int(m), day=1)
                if pd.notna(m) and 1 <= m <= 12 else pd.NaT
            )
        else:
            df["Date"] = pd.to_datetime(period, dayfirst=True, errors="coerce")
    elif "Date" not in df.columns:
        df["Date"] = pd.NaT

    for col in ("Debit", "Credit"):
        if col in df.columns:
            df[col] = _to_numeric_safe(df[col])
        else:
            df[col] = 0.0

    df["Amount"] = df["Debit"] - df["Credit"]

    if "SupplierID" in df.columns:
        df["SupplierID"] = pd.to_numeric(df["SupplierID"], errors="coerce")

    if "SupplierName" not in df.columns:
        df["SupplierName"] = "Unknown"
    df["SupplierName"] = df["SupplierName"].fillna("Unknown")

    if "GLAccount" not in df.columns:
        df["GLAccount"] = ""
    if "Description" not in df.columns:
        df["Description"] = ""

    keep = ["Date", "SupplierID", "SupplierName", "Debit", "Credit", "Amount",
            "GLAccount", "Description"]
    return df[[c for c in keep if c in df.columns]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Merging
# ---------------------------------------------------------------------------

def merge_data(transactions: pd.DataFrame, suppliers: pd.DataFrame) -> pd.DataFrame:
    if "SupplierID" in transactions.columns and "SupplierID" in suppliers.columns and not suppliers.empty:
        merged = transactions.merge(
            suppliers[["SupplierID", "Company"]].drop_duplicates(),
            on="SupplierID",
            how="left",
        )
        merged["Company"] = merged["Company"].fillna("Unknown")
    else:
        merged = transactions.copy()
        merged["Company"] = "Unknown"

    merged["SupplierName"] = merged["SupplierName"].fillna("Unknown")
    return merged


# ---------------------------------------------------------------------------
# Excel export helper
# ---------------------------------------------------------------------------

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="CleanedData")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Module-level caches used by build_suppliers when re-reading headerless sheets
# ---------------------------------------------------------------------------
_xlsx_bytes_cache: bytes = b""
_kreditoren_sheet_name_cache: str = ""


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

def _read_sheets(uploaded) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Read Kreditoren and Materialkonten sheets (case-insensitive match)."""
    global _xlsx_bytes_cache, _kreditoren_sheet_name_cache

    raw_bytes = uploaded.getvalue()
    _xlsx_bytes_cache = raw_bytes

    xls = pd.ExcelFile(io.BytesIO(raw_bytes))
    sheet_map = {s.strip().lower(): s for s in xls.sheet_names}

    suppliers_raw = None
    transactions_raw = None

    for key, original in sheet_map.items():
        if "kreditor" in key:
            _kreditoren_sheet_name_cache = original
            suppliers_raw = pd.read_excel(xls, sheet_name=original)
            break

    for key, original in sheet_map.items():
        if "material" in key:
            transactions_raw = pd.read_excel(xls, sheet_name=original)
            break

    if suppliers_raw is None and len(xls.sheet_names) >= 1:
        _kreditoren_sheet_name_cache = xls.sheet_names[0]
        suppliers_raw = pd.read_excel(xls, sheet_name=xls.sheet_names[0])
    if transactions_raw is None and len(xls.sheet_names) >= 2:
        transactions_raw = pd.read_excel(xls, sheet_name=xls.sheet_names[1])

    return suppliers_raw, transactions_raw


def main() -> None:
    st.set_page_config(page_title="ERP Procurement Dashboard", layout="wide")
    st.title("ERP Procurement Dashboard")

    # ── Sidebar ──────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Upload & Filters")
        uploaded = st.file_uploader("Upload Excel file (.xlsx)", type=["xlsx"])

    if uploaded is None:
        st.info("Upload an Excel file with sheets **Kreditoren** and **Materialkonten** to get started.")
        return

    # ── Load ─────────────────────────────────────────────────────────────
    suppliers_raw, transactions_raw = _read_sheets(uploaded)

    if transactions_raw is None:
        st.error("Could not find a transactions sheet. Please check the uploaded file.")
        return

    # ── Structure & Clean ────────────────────────────────────────────────
    suppliers = build_suppliers(suppliers_raw) if suppliers_raw is not None else pd.DataFrame(
        columns=["SupplierID", "SupplierName", "Company"]
    )
    transactions = build_transactions(transactions_raw)
    merged = merge_data(transactions, suppliers)

    total_raw = len(transactions_raw) + (len(suppliers_raw) if suppliers_raw is not None else 0)
    total_clean = len(merged)

    # ── Raw vs Cleaned preview ───────────────────────────────────────────
    with st.expander("Raw vs Cleaned Data Preview", expanded=False):
        tab_txn, tab_sup = st.tabs(["Transactions", "Suppliers"])
        with tab_txn:
            col_raw, col_clean = st.columns(2)
            with col_raw:
                st.caption("Raw")
                st.dataframe(transactions_raw.head(20), use_container_width=True)
            with col_clean:
                st.caption("Cleaned")
                st.dataframe(transactions.head(20), use_container_width=True)
        with tab_sup:
            st.caption("Suppliers (cleaned)")
            st.dataframe(suppliers.head(20), use_container_width=True)

    # ── Sidebar filters ─────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("---")

        supplier_options = sorted(merged["SupplierName"].dropna().unique().tolist())
        selected_suppliers = st.multiselect("Supplier", options=supplier_options, default=[])

        company_options = sorted(merged["Company"].dropna().unique().tolist()) if "Company" in merged.columns else []
        selected_companies = st.multiselect("Company", options=company_options, default=[])

        if "Date" in merged.columns and merged["Date"].notna().any():
            merged["Month"] = merged["Date"].dt.to_period("M").astype(str)
            month_options = sorted(merged["Month"].dropna().unique().tolist())
            selected_months = st.multiselect("Month", options=month_options, default=[])
        else:
            selected_months = []

    # ── Apply filters ────────────────────────────────────────────────────
    view = merged.copy()
    if selected_suppliers:
        view = view[view["SupplierName"].isin(selected_suppliers)]
    if selected_companies and "Company" in view.columns:
        view = view[view["Company"].isin(selected_companies)]
    if selected_months and "Month" in view.columns:
        view = view[view["Month"].isin(selected_months)]

    # ── KPIs ─────────────────────────────────────────────────────────────
    total_debit = view["Debit"].sum()
    total_credit = view["Credit"].sum()
    total_net = view["Amount"].sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Debit (Spend)", f"€ {total_debit:,.2f}")
    k2.metric("Total Credit (Returns)", f"€ {total_credit:,.2f}")
    k3.metric("Net Amount", f"€ {total_net:,.2f}")
    k4.metric("Records (raw → clean)", f"{total_raw:,} → {total_clean:,}")

    st.markdown("---")

    # ── Charts ───────────────────────────────────────────────────────────
    chart_left, chart_right = st.columns(2)

    spend_view = view[view["Debit"] > 0]

    with chart_left:
        st.subheader("Spend by Supplier (Top 20)")
        spend_by_supplier = (
            spend_view.groupby("SupplierName", as_index=False)["Debit"]
            .sum()
            .sort_values("Debit", ascending=False)
            .head(20)
        )
        if not spend_by_supplier.empty:
            fig_bar = px.bar(
                spend_by_supplier,
                x="SupplierName",
                y="Debit",
                color="Debit",
                color_continuous_scale="Blues",
                labels={"Debit": "Spend (€)", "SupplierName": "Supplier"},
            )
            fig_bar.update_layout(xaxis_tickangle=-45, showlegend=False,
                                  coloraxis_showscale=False)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No data to display.")

    with chart_right:
        st.subheader("Monthly Spend")
        if "Month" in view.columns and spend_view["Month"].notna().any():
            monthly = (spend_view.groupby("Month", as_index=False)["Debit"]
                       .sum().sort_values("Month"))
            fig_line = px.line(
                monthly, x="Month", y="Debit", markers=True,
                labels={"Debit": "Spend (€)", "Month": "Month"},
            )
            fig_line.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("Date information not available for monthly chart.")

    st.markdown("---")

    # ── Data table ───────────────────────────────────────────────────────
    st.subheader("Cleaned Dataset")
    st.dataframe(view, use_container_width=True, height=400)

    # ── Download ─────────────────────────────────────────────────────────
    st.download_button(
        label="Download Cleaned Data as Excel",
        data=to_excel_bytes(view),
        file_name="cleaned_procurement_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    main()
