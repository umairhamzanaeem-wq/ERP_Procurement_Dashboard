import io
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

C_SOURCE = "Source"
C_DATE = "Date"
C_PERIOD = "Period"
C_SUPPLIER_ID = "SupplierID"
C_SUPPLIER_NAME = "SupplierName"
C_DEBIT = "Debit"
C_CREDIT = "Credit"
C_AMOUNT = "Amount"
C_DESCRIPTION = "Description"
C_GL_ACCOUNT = "GLAccount"
C_COST_CENTER = "CostCenter"
C_COMPANY = "Company"
C_CATEGORY = "Category"
C_SHEET = "Sheet"

_COLUMN_SIGNATURES: dict[str, list[str]] = {
    C_DATE: [
        "datum", "date", "leistungsdatum", "buchungsdatum", "belegdatum",
        "rechnungsdatum", "invoice date", "posting date", "erfassungsdatum",
        "valutadatum", "due date", "wertstellung",
    ],
    C_PERIOD: [
        "periode", "period", "monat", "month", "zeitraum", "quartal",
        "quarter", "jahr", "year",
    ],
    C_SUPPLIER_ID: [
        "lieferant nummer", "lieferantnummer", "lieferantennummer",
        "kreditoren nr", "kreditorennr", "kreditoren nummer", "kreditorennummer",
        "supplier id", "supplierid", "konto", "lieferant nummer 2",
        "dim_subledgeraccount", "kontonummer", "account number", "account no",
        "debitor nr", "debitornr", "kunden nr", "kundennr", "vendor id",
        "vendor no", "vendor number", "lieferanten nummer",
    ],
    C_SUPPLIER_NAME: [
        "lieferant name", "lieferantname", "lieferant", "kreditorenname",
        "supplier name", "suppliername", "beschriftung", "kurzbezeichnung",
        "name", "firma", "vendor name", "lieferantenname", "kontobezeichnung",
        "bezeichnung", "account name", "manufacturer",
    ],
    C_DEBIT: [
        "soll", "debit", "umsatz soll", "soll betrag", "sollbetrag",
        "debit amount",
    ],
    C_CREDIT: [
        "haben", "credit", "umsatz haben", "haben betrag", "habenbetrag",
        "credit amount",
    ],
    C_AMOUNT: [
        "summe", "amount", "betrag", "netto", "net", "gesamtbetrag",
        "total", "wert", "value", "brutto", "gross", "rechnungsbetrag",
        "invoice amount", "umsatz", "turnover", "nettobetrag", "bruttobetrag",
        "endbetrag", "gesamt",
    ],
    C_DESCRIPTION: [
        "beschreibung", "description", "text", "buchungstext", "bemerkung",
        "kommentar", "comment", "note", "anmerkung", "verwendungszweck",
        "purpose", "referenz", "reference", "betreff", "subject",
        "belegtext", "posting text",
    ],
    C_GL_ACCOUNT: [
        "sachkonto", "glaccount", "gl account", "dim_glaccount", "gegenkonto",
        "hauptbuch", "ledger account", "kontenrahmen", "sachkontonummer",
        "account", "konto (gl)",
    ],
    C_COST_CENTER: [
        "kostenstelle", "cost center", "dim_costcenter", "kost1", "kost2",
        "profit center", "profitcenter",
    ],
    C_CATEGORY: [
        "kategorie", "category", "warengruppe", "product group", "gruppe",
        "group", "materialgruppe", "material group", "typ", "type",
        "art", "kind", "klasse", "class",
    ],
}

_TYPE_DESCRIPTOR_KEYWORDS = frozenset({
    "varchar", "int", "double", "string", "float", "text",
    "ganzzahl (int)", "ganzzahl", "nvarchar", "datetime", "decimal",
})

_MONTH_NAMES_DE = {
    "januar": 1, "jan": 1, "februar": 2, "feb": 2, "maerz": 3, "mar": 3,
    "april": 4, "apr": 4, "mai": 5, "juni": 6, "jun": 6,
    "juli": 7, "jul": 7, "august": 8, "aug": 8, "september": 9, "sep": 9,
    "oktober": 10, "okt": 10, "november": 11, "nov": 11,
    "dezember": 12, "dez": 12,
}


def _best_column_match(raw_col: str) -> Optional[str]:
    normed = raw_col.strip().lower().replace("_", " ").replace("-", " ")
    for canonical, patterns in _COLUMN_SIGNATURES.items():
        if normed in patterns:
            return canonical
    for canonical, patterns in _COLUMN_SIGNATURES.items():
        for pat in patterns:
            if pat in normed or normed in pat:
                return canonical
    for canonical, patterns in _COLUMN_SIGNATURES.items():
        for pat in patterns:
            if SequenceMatcher(None, normed, pat).ratio() >= 0.85 and len(normed) > 3:
                return canonical
    return None


def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    raw_cols = [str(c).strip().lower() for c in df.columns]
    new_names: list[str] = []
    used: set[str] = set()
    keep: list[bool] = []
    for idx, raw in enumerate(raw_cols):
        target = _best_column_match(raw)
        if target and target not in used:
            new_names.append(target)
            used.add(target)
            keep.append(True)
        elif target and target in used:
            new_names.append(raw)
            keep.append(False)
        else:
            new_names.append(str(df.columns[idx]))
            keep.append(True)
    df.columns = new_names
    df = df.loc[:, keep]
    return df


def _to_numeric_safe(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace("\u20ac", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace("\xa0", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0.0)


def _first_row_has_type_descriptors(df: pd.DataFrame) -> bool:
    """True if row 0 looks like Excel type-metadata (VarChar, Int, …)."""
    for val in df.iloc[0].values:
        vs = str(val).lower().strip()
        if vs in ("nan", "<na>", "nat", "none", ""):
            continue
        if any(kw in vs for kw in _TYPE_DESCRIPTOR_KEYWORDS):
            return True
    return False


def _drop_descriptor_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if not _first_row_has_type_descriptors(df):
        return df
    rename_map: dict[str, str] = {}
    for col in df.columns:
        col_str = str(col)
        if "unnamed" not in col_str.lower() and not isinstance(col, (int, float)):
            continue
        desc_val = str(df.iloc[0][col]).strip()
        if desc_val and desc_val.lower() not in _TYPE_DESCRIPTOR_KEYWORDS and desc_val.lower() != "nan":
            rename_map[col] = desc_val
    if rename_map:
        df = df.rename(columns=rename_map)
    df = df.iloc[1:].reset_index(drop=True)
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all").reset_index(drop=True)
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.drop_duplicates().reset_index(drop=True)
    for col in df.select_dtypes(include="object").columns:
        try:
            df[col] = df[col].str.strip()
        except (AttributeError, TypeError):
            pass
    return df


def _detect_header_row(df_raw: pd.DataFrame, max_scan: int = 10) -> int:
    best_row, best_score = 0, 0
    for i in range(min(max_scan, len(df_raw))):
        row_vals = df_raw.iloc[i].astype(str).str.strip().str.lower()
        non_empty = row_vals[row_vals != ""].dropna()
        if len(non_empty) < 2:
            continue
        score = 0
        for v in non_empty:
            if _best_column_match(v):
                score += 3
            elif len(v) > 1 and not v.replace(".", "").replace(",", "").replace("-", "").isdigit():
                score += 1
        if score > best_score:
            best_score = score
            best_row = i
    return best_row


def _read_sheet_smart(raw_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    try:
        df_peek = pd.read_excel(io.BytesIO(raw_bytes), sheet_name=sheet_name, header=None, nrows=15)
    except Exception:
        return pd.read_excel(io.BytesIO(raw_bytes), sheet_name=sheet_name)
    header_row = _detect_header_row(df_peek)
    df = pd.read_excel(io.BytesIO(raw_bytes), sheet_name=sheet_name, header=header_row)
    unnamed = [c for c in df.columns if str(c).startswith("Unnamed")]
    if len(unnamed) > len(df.columns) * 0.7 and header_row == 0:
        for try_row in range(1, min(6, len(df))):
            row_vals = df.iloc[try_row - 1].astype(str).str.lower()
            if sum(1 for v in row_vals if _best_column_match(v)) >= 2:
                df = pd.read_excel(io.BytesIO(raw_bytes), sheet_name=sheet_name, header=try_row)
                break
    return df


def _classify_sheet(sheet_name: str, df: pd.DataFrame) -> str:
    name = sheet_name.strip().lower()
    if "material" in name:
        return "transactions"
    if "kreditor" in name:
        return "suppliers"
    if "pivot" in name or "tabelle" in name:
        return "pivot"
    if "verbindlich" in name or "liabilit" in name:
        return "liabilities"
    if "zusammenfassung" in name or "summary" in name:
        return "summary"
    cols_lower = {str(c).strip().lower() for c in df.columns}
    tx_signals = {"debit", "credit", "soll", "haben", "umsatz soll", "umsatz haben",
                  "period", "periode", "bundlecode", "financialpostingcode",
                  "betrag", "amount", "netto", "brutto"}
    if len(cols_lower & tx_signals) >= 2:
        return "transactions"
    supplier_signals = {"beschriftung", "kurzbezeichnung", "plz", "ort",
                        "supplier name", "kreditorenstammdaten", "telefon",
                        "email", "adresse", "address"}
    first_col_str = str(list(df.columns)[0]).lower() if len(df.columns) > 0 else ""
    if len(cols_lower & supplier_signals) >= 1 or "kreditorenstammdaten" in first_col_str:
        return "suppliers"
    if df.shape[1] == 2 and df.shape[0] < 500:
        return "pivot"
    if len(_detect_numeric_columns(df)) >= 1 and len(df) >= 3:
        return "transactions"
    return "unknown"


def _detect_numeric_columns(df: pd.DataFrame) -> list[str]:
    result = []
    for col in df.columns:
        if df[col].dtype in ("float64", "int64", "float32", "int32"):
            if df[col].notna().sum() > len(df) * 0.1:
                result.append(str(col))
            continue
        try:
            cleaned = (df[col].astype(str)
                       .str.replace("\u20ac", "", regex=False)
                       .str.replace("$", "", regex=False)
                       .str.replace(",", ".", regex=False)
                       .str.replace(" ", "", regex=False)
                       .str.replace("\xa0", "", regex=False).str.strip())
            converted = pd.to_numeric(cleaned, errors="coerce")
            if converted.notna().sum() / max(len(df), 1) > 0.3 and converted.abs().sum() > 0:
                result.append(str(col))
        except Exception:
            pass
    return result


def _detect_date_columns(df: pd.DataFrame) -> list[str]:
    result = []
    for col in df.columns:
        if df[col].dtype == "datetime64[ns]":
            result.append(str(col))
            continue
        try:
            sample = df[col].dropna().head(50)
            if sample.empty:
                continue
            parsed = pd.to_datetime(sample.astype(str), dayfirst=True, errors="coerce")
            if parsed.notna().sum() > len(sample) * 0.5:
                result.append(str(col))
        except Exception:
            pass
    return result


def _detect_category_columns(df: pd.DataFrame) -> list[str]:
    result = []
    for col in df.columns:
        if col in (C_DATE, C_PERIOD, C_DEBIT, C_CREDIT, C_AMOUNT):
            continue
        try:
            nunique = df[col].nunique()
            if 2 <= nunique <= min(200, len(df) * 0.5) and df[col].dtype == "object":
                result.append(str(col))
        except Exception:
            pass
    return result


def _parse_period_to_date(series: pd.Series, year: int = 2025) -> pd.Series:
    dates = []
    for val in series:
        val_str = str(val).strip().lower()
        if val_str in _MONTH_NAMES_DE:
            dates.append(pd.Timestamp(year=year, month=_MONTH_NAMES_DE[val_str], day=1))
            continue
        try:
            num = int(float(val_str))
            if 1 <= num <= 12:
                dates.append(pd.Timestamp(year=year, month=num, day=1))
            elif 202000 <= num <= 209912:
                y, m = divmod(num, 100)
                dates.append(pd.Timestamp(year=y, month=m, day=1) if 1 <= m <= 12 else pd.NaT)
            else:
                dates.append(pd.NaT)
        except (ValueError, TypeError):
            try:
                dates.append(pd.to_datetime(val_str, dayfirst=True))
            except Exception:
                dates.append(pd.NaT)
    return pd.Series(dates, index=series.index)


def _extract_year_from_filename(filename: str) -> int:
    match = re.search(r"(20\d{2})", filename)
    return int(match.group(1)) if match else 2025


def _parse_transactions(df: pd.DataFrame, filename: str = "") -> pd.DataFrame:
    df = _drop_descriptor_rows(df.copy())
    df = _map_columns(df)
    df = clean_dataframe(df)
    if df.empty:
        return pd.DataFrame()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str)
    file_year = _extract_year_from_filename(filename)

    if C_PERIOD in df.columns and C_DATE not in df.columns:
        df[C_DATE] = _parse_period_to_date(df[C_PERIOD], year=file_year)
    elif C_DATE in df.columns:
        if df[C_DATE].dtype == "object" or df[C_DATE].dtype == "O":
            df[C_DATE] = pd.to_datetime(df[C_DATE], dayfirst=True, errors="coerce")
    if C_DATE not in df.columns:
        dc = _detect_date_columns(df)
        if dc:
            df[C_DATE] = pd.to_datetime(df[dc[0]], dayfirst=True, errors="coerce")
        else:
            df[C_DATE] = pd.NaT

    for col in (C_DEBIT, C_CREDIT):
        if col in df.columns:
            df[col] = _to_numeric_safe(df[col])
        else:
            df[col] = 0.0

    debit_looks_wrong = (
        df[C_DEBIT].abs().max() <= 12 and df[C_DEBIT].nunique() <= 13 and len(df) > 20
    )

    if C_AMOUNT in df.columns:
        summe = _to_numeric_safe(df[C_AMOUNT])
        if debit_looks_wrong or (df[C_DEBIT].sum() == 0 and df[C_CREDIT].sum() == 0):
            df[C_DEBIT] = summe.clip(lower=0)
            df[C_CREDIT] = summe.clip(upper=0).abs()
    else:
        if df[C_DEBIT].sum() == 0 and df[C_CREDIT].sum() == 0:
            nc = _detect_numeric_columns(df)
            non_canon = [c for c in nc if c not in {C_DEBIT, C_CREDIT, C_AMOUNT, C_SUPPLIER_ID} and c in df.columns]
            if non_canon:
                av = _to_numeric_safe(df[non_canon[0]])
                df[C_DEBIT] = av.clip(lower=0)
                df[C_CREDIT] = av.clip(upper=0).abs()

    df[C_AMOUNT] = df[C_DEBIT] - df[C_CREDIT]

    if C_SUPPLIER_ID in df.columns:
        df[C_SUPPLIER_ID] = pd.to_numeric(
            df[C_SUPPLIER_ID].astype(str).str.replace("C_", "", regex=False), errors="coerce"
        )

    if C_SUPPLIER_NAME not in df.columns:
        if C_DESCRIPTION in df.columns:
            df[C_SUPPLIER_NAME] = df[C_DESCRIPTION]
        else:
            cc = _detect_category_columns(df)
            tc = [c for c in cc if c not in {C_DATE, C_PERIOD, C_DEBIT, C_CREDIT, C_AMOUNT, C_SUPPLIER_ID, C_GL_ACCOUNT, C_COST_CENTER}]
            df[C_SUPPLIER_NAME] = df[tc[0]] if tc else "Unknown"
    df[C_SUPPLIER_NAME] = df[C_SUPPLIER_NAME].fillna("Unknown").replace("nan", "Unknown")

    for col in (C_DESCRIPTION, C_GL_ACCOUNT, C_COST_CENTER, C_CATEGORY):
        if col not in df.columns:
            df[col] = ""

    keep = [C_DATE, C_SUPPLIER_ID, C_SUPPLIER_NAME, C_DEBIT, C_CREDIT, C_AMOUNT,
            C_DESCRIPTION, C_GL_ACCOUNT, C_COST_CENTER, C_CATEGORY]
    extras = [c for c in df.columns if c not in keep and c != C_PERIOD and not str(c).startswith("Unnamed")]
    result = df[[c for c in keep if c in df.columns] + extras].reset_index(drop=True)
    result = result[(result[C_DEBIT] != 0) | (result[C_CREDIT] != 0) | (result[C_AMOUNT] != 0)]
    return result.reset_index(drop=True)


def _parse_suppliers(df_raw: pd.DataFrame, raw_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    first_col = str(list(df_raw.columns)[0]).lower()
    if "kreditorenstammdaten" in first_col or "rechnungswesen" in first_col:
        header_row = df_raw.iloc[0]
        cols = [str(v).strip() if pd.notna(v) else f"col_{i}" for i, v in enumerate(header_row)]
        df = df_raw.iloc[1:].copy()
        df.columns = cols
        df = clean_dataframe(df)
        id_col, name_col = None, None
        cols_lower = {c.lower(): c for c in df.columns}
        for key in ["konto", "nr", "nummer", "kreditoren nr"]:
            if key in cols_lower:
                id_col = cols_lower[key]
                break
        for key in ["beschriftung", "name", "kurzbezeichnung"]:
            if key in cols_lower:
                name_col = cols_lower[key]
                break
        if id_col is None:
            id_col = df.columns[0]
        if name_col is None:
            name_col = df.columns[2] if len(df.columns) > 2 else df.columns[min(1, len(df.columns) - 1)]
        result = pd.DataFrame({
            C_SUPPLIER_ID: pd.to_numeric(df[id_col].astype(str).str.replace(" ", ""), errors="coerce"),
            C_SUPPLIER_NAME: df[name_col].astype(str).str.strip(),
        })
        result = result.dropna(subset=[C_SUPPLIER_ID])
        result[C_SUPPLIER_ID] = result[C_SUPPLIER_ID].astype(int)
        result[C_SUPPLIER_NAME] = result[C_SUPPLIER_NAME].replace("nan", "Unknown")
        return result.drop_duplicates(subset=[C_SUPPLIER_ID]).reset_index(drop=True)

    all_unnamed = all("unnamed" in str(c).lower() or isinstance(c, (int, float)) for c in df_raw.columns)
    str_cols = [str(c).strip().lower() for c in df_raw.columns]
    hdr_kw = {"nr", "nummer", "name", "bezeichnung", "kreditoren", "supplier", "lieferant", "id", "konto", "beschriftung"}
    has_real = any(set(c.replace("-", " ").replace("_", " ").split()) & hdr_kw for c in str_cols if not c.startswith("unnamed"))

    if all_unnamed or not has_real:
        try:
            df_nh = pd.read_excel(io.BytesIO(raw_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            df_nh = df_raw.copy()
            df_nh.columns = range(len(df_nh.columns))
        id_col_idx, name_col_idx, company_col_idx = None, None, None
        for idx in range(min(df_nh.shape[1], 10)):
            sample = df_nh[idx].dropna()
            if sample.empty:
                continue
            is_numeric = pd.to_numeric(sample, errors="coerce").notna().mean() > 0.8
            has_dash = sample.astype(str).str.contains(" - ").mean() > 0.3
            if has_dash and company_col_idx is None:
                company_col_idx = idx
                continue
            if is_numeric and id_col_idx is None:
                nums = pd.to_numeric(sample, errors="coerce").dropna()
                if len(nums) > 0 and nums.mean() > 1000:
                    id_col_idx = idx
                    continue
            if not is_numeric and name_col_idx is None and sample.dtype == object:
                name_col_idx = idx
                continue
        if id_col_idx is None:
            id_col_idx = 3 if df_nh.shape[1] > 3 else 0
        if name_col_idx is None:
            name_col_idx = 4 if df_nh.shape[1] > 4 else 1
        result = pd.DataFrame({
            C_SUPPLIER_ID: pd.to_numeric(df_nh[id_col_idx], errors="coerce"),
            C_SUPPLIER_NAME: df_nh[name_col_idx].astype(str).str.strip(),
        })
        if company_col_idx is not None:
            result[C_COMPANY] = df_nh[company_col_idx].apply(
                lambda v: str(v).split("-", 1)[1].strip() if isinstance(v, str) and "-" in v else str(v))
        result = result.dropna(subset=[C_SUPPLIER_ID])
        result[C_SUPPLIER_ID] = result[C_SUPPLIER_ID].astype(int)
        result[C_SUPPLIER_NAME] = result[C_SUPPLIER_NAME].replace("nan", "Unknown")
        return result.drop_duplicates(subset=[C_SUPPLIER_ID]).reset_index(drop=True)

    df = _map_columns(df_raw.copy())
    df = clean_dataframe(df)
    if C_SUPPLIER_ID not in df.columns:
        df = df.rename(columns={df.columns[0]: C_SUPPLIER_ID})
    if C_SUPPLIER_NAME not in df.columns:
        for c in df.columns:
            if c != C_SUPPLIER_ID:
                df = df.rename(columns={c: C_SUPPLIER_NAME})
                break
    if C_SUPPLIER_NAME not in df.columns:
        df[C_SUPPLIER_NAME] = "Unknown"
    df[C_SUPPLIER_ID] = pd.to_numeric(df[C_SUPPLIER_ID], errors="coerce")
    df = df.dropna(subset=[C_SUPPLIER_ID])
    df[C_SUPPLIER_ID] = df[C_SUPPLIER_ID].astype(int)
    keep = [c for c in [C_SUPPLIER_ID, C_SUPPLIER_NAME, C_COMPANY] if c in df.columns]
    return df[keep].drop_duplicates(subset=[C_SUPPLIER_ID]).reset_index(drop=True)


def _parse_pivot(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all").reset_index(drop=True)
    for i in range(min(5, len(df))):
        row_str = df.iloc[i].astype(str).str.lower()
        if row_str.str.contains("zeilenbeschriftung|summe von|supplier|manufacturer", regex=True).any():
            df = df.iloc[i + 1:].reset_index(drop=True)
            break
    if df.shape[1] < 2:
        return pd.DataFrame(columns=[C_SUPPLIER_NAME, C_AMOUNT])
    result = pd.DataFrame({
        C_SUPPLIER_NAME: df[df.columns[0]].astype(str).str.strip(),
        C_AMOUNT: _to_numeric_safe(df[df.columns[1]]),
    })
    result = result[result[C_SUPPLIER_NAME].notna() & (result[C_SUPPLIER_NAME] != "nan")]
    return result[result[C_AMOUNT].abs() > 0].reset_index(drop=True)


def _parse_generic(df: pd.DataFrame, filename: str = "") -> pd.DataFrame:
    df = _map_columns(df.copy())
    df = _drop_descriptor_rows(df)
    df = clean_dataframe(df)
    if df.empty or len(df.columns) < 2:
        return pd.DataFrame()
    nc = _detect_numeric_columns(df)
    if not nc:
        return pd.DataFrame()
    for col in nc:
        if col in df.columns:
            df[col] = _to_numeric_safe(df[col])
    if C_AMOUNT not in df.columns:
        best = next((c for c in nc if c not in {C_SUPPLIER_ID, C_DEBIT, C_CREDIT}), None)
        df[C_AMOUNT] = _to_numeric_safe(df[best]) if best else 0.0
    if C_DEBIT not in df.columns:
        df[C_DEBIT] = df[C_AMOUNT].clip(lower=0) if C_AMOUNT in df.columns else 0.0
    if C_CREDIT not in df.columns:
        df[C_CREDIT] = df[C_AMOUNT].clip(upper=0).abs() if C_AMOUNT in df.columns else 0.0
    if C_DATE not in df.columns:
        dc = _detect_date_columns(df)
        if dc:
            df[C_DATE] = pd.to_datetime(df[dc[0]], dayfirst=True, errors="coerce")
        elif C_PERIOD in df.columns:
            df[C_DATE] = _parse_period_to_date(df[C_PERIOD], year=_extract_year_from_filename(filename))
        else:
            df[C_DATE] = pd.NaT
    if C_SUPPLIER_NAME not in df.columns:
        cc = _detect_category_columns(df)
        tc = [c for c in cc if c not in set(nc)]
        if tc:
            df[C_SUPPLIER_NAME] = df[tc[0]]
        elif C_DESCRIPTION in df.columns:
            df[C_SUPPLIER_NAME] = df[C_DESCRIPTION]
        else:
            df[C_SUPPLIER_NAME] = "Unknown"
    df[C_SUPPLIER_NAME] = df[C_SUPPLIER_NAME].fillna("Unknown").replace("nan", "Unknown")
    for col in (C_DESCRIPTION, C_GL_ACCOUNT, C_COST_CENTER, C_CATEGORY):
        if col not in df.columns:
            df[col] = ""
    keep = [C_DATE, C_SUPPLIER_ID, C_SUPPLIER_NAME, C_DEBIT, C_CREDIT, C_AMOUNT,
            C_DESCRIPTION, C_GL_ACCOUNT, C_COST_CENTER, C_CATEGORY]
    return df[[c for c in keep if c in df.columns]].reset_index(drop=True)


_INVOICE_SUFFIXES = (
    "_rechnungseingang", "_rechnungseingan", "_rechnungseing",
    "_e-rechnung", "_rechnung", "_rechnungseing",
    "rechnungseingang", "rechnungseing",
)

_LEGAL_SUFFIXES = (
    " gmbh & co. kg", " gmbh & co.kg", " gmbh & co kg",
    " gmbh & co", " gmbh", " ag", " kg", " ohg", " e.k.",
    " e.k", " mbh", " ug", " se", " co.", " inc.", " ltd.",
    " corp.", " s.a.", " s.r.l.",
)


def _german_ascii_fold(s: str) -> str:
    """Lowercase, expand umlauts, strip accents (Müller/Mueller/Muller align better)."""
    s = s.strip().lower()
    repl = {
        "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
        "à": "a", "á": "a", "â": "a", "ã": "a",
        "è": "e", "é": "e", "ê": "e", "ë": "e",
        "ì": "i", "í": "i", "î": "i", "ï": "i",
        "ò": "o", "ó": "o", "ô": "o", "õ": "o",
        "ù": "u", "ú": "u", "û": "u",
        "ý": "y", "ÿ": "y", "ñ": "n", "ç": "c",
    }
    s = "".join(repl.get(c, c) for c in s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _supplier_match_key(name: str) -> str:
    """Stable key for grouping: strips booking refs, Skontoabzug line IDs, invoice tails."""
    if not name or str(name).strip().lower() in ("", "nan", "unknown", "none"):
        return ""
    n = _german_ascii_fold(str(name))

    for suf in _LEGAL_SUFFIXES:
        if n.endswith(suf):
            n = n[: -len(suf)].strip()

    n = re.sub(r"\s*#\s*\d+.*$", "", n)
    n = re.sub(r"\s+proj\.?\s*_.*$", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s+wu\s*$", "", n, flags=re.IGNORECASE)

    for suf in _INVOICE_SUFFIXES:
        if suf in n:
            n = n[: n.index(suf)].strip()
            break

    n = re.sub(r"\s+/\s*hn-v\s+.*$", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s+/\s*\d[\d\s]*$", "", n)
    n = re.sub(r"\s+/\s*\d+$", "", n)

    sk = re.match(r"^(skontoabzug)\s+(\d+)\s*$", n)
    if sk:
        return f"{sk.group(1)} {sk.group(2)}"

    hm = re.search(r"hn-?\s*v\s*(\d+)", n.replace(" ", ""))
    if hm:
        return f"hnv{hm.group(1)}"

    n = re.sub(r"[/\\]+", " ", n)
    n = re.sub(r"[_]+", " ", n)
    n = re.sub(r"[^\w\s\-]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    n = re.sub(r"\s+\d{6,}\s*$", "", n)
    return n


def _normalize_supplier_name(name: str) -> str:
    """Strip legal suffixes; used as secondary signal with match key."""
    n = _german_ascii_fold(str(name))
    for suffix in _LEGAL_SUFFIXES:
        if n.endswith(suffix):
            n = n[: -len(suffix)].strip()
    n = re.sub(r"[,./&\-]+$", "", n).strip()
    n = re.sub(r"\s+", " ", n)
    return n


def _name_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    ra = SequenceMatcher(None, a, b).ratio()
    rb = 0.0
    la, lb = len(a), len(b)
    shorter, longer = (a, b) if la <= lb else (b, a)
    if len(shorter) >= 4 and len(longer) >= 4:
        if longer.startswith(shorter) or longer.endswith(" " + shorter):
            rb = max(rb, 0.92)
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if tokens_a and tokens_b:
        inter = len(tokens_a & tokens_b)
        union = len(tokens_a | tokens_b)
        if union > 0:
            rb = max(rb, inter / union)
    return max(ra, rb)


def _fuzzy_group_names(names: list[str], threshold: float = 0.82) -> dict[str, str]:
    """Map similar supplier names to one canonical display name (longest original)."""
    raw = sorted({str(n).strip() for n in names if str(n).strip() and str(n).lower() not in ("nan", "unknown")})
    if len(raw) <= 1:
        return {n: n for n in raw}

    key_for: dict[str, str] = {}
    buckets: dict[str, list[str]] = {}
    for name in raw:
        k = _supplier_match_key(name)
        if not k:
            k = _normalize_supplier_name(name) or name.lower()
        key_for[name] = k
        buckets.setdefault(k, []).append(name)

    def canonical_for_bucket(members: list[str]) -> str:
        return max(members, key=lambda x: (len(x), x))

    key_canon: dict[str, str] = {k: canonical_for_bucket(v) for k, v in buckets.items()}
    keys = list(key_canon.keys())

    parent = {k: k for k in keys}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: str, y: str) -> None:
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    def _structured_keys_distinct(a: str, b: str) -> bool:
        """Do not fuzzy-merge different booking accounts (Skontoabzug / HN-V IDs)."""
        if re.match(r"^skontoabzug \d+$", a) and re.match(r"^skontoabzug \d+$", b):
            return a != b
        if re.match(r"^hnv\d+$", a) and re.match(r"^hnv\d+$", b):
            return a != b
        return False

    short_thr = max(0.72, threshold - 0.08)
    for i, ka in enumerate(keys):
        for kb in keys[i + 1 :]:
            if ka == kb:
                continue
            if _structured_keys_distinct(ka, kb):
                continue
            ca, cb = key_canon[ka], key_canon[kb]
            sim_key = _name_similarity(ka, kb)
            sim_norm = _name_similarity(
                _normalize_supplier_name(ca), _normalize_supplier_name(cb)
            )
            min_len = min(len(ka), len(kb))
            thr = short_thr if min_len <= 12 else threshold
            if sim_key >= thr or sim_norm >= thr:
                union(ka, kb)
                continue
            if min_len >= 5 and (ka.startswith(kb + " ") or kb.startswith(ka + " ")):
                union(ka, kb)

    root_members: dict[str, list[str]] = {}
    for k in keys:
        r = find(k)
        root_members.setdefault(r, []).extend(buckets[k])

    mapping: dict[str, str] = {}
    for _root, members in root_members.items():
        canon = canonical_for_bucket(list(dict.fromkeys(members)))
        for m in members:
            mapping[m] = canon

    return mapping


def _merge_transactions_suppliers(transactions: pd.DataFrame, suppliers: pd.DataFrame) -> pd.DataFrame:
    if suppliers.empty or C_SUPPLIER_ID not in transactions.columns or C_SUPPLIER_ID not in suppliers.columns:
        merged = transactions.copy()
    else:
        sup_merge = suppliers[[c for c in [C_SUPPLIER_ID, C_SUPPLIER_NAME, C_COMPANY] if c in suppliers.columns]].drop_duplicates(subset=[C_SUPPLIER_ID])
        if C_SUPPLIER_NAME in sup_merge.columns:
            sup_merge = sup_merge.rename(columns={C_SUPPLIER_NAME: "_SupName"})
        merged = transactions.merge(sup_merge, on=C_SUPPLIER_ID, how="left")
        if "_SupName" in merged.columns:
            mask = merged[C_SUPPLIER_NAME].isin(["Unknown", "nan", ""])
            merged.loc[mask, C_SUPPLIER_NAME] = merged.loc[mask, "_SupName"]
            merged.drop(columns=["_SupName"], inplace=True)
    if C_COMPANY not in merged.columns:
        merged[C_COMPANY] = "Unknown"
    merged[C_COMPANY] = merged[C_COMPANY].fillna("Unknown")
    merged[C_SUPPLIER_NAME] = merged[C_SUPPLIER_NAME].fillna("Unknown")
    return merged


def _extract_company_from_filename(filename: str) -> str:
    name = filename.replace(".xlsx", "").replace(".xls", "").replace(".csv", "")
    name = re.sub(r"^Kopie von\s*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"^Copy of\s*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"^\d{6}_", "", name)
    name = re.sub(r"_Materialkosten.*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"_Material.*$", "", name, flags=re.IGNORECASE)
    return name.replace("_", " ").strip() or filename


def process_file(uploaded) -> dict:
    raw_bytes = uploaded.getvalue()
    filename = uploaded.name
    company_label = _extract_company_from_filename(filename)
    all_transactions: list[pd.DataFrame] = []
    all_suppliers: list[pd.DataFrame] = []
    all_pivots: list[pd.DataFrame] = []
    sheet_info: list[dict] = []
    is_csv = filename.lower().endswith(".csv")

    if is_csv:
        try:
            df_raw = pd.read_csv(io.BytesIO(raw_bytes), encoding="utf-8", sep=None, engine="python")
        except Exception:
            try:
                df_raw = pd.read_csv(io.BytesIO(raw_bytes), encoding="latin-1", sep=None, engine="python")
            except Exception:
                df_raw = pd.DataFrame()
        if not df_raw.empty:
            classification = _classify_sheet("data", df_raw)
            sheet_info.append({"sheet": "CSV Data", "type": classification, "rows": len(df_raw), "cols": len(df_raw.columns)})
            parsed = _parse_transactions(df_raw, filename)
            if not parsed.empty:
                parsed[C_SOURCE] = company_label
                all_transactions.append(parsed)
    else:
        try:
            xls = pd.ExcelFile(io.BytesIO(raw_bytes))
        except Exception:
            return {"filename": filename, "company": company_label, "transactions": pd.DataFrame(),
                    "suppliers": pd.DataFrame(), "pivots": pd.DataFrame(),
                    "sheet_info": [{"sheet": "ERROR", "type": "unreadable", "rows": 0, "cols": 0}]}
        for sheet_name in xls.sheet_names:
            try:
                df_raw = _read_sheet_smart(raw_bytes, sheet_name)
            except Exception:
                try:
                    df_raw = pd.read_excel(xls, sheet_name=sheet_name)
                except Exception:
                    sheet_info.append({"sheet": sheet_name, "type": "unreadable", "rows": 0, "cols": 0})
                    continue
            classification = _classify_sheet(sheet_name, df_raw)
            sheet_info.append({"sheet": sheet_name, "type": classification, "rows": len(df_raw), "cols": len(df_raw.columns)})
            if classification == "transactions":
                parsed = _parse_transactions(df_raw, filename)
                if not parsed.empty:
                    parsed[C_SOURCE] = company_label
                    parsed[C_SHEET] = sheet_name
                    all_transactions.append(parsed)
            elif classification == "suppliers":
                parsed = _parse_suppliers(df_raw, raw_bytes, sheet_name)
                all_suppliers.append(parsed)
            elif classification == "pivot":
                parsed = _parse_pivot(df_raw)
                if not parsed.empty:
                    parsed[C_SOURCE] = company_label
                    all_pivots.append(parsed)
            elif classification in ("liabilities", "summary"):
                parsed = _parse_transactions(df_raw, filename)
                if not parsed.empty:
                    parsed[C_SOURCE] = company_label
                    parsed[C_SHEET] = sheet_name
                    all_transactions.append(parsed)
            else:
                parsed = _parse_generic(df_raw, filename)
                if not parsed.empty and len(parsed) >= 2:
                    parsed[C_SOURCE] = company_label
                    parsed[C_SHEET] = sheet_name
                    all_transactions.append(parsed)

    transactions = pd.concat(all_transactions, ignore_index=True) if all_transactions else pd.DataFrame()
    suppliers = pd.concat(all_suppliers, ignore_index=True) if all_suppliers else pd.DataFrame()
    pivots = pd.concat(all_pivots, ignore_index=True) if all_pivots else pd.DataFrame()
    if not transactions.empty and not suppliers.empty:
        transactions = _merge_transactions_suppliers(transactions, suppliers)
    return {"filename": filename, "company": company_label, "transactions": transactions,
            "suppliers": suppliers, "pivots": pivots, "sheet_info": sheet_info}


def _agg_join_unique_strings(series: pd.Series) -> str:
    """Join unique non-empty string values in a group (Arrow/pd.NA safe)."""
    seen: set[str] = set()
    for val in series:
        if pd.isna(val):
            continue
        s = str(val).strip()
        if not s or s.lower() == "nan":
            continue
        seen.add(s)
    return ", ".join(sorted(seen))


def _agg_join_months(series: pd.Series) -> str:
    seen: set[str] = set()
    for val in series:
        if pd.isna(val):
            continue
        s = str(val).strip()
        if not s or s in ("N/A", "nan"):
            continue
        for part in s.split(","):
            p = part.strip()
            if p and p not in ("N/A", "nan"):
                seen.add(p)
    return ", ".join(sorted(seen))


def _aggregate_by_supplier(df: pd.DataFrame) -> pd.DataFrame:
    """Consolidate transactions into one row per supplier with summed amounts."""
    if df.empty or C_SUPPLIER_NAME not in df.columns:
        return df

    group_cols = [C_SUPPLIER_NAME]
    if C_SOURCE in df.columns:
        group_cols.append(C_SOURCE)

    agg_dict = {}
    if C_DEBIT in df.columns:
        agg_dict[C_DEBIT] = "sum"
    if C_CREDIT in df.columns:
        agg_dict[C_CREDIT] = "sum"
    if C_AMOUNT in df.columns:
        agg_dict[C_AMOUNT] = "sum"

    if not agg_dict:
        return df

    extra_agg = {}
    if C_SUPPLIER_ID in df.columns:
        extra_agg[C_SUPPLIER_ID] = "first"
    if C_GL_ACCOUNT in df.columns:
        gl = df[C_GL_ACCOUNT].map(lambda v: str(v).strip() if pd.notna(v) else "")
        if (gl != "").any():
            extra_agg[C_GL_ACCOUNT] = _agg_join_unique_strings
    if C_COST_CENTER in df.columns:
        cc = df[C_COST_CENTER].map(lambda v: str(v).strip() if pd.notna(v) else "")
        if (cc != "").any():
            extra_agg[C_COST_CENTER] = _agg_join_unique_strings
    if C_COMPANY in df.columns:
        extra_agg[C_COMPANY] = "first"
    if "Month" in df.columns:
        extra_agg["Month"] = _agg_join_months

    agg_dict.update(extra_agg)

    agg_df = df.groupby(group_cols, as_index=False).agg(agg_dict)
    agg_df["Transactions"] = df.groupby(group_cols).size().values
    agg_df = agg_df.sort_values(C_DEBIT, ascending=False).reset_index(drop=True)
    return agg_df


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="CleanedData")
    return buf.getvalue()


EURO = "\u20ac"


def main() -> None:
    st.set_page_config(page_title="ERP Procurement Dashboard", layout="wide")
    st.markdown("""<style>
    [data-testid="stMetricValue"] { font-size: 1.15rem; }
    .block-container { padding-top: 1.5rem; }
    div[data-testid="stExpander"] details summary p { font-weight: 600; }
    </style>""", unsafe_allow_html=True)
    st.title("ERP Procurement Dashboard")

    with st.sidebar:
        st.header("Upload & Filters")
        uploaded_files = st.file_uploader("Upload Excel or CSV files", type=["xlsx", "xls", "csv"], accept_multiple_files=True)

    if not uploaded_files:
        st.info("Upload one or more Excel/CSV files to get started.\n\n"
                "The dashboard **auto-detects** sheet types, column names, header rows, "
                "numeric fields, dates, and categories -- no manual configuration needed.")
        return

    results: list[dict] = []
    progress = st.progress(0, text="Processing files...")
    for i, f in enumerate(uploaded_files):
        results.append(process_file(f))
        progress.progress((i + 1) / len(uploaded_files), text=f"Processed {f.name}")
    progress.empty()

    all_tx = pd.concat([r["transactions"] for r in results if not r["transactions"].empty], ignore_index=True) if any(not r["transactions"].empty for r in results) else pd.DataFrame()
    all_suppliers = pd.concat([r["suppliers"] for r in results if not r["suppliers"].empty], ignore_index=True) if any(not r["suppliers"].empty for r in results) else pd.DataFrame()
    all_pivots = pd.concat([r["pivots"] for r in results if not r["pivots"].empty], ignore_index=True) if any(not r["pivots"].empty for r in results) else pd.DataFrame()

    if all_tx.empty and all_pivots.empty:
        st.error("No transaction data could be extracted from the uploaded files.")
        with st.expander("File Processing Details"):
            for r in results:
                st.markdown(f"**{r['filename']}**")
                if r["sheet_info"]:
                    st.dataframe(pd.DataFrame(r["sheet_info"]), use_container_width=True, hide_index=True)
        return

    if all_tx.empty and not all_pivots.empty:
        all_tx = all_pivots.copy()
        all_tx[C_DEBIT] = all_tx[C_AMOUNT].clip(lower=0)
        all_tx[C_CREDIT] = all_tx[C_AMOUNT].clip(upper=0).abs()
        if C_DATE not in all_tx.columns:
            all_tx[C_DATE] = pd.NaT
        if C_DESCRIPTION not in all_tx.columns:
            all_tx[C_DESCRIPTION] = ""

    if C_DATE in all_tx.columns and all_tx[C_DATE].notna().any():
        all_tx["Month"] = pd.to_datetime(all_tx[C_DATE], errors="coerce").dt.to_period("M").astype(str)
    else:
        all_tx["Month"] = "N/A"

    if C_SUPPLIER_NAME in all_tx.columns:
        unique_names = all_tx[C_SUPPLIER_NAME].dropna().unique().tolist()
        if 1 < len(unique_names) < 2000:
            name_map = _fuzzy_group_names(unique_names)
            all_tx[C_SUPPLIER_NAME] = all_tx[C_SUPPLIER_NAME].map(name_map).fillna(all_tx[C_SUPPLIER_NAME])

    with st.expander("Uploaded Files Overview", expanded=False):
        for r in results:
            st.markdown(f"**{r['filename']}** -- Company: *{r['company']}*")
            st.dataframe(pd.DataFrame(r["sheet_info"]), use_container_width=True, hide_index=True)

    with st.expander("Cleaned Data Preview", expanded=False):
        tab_tx, tab_sup = st.tabs(["Transactions", "Suppliers"])
        with tab_tx:
            st.dataframe(all_tx.head(100), use_container_width=True, hide_index=True)
        with tab_sup:
            if not all_suppliers.empty:
                st.dataframe(all_suppliers.head(100), use_container_width=True, hide_index=True)
            else:
                st.info("No supplier master data found.")

    with st.sidebar:
        st.markdown("---")
        selected_companies = []
        if C_SOURCE in all_tx.columns:
            selected_companies = st.multiselect("Company / File", options=sorted(all_tx[C_SOURCE].dropna().unique().tolist()), default=[])
        selected_suppliers = []
        if C_SUPPLIER_NAME in all_tx.columns:
            selected_suppliers = st.multiselect("Supplier", options=sorted(all_tx[C_SUPPLIER_NAME].dropna().unique().tolist()), default=[])
        month_options = sorted([m for m in all_tx["Month"].dropna().unique().tolist() if m != "N/A"])
        selected_months = st.multiselect("Month", options=month_options, default=[])
        extra_filters: dict[str, list] = {}
        for col in [c for c in [C_GL_ACCOUNT, C_COST_CENTER, C_CATEGORY] if c in all_tx.columns]:
            vals = all_tx[col].dropna()
            vals = vals[vals.astype(str).str.strip() != ""]
            uv = sorted(vals.unique().tolist())
            if 2 <= len(uv) <= 100:
                sel = st.multiselect(col, options=uv, default=[])
                if sel:
                    extra_filters[col] = sel

    view = all_tx.copy()
    if selected_companies:
        view = view[view[C_SOURCE].isin(selected_companies)]
    if selected_suppliers:
        view = view[view[C_SUPPLIER_NAME].isin(selected_suppliers)]
    if selected_months:
        view = view[view["Month"].isin(selected_months)]
    for col, vals in extra_filters.items():
        view = view[view[col].isin(vals)]

    total_debit = view[C_DEBIT].sum() if C_DEBIT in view.columns else 0
    total_credit = view[C_CREDIT].sum() if C_CREDIT in view.columns else 0
    total_net = view[C_AMOUNT].sum() if C_AMOUNT in view.columns else 0
    n_suppliers = view[C_SUPPLIER_NAME].nunique() if C_SUPPLIER_NAME in view.columns else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Debit (Spend)", f"{EURO} {total_debit:,.2f}")
    k2.metric("Total Credit (Returns)", f"{EURO} {total_credit:,.2f}")
    k3.metric("Net Amount", f"{EURO} {total_net:,.2f}")
    k4.metric("Unique Suppliers", f"{n_suppliers:,}")
    k5.metric("Files / Records", f"{len(uploaded_files)} / {len(view):,}")
    st.markdown("---")

    col_left, col_right = st.columns(2)
    spend_view = view[view[C_DEBIT] > 0] if C_DEBIT in view.columns else view

    with col_left:
        st.subheader("Top 20 Suppliers by Spend")
        if C_SUPPLIER_NAME in spend_view.columns and not spend_view.empty:
            top_sup = spend_view.groupby(C_SUPPLIER_NAME, as_index=False)[C_DEBIT].sum().sort_values(C_DEBIT, ascending=False).head(20)
            if not top_sup.empty:
                fig = px.bar(top_sup, x=C_SUPPLIER_NAME, y=C_DEBIT, color=C_DEBIT,
                             color_continuous_scale="Blues", labels={C_DEBIT: f"Spend ({EURO})", C_SUPPLIER_NAME: "Supplier"})
                fig.update_layout(xaxis_tickangle=-45, showlegend=False, coloraxis_showscale=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No spend data.")
        else:
            st.info("No spend data.")

    with col_right:
        st.subheader("Monthly Spend Trend")
        vm = spend_view[spend_view["Month"] != "N/A"] if "Month" in spend_view.columns else pd.DataFrame()
        if not vm.empty and C_DEBIT in vm.columns:
            monthly = vm.groupby("Month", as_index=False)[C_DEBIT].sum().sort_values("Month")
            fig = px.line(monthly, x="Month", y=C_DEBIT, markers=True,
                          labels={C_DEBIT: f"Spend ({EURO})", "Month": "Month"})
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Date information not available for monthly chart.")

    st.markdown("---")

    if C_SOURCE in view.columns and view[C_SOURCE].nunique() > 1:
        st.subheader("Spend by Company / File")
        c1, c2 = st.columns(2)
        by_company = view.groupby(C_SOURCE, as_index=False)[C_DEBIT].sum().sort_values(C_DEBIT, ascending=False)
        with c1:
            fig = px.bar(by_company, x=C_SOURCE, y=C_DEBIT, color=C_DEBIT,
                         color_continuous_scale="Greens", labels={C_DEBIT: f"Spend ({EURO})", C_SOURCE: "Company"})
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = px.pie(by_company, names=C_SOURCE, values=C_DEBIT, labels={C_DEBIT: f"Spend ({EURO})", C_SOURCE: "Company"})
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")

    if C_SOURCE in view.columns and view[C_SOURCE].nunique() > 1 and "Month" in view.columns:
        vm2 = view[view["Month"] != "N/A"]
        if not vm2.empty:
            st.subheader("Monthly Spend by Company")
            mc = vm2.groupby([C_SOURCE, "Month"], as_index=False)[C_DEBIT].sum().sort_values("Month")
            fig = px.line(mc, x="Month", y=C_DEBIT, color=C_SOURCE, markers=True,
                          labels={C_DEBIT: f"Spend ({EURO})", "Month": "Month", C_SOURCE: "Company"})
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("---")

    breakdown_cols = [c for c in [C_GL_ACCOUNT, C_COST_CENTER, C_CATEGORY]
                      if c in view.columns and view[c].astype(str).str.strip().replace("", pd.NA).dropna().nunique() >= 2]
    if breakdown_cols:
        st.subheader("Spend Breakdown")
        tabs = st.tabs(breakdown_cols)
        for tab, col in zip(tabs, breakdown_cols):
            with tab:
                bd = view[view[col].astype(str).str.strip() != ""].groupby(col, as_index=False)[C_DEBIT].sum().sort_values(C_DEBIT, ascending=False).head(25)
                if not bd.empty:
                    bc1, bc2 = st.columns(2)
                    with bc1:
                        fig = px.bar(bd, x=col, y=C_DEBIT, color=C_DEBIT, color_continuous_scale="Oranges", labels={C_DEBIT: f"Spend ({EURO})"})
                        fig.update_layout(xaxis_tickangle=-45, showlegend=False, coloraxis_showscale=False)
                        st.plotly_chart(fig, use_container_width=True)
                    with bc2:
                        fig = px.pie(bd.head(10), names=col, values=C_DEBIT)
                        fig.update_traces(textposition="inside", textinfo="percent+label")
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No data for {col}.")
        st.markdown("---")

    if C_SUPPLIER_NAME in view.columns and not view.empty:
        st.subheader("Supplier Debit vs Credit")
        sc = view.groupby(C_SUPPLIER_NAME, as_index=False).agg({C_DEBIT: "sum", C_CREDIT: "sum"})
        sc = sc[(sc[C_DEBIT] > 0) | (sc[C_CREDIT] > 0)]
        if not sc.empty and len(sc) > 1:
            sc["_size"] = (sc[C_DEBIT].abs() + sc[C_CREDIT].abs()).clip(lower=1)
            fig = px.scatter(sc, x=C_DEBIT, y=C_CREDIT, hover_name=C_SUPPLIER_NAME,
                             size="_size",
                             labels={C_DEBIT: f"Total Debit ({EURO})", C_CREDIT: f"Total Credit ({EURO})"}, size_max=40)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("---")

    if C_AMOUNT in view.columns and len(view) > 10:
        st.subheader("Transaction Amount Distribution")
        amounts = view[C_AMOUNT][view[C_AMOUNT] != 0]
        if len(amounts) > 5:
            fig = px.histogram(amounts, nbins=50, labels={"value": f"Amount ({EURO})", "count": "Frequency"},
                               color_discrete_sequence=["#636EFA"])
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("---")

    vm3 = view[view["Month"] != "N/A"] if "Month" in view.columns else pd.DataFrame()
    if not vm3.empty and C_SUPPLIER_NAME in vm3.columns and vm3["Month"].nunique() >= 2:
        st.subheader("Top 15 Suppliers -- Monthly Heatmap")
        top15 = vm3.groupby(C_SUPPLIER_NAME)[C_DEBIT].sum().nlargest(15).index.tolist()
        hm = vm3[vm3[C_SUPPLIER_NAME].isin(top15)].groupby([C_SUPPLIER_NAME, "Month"])[C_DEBIT].sum().unstack(fill_value=0)
        if not hm.empty:
            fig = px.imshow(hm, labels=dict(x="Month", y="Supplier", color=f"Spend ({EURO})"),
                            color_continuous_scale="YlOrRd", aspect="auto")
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("---")

    st.subheader("Supplier Summary (Aggregated)")
    agg_view = _aggregate_by_supplier(view)
    st.dataframe(agg_view, use_container_width=True, height=400, hide_index=True)

    with st.expander("All Individual Transactions", expanded=False):
        st.dataframe(view, use_container_width=True, height=400, hide_index=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("Download Supplier Summary (Excel)", data=to_excel_bytes(agg_view),
                           file_name="supplier_summary.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with c2:
        st.download_button("Download All Transactions (Excel)", data=to_excel_bytes(view),
                           file_name="cleaned_procurement_data.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with c3:
        if not all_suppliers.empty:
            st.download_button("Download Suppliers (Excel)", data=to_excel_bytes(all_suppliers),
                               file_name="cleaned_suppliers.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    main()
