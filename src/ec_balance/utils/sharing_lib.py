# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import List, Tuple
import re
import numpy as np
import pandas as pd

# ---------------- I/O ----------------
def ensure_csv_dir(outdir: Path) -> Path:
    outdir = Path(outdir)
    (outdir / "csv").mkdir(parents=True, exist_ok=True)
    return outdir / "csv"

# v sharing_lib.py nahraď původní safe_to_csv touto verzí
def safe_to_csv(df, outroot, name, *, strict: bool | None = None):
    """
    Ulož CSV bez překvapení:
      - Pokud strict=True (nebo ENERGO_STRICT_OUTDIR=1), ukládá **přesně** do outroot.
      - Jinak (kvůli zpětné kompatibilitě) přidá podadresář 'csv' jen tehdy,
        když outroot NEkončí na 'csv'.
    Vrací plnou cestu k výslednému souboru.
    """
    import os
    from pathlib import Path

    if strict is None:
        strict = os.getenv("ENERGO_STRICT_OUTDIR", "0") == "1"

    outroot = Path(outroot)
    if strict:
        target_dir = outroot
    else:
        target_dir = outroot if outroot.name.lower() == "csv" else (outroot / "csv")

    target_dir.mkdir(parents=True, exist_ok=True)
    out_path = target_dir / f"{name}.csv"
    df.to_csv(out_path, index=False)
    print(f"[OK] {name}: {out_path}")
    return out_path


# ------------- helpers -------------
def _coerce_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

def iso_datetime_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    df = df.copy()
    tcol = None
    for cand in ("datetime", "ts", "Timestamp", "timestamp", "time"):
        if cand in df.columns:
            tcol = cand; break
    if tcol is None:
        tcol = df.columns[0]
    s = _coerce_datetime(df[tcol])
    df = df.drop(columns=[tcol])
    df.insert(0, "datetime", s)
    return df

def _flatten_multiindex_cols(cols: pd.Index) -> List[str]:
    if not isinstance(cols, pd.MultiIndex):
        return [str(c) for c in cols]
    return [" / ".join([str(x) for x in tup if str(x) != "nan"]) for tup in cols]

# -------- wide -> long (v kWh) -------
def read_wide_to_long(path: str | Path, *, sep: str = ",", header_rows: int = 1, units: str = "mwh") -> pd.DataFrame:
    """
    Vstupní wide CSV (1. sloupec = čas), ostatní sloupce = EAN/site.
    - `header_rows`: 1 = běžná hlavička, >1 = multiindex → zploštíme.
    - `units`: 'mwh' nebo 'kwh'. Vracíme kWh ve sloupci `value_kwh`.

    Výstup long: [datetime, site, ean, value_kwh]
    """
    path = Path(path)
    hr = list(range(header_rows)) if header_rows and header_rows > 1 else 0
    df = pd.read_csv(path, sep=sep, header=hr)
    df.columns = _flatten_multiindex_cols(df.columns)
    df = iso_datetime_col(df)

    value_cols = [c for c in df.columns if c != "datetime"]
    if not value_cols:
        raise ValueError("Wide vstup nemá žádné datové sloupce.")

    long_df = df.melt(id_vars=["datetime"], value_vars=value_cols, var_name="site", value_name="value")
    long_df["ean"] = long_df["site"].astype(str)

    if units.lower() == "mwh":
        long_df["value_kwh"] = pd.to_numeric(long_df["value"], errors="coerce") * 1000.0
    elif units.lower() == "kwh":
        long_df["value_kwh"] = pd.to_numeric(long_df["value"], errors="coerce")
    else:
        raise ValueError("units musí být 'mwh' nebo 'kwh'")

    long_df = long_df.drop(columns=["value"])
    long_df = long_df.dropna(subset=["datetime"]).sort_values(["datetime", "site"]).reset_index(drop=True)
    return long_df

# ------- canonical key (fallback) -----
def _canonical_site_text(s: str) -> str:
    t = str(s).strip().lower()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s*[\(\[\{].*?[\)\]\}]\s*$", "", t)  # odstraň [poznámky]
    for tok in (" odběr"," odber"," import"," load"," výroba"," vyroba"," export"," prod"," pv"," fve"," o"," d"):
        if t.endswith(tok): t = t[: -len(tok)]
    return t.strip()

def apply_site_key(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "site" not in out.columns:
        raise ValueError("Očekávám sloupec 'site'.")
    digits = out["site"].astype(str).str.replace(r"\D", "", regex=True)
    has_ean = digits.str.len() >= 8
    out["site"] = np.where(has_ean, digits, out["site"].apply(_canonical_site_text))
    return out

# ------- binning O/D a pairing --------
def _sum_by_site_bin(df: pd.DataFrame, value_col: str = "value_kwh", freq: str = "H") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "site", value_col])
    tmp = df.copy()
    tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
    tmp = tmp.dropna(subset=["datetime"])
    tmp["tbin"] = tmp["datetime"].dt.floor(freq)
    out = (
        tmp.groupby(["tbin", "site"], as_index=False)[value_col]
           .sum()
           .rename(columns={"tbin":"datetime"})
           .sort_values(["datetime","site"])
           .reset_index(drop=True)
    )
    return out

def local_pairing(
    eano_long: pd.DataFrame,
    eand_long: pd.DataFrame,
    *,
    freq: str = "H",
    use_canonical: bool = True  # False = respektuj přesně 'site' (např. site_group ze 2. řádku hlaviček)
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    Oin = eano_long.copy()
    Din = eand_long.copy()
    if use_canonical:
        Oin = apply_site_key(Oin)
        Din = apply_site_key(Din)

    O = _sum_by_site_bin(Oin, value_col="value_kwh", freq=freq).rename(columns={"value_kwh":"cons_kwh"})
    D = _sum_by_site_bin(Din, value_col="value_kwh", freq=freq).rename(columns={"value_kwh":"prod_kwh"})

    df = pd.merge(O, D, on=["datetime","site"], how="outer").fillna(0.0)
    df["local_selfcons_kwh"] = np.minimum(df["cons_kwh"], df["prod_kwh"])
    df["import_after_kwh"]   = np.maximum(df["cons_kwh"] - df["prod_kwh"], 0.0)
    df["export_after_kwh"]   = np.maximum(df["prod_kwh"] - df["cons_kwh"], 0.0)

    eano_after = df[["datetime","site","import_after_kwh"]].copy()
    eand_after = df[["datetime","site","export_after_kwh"]].copy()
    local_self = df[["datetime","site","local_selfcons_kwh"]].copy()
    return eano_after, eand_after, local_self

# ------- Ekonomika z citlivostí (NPV/payback) -------
def econ_from_sensitivity(
    df: pd.DataFrame, *,
    discharge_col: str = "discharge_mwh",
    charge_shared_col: str = "charge_shared_mwh",
    cap_col: str = "cap_kwh",
    eq_cycles_col: str = "eq_cycles",
    price_comm_mwh: float,
    price_dist_mwh: float,
    price_feed_mwh: float,
    price_per_kwh: float,
    project_years: int,
    discount_rate: float,
    cycle_life: int
) -> pd.DataFrame:
    """
    Z df citlivosti (výkon baterie dle kapacity) spočti jednoduché NPV a payback.
    Očekávané jednotky:
      - discharge_mwh: MWh/rok pokrytého importu (dispečink vybíjení)
      - charge_shared_mwh: MWh/rok nabíjené ze sdílené elektřiny (přichází o feed-in výnos)
      - cap_kwh: kapacita baterie v kWh (pro CAPEX)
    Přidá sloupce: saved_kcz_year, capex_total_kcz, npv_kcz, simple_payback_years, eq_cycles.
    """
    df = df.copy()
    # ceny v Kč/kWh
    price_use_kwh  = (price_comm_mwh + price_dist_mwh) / 1000.0
    price_feed_kwh =  price_feed_mwh / 1000.0

    # vstupy a defaulty
    cap_kwh_vals = pd.to_numeric(df.get(cap_col, 0.0), errors="coerce").fillna(0.0)
    discharge_kwh = pd.to_numeric(df.get(discharge_col, 0.0), errors="coerce").fillna(0.0) * 1000.0
    charge_shared_kwh = pd.to_numeric(df.get(charge_shared_col, 0.0), errors="coerce").fillna(0.0) * 1000.0

    # roční cashflow: ušetřený nákup (komodita+distribuce) mínus ušlý výnos z přetoku při nabíjení sdílenou el.
    saved_kcz_year = discharge_kwh * price_use_kwh - charge_shared_kwh * price_feed_kwh

    # CAPEX
    capex_total_kcz = cap_kwh_vals * float(price_per_kwh)

    # NPV
    years = int(project_years)
    r = float(discount_rate)
    factor = ((1 - (1 + r) ** (-years)) / r) if r > 0 else years
    npv_kcz = saved_kcz_year * factor - capex_total_kcz

    # cykly (pokud nejsou dané, dopočti hrubě)
    if eq_cycles_col in df.columns:
        eq_cycles = pd.to_numeric(df[eq_cycles_col], errors="coerce").fillna(0.0)
    else:
        eq_cycles = (discharge_kwh / cap_kwh_vals.replace(0, np.nan)).fillna(0.0)

    # jednoduchá návratnost
    payback_years = np.where(saved_kcz_year > 0, capex_total_kcz / saved_kcz_year, np.inf)

    out = df.copy()
    out["saved_kcz_year"] = saved_kcz_year
    out["capex_total_kcz"] = capex_total_kcz
    out["npv_kcz"] = npv_kcz
    out["simple_payback_years"] = payback_years
    out["eq_cycles"] = eq_cycles
    return out
