# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

# -*- coding: utf-8 -*-
"""
Krok 1 â€“ wide â†’ long (fix datetime, site_map, kwp_by_site)
- Explicitní­ řádky: --site_row_file 2, --kwp_row_file 3 (1-based v SOUBORU).
- Pokud oba zadáĹˇ, autodetekce se NEpoužije.
- Výstupy: csv/ean_o_long.csv, csv/ean_d_long.csv, csv/site_map.csv, csv/kwp_by_site.csv (z EAN_D)
"""

import argparse
from pathlib import Path
from typing import Tuple, Dict, Optional
import numpy as np
import pandas as pd
import re

def _detect_sep(sample_path: str) -> str:
    try:
        with open(sample_path, "r", encoding="utf-8", errors="ignore") as f:
            head = f.readline() + f.readline()
    except Exception:
        return ","
    # ví­c střední­ků než čárek => ; jinak ,
    return ";" if head.count(";") > head.count(",") else ","
from ..utils.sharing_lib import safe_to_csv

def _is_numberlike(x) -> bool:
    try:
        float(str(x).replace(",", "."))
        return True
    except Exception:
        return False

def _auto_detect_rows(df_head: pd.DataFrame) -> Tuple[Optional[int], Optional[int]]:
    """Heuristika (POUZE když nejsou dány řádky): z první­ch 5 řádků najdi pravděpodobný řádek 'site' a 'kwp'.
       Pozor: tady NEurčujeme datovou řádku, to děláme až na plném DF."""
    site_idx = None
    kwp_idx = None
    look_rows = min(5, len(df_head))
    ncols = df_head.shape[1]
    for i in range(look_rows):
        row = df_head.iloc[i, 1:ncols]  # bez 1. sloupce (čas)
        vals = row.fillna("").astype(str).tolist()
        num_frac = sum(_is_numberlike(v) and str(v).strip() != "" for v in vals) / max(1, len(vals))
        alpha_frac = sum(any(ch.isalpha() for ch in v) for v in vals) / max(1, len(vals))
        if site_idx is None and alpha_frac >= 0.6:
            site_idx = i
        if kwp_idx is None and num_frac >= 0.6:
            kwp_idx = i
    return site_idx, kwp_idx

def _read_wide(path: str, sep: str | None = None, site_row_file: Optional[int] = None, kwp_row_file: Optional[int] = None
) -> Tuple[pd.DataFrame, Dict[str, str], Dict[str, float], int]:
    """Načti wide a vraĹĄ (df_data, ean->site, ean->kwp, first_data_row_1based)."""
    if sep in (None, '', 'auto'):
        sep = _detect_sep(path)
    df = pd.read_csv(path, sep=sep)
    if df.empty:
        raise ValueError(f"Soubor je prázdný: {path}")
    
    # --- CLEANUP: vyhoď prázdné sloupce a bordel z exportu (;;;;) ---
    # Pandas často vytvoří "Unnamed: X" pro prázdné hlavičky / extra středníky.
    # První sloupec necháváme vždy (čas může být klidně "Unnamed: 0").
    df.columns = pd.Index([str(c).replace("\ufeff", "").strip() for c in df.columns])

    # Vyhoď prázdné / "Unnamed" hlavičky (krom 1. sloupce)
    col_s = df.columns.astype(str)
    drop_mask = (
        col_s.str.match(r"^\s*$") |                        # úplně prázdné názvy
        col_s.str.match(r"(?i)^(unnamed)[:\s]") |          # Unnamed: X / Unnamed  X
        col_s.str.lower().isin(["nan", "none"])            # občas vznikne z prázdna
    )
    drop_mask = np.asarray(drop_mask)
    if drop_mask.size > 0:
        drop_mask[0] = False  # první sloupec nech vždycky
    df = df.loc[:, ~drop_mask]

    # prázdné řádky pryč
    df = df.dropna(how="all")

    # prázdné buňky jako NaN -> umožní spolehlivě vyhodit úplně prázdné sloupce
    df = df.replace(r"^\s*$", np.nan, regex=True)

    # vyhoď sloupce, které jsou úplně prázdné (krom 1. sloupce)
    if df.shape[1] > 1:
        df = pd.concat([df.iloc[:, [0]], df.iloc[:, 1:].dropna(axis=1, how="all")], axis=1)
 
    # 3) sjednoť duplicitní EANy: EAN_O_18 a EAN_O_18.1 → sečti do EAN_O_18
    #    (děláme jen pro EAN_* sloupce, ať nerozbijeme datetime apod.)
    
    cols = list(df.columns)
    groups = {}
    for c in cols:
        cs = str(c)
        if not cs.startswith("EAN_"):
            continue
        base = re.sub(r"\.\d+$", "", cs)  # odstraň .1, .2...
        groups.setdefault(base, []).append(cs)
    for base, members in groups.items():
        if len(members) <= 1:
            continue
        # sečti duplicitní sloupce (robustně jako čísla)
        tmp = pd.DataFrame({
            m: pd.to_numeric(df[m], errors="coerce").fillna(0.0) for m in members
        })
        df[base] = tmp.sum(axis=1)
        # nech jen base, ostatní zahodit
        for m in members:
            if m != base and m in df.columns:
                df.drop(columns=m, inplace=True)

    def file_row_to_idx(n: Optional[int]) -> Optional[int]:
        # Header je 'řádek 1' souboru; první­ řádek POD ní­m má index 0.
        return None if n is None else max(0, int(n) - 2)

    # 1) Zí­skej kandidáty na site/kwp řádky
    site_idx = file_row_to_idx(site_row_file)
    kwp_idx  = file_row_to_idx(kwp_row_file)
    if site_idx is None or kwp_idx is None:
        auto_site, auto_kwp = _auto_detect_rows(df.head(5))
        if site_idx is None: site_idx = auto_site
        if kwp_idx  is None: kwp_idx  = auto_kwp

    # 2) Najdi první­ datovou řádku ve FULL df (první­ validní­ datetime v 1. sloupci)
    data_idx = None
    for i in range(len(df)):
        ts = pd.to_datetime(df.iloc[i, 0], errors="coerce", dayfirst=True)
        if pd.notna(ts):
            data_idx = i
            break
    if data_idx is None:
        raise ValueError("V 1. sloupci nebyl nalezen žádný datum/čas â€“ zkontroluj CSV a --wide_sep.")

    # 3) Mapování­ ean->site/kwp (jen z řádků NAD daty)
    time_col = df.columns[0]
    ean_cols = [c for c in df.columns if c != time_col]
    ean_to_site: Dict[str, str] = {}
    ean_to_kwp: Dict[str, float] = {}

    if site_idx is not None and 0 <= site_idx < data_idx:
        for c in ean_cols:
            v = df.iloc[site_idx, df.columns.get_loc(c)]
            ean_to_site[c] = str(v).strip() if pd.notna(v) and str(v).strip() != "" else str(c)
    else:
        # fallback: použij jména sloupců (EANNNNNâ€¦)
        for c in ean_cols:
            ean_to_site[c] = str(c)

    if kwp_idx is not None and 0 <= kwp_idx < data_idx:
        for c in ean_cols:
            v = df.iloc[kwp_idx, df.columns.get_loc(c)]
            try:
                ean_to_kwp[c] = float(str(v).replace(",", "."))
            except Exception:
                pass  # OK, některé můžou chybět

    # 4) Datová část
    body = df.iloc[data_idx:, :].reset_index(drop=True).copy()
    body.rename(columns={time_col: "datetime"}, inplace=True)
    body["datetime"] = pd.to_datetime(body["datetime"], errors="coerce", dayfirst=True)

    return body, ean_to_site, ean_to_kwp, data_idx + 1  # jako 1-based "file row"

def _wide_to_long(df_wide: pd.DataFrame, ean_to_site: Dict[str, str], units: str = "kwh") -> pd.DataFrame:
    # pojistka: nikdy netahej do LONGu "Unnamed"/prázdné hlavičky
    value_cols = []
    for c in df_wide.columns:
        if c == "datetime":
            continue
        cs = str(c).strip()
        if cs == "" or cs.lower() in ("nan", "none"):
            continue
        if re.match(r"(?i)^(unnamed)[:\s]", cs):
            continue
        value_cols.append(c)
    df = df_wide.melt(id_vars=["datetime"], value_vars=value_cols, var_name="ean", value_name="value")
    df["site"] = df["ean"].map(ean_to_site).fillna(df["ean"])
    df["value"] = pd.to_numeric(df["value"].astype(str).str.replace(",", "."), errors="coerce").fillna(0.0)
    df["value_kwh"] = df["value"] * (1000.0 if units.lower() == "mwh" else 1.0)
    return df[["datetime", "site", "ean", "value_kwh"]].dropna(subset=["datetime"]).sort_values(
        ["datetime", "site", "ean"]
    ).reset_index(drop=True)

def _build_kwp_by_site(ean_to_site: Dict[str, str], ean_to_kwp: Dict[str, float]) -> pd.DataFrame:
    rows = [{"site": ean_to_site.get(e, e), "ean": e, "kwp": ean_to_kwp.get(e, np.nan)} for e in ean_to_site]
    return pd.DataFrame(rows).groupby("site", as_index=False)["kwp"].sum(min_count=1)

def main():
    ap = argparse.ArgumentParser(description="Krok 1 â€“ wide â†’ long (site_map + kwp_by_site).")
    ap.add_argument("--eano_wide", required=True)
    ap.add_argument("--eand_wide", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--wide_sep", default="auto")
    ap.add_argument("--site_row_file", type=int, default=None, help="1-based řádek se jmény site (typ. 2).")
    ap.add_argument("--kwp_row_file", type=int, default=None, help="1-based řádek s kWp (typ. 3).")
    ap.add_argument("--units", choices=["kwh", "mwh"], default="kwh")
    args = ap.parse_args()

    outroot = Path(args.outdir)

    # EANo (spotřeba) NEMÁ kWp řádek → nepoužívat kwp_row_file
    o_body, o_site_map, _o_kwp, o_row = _read_wide(
        args.eano_wide,
        sep=args.wide_sep,
        site_row_file=args.site_row_file,
        kwp_row_file=0,
    )
    # EANd (výroba) může mít kWp řádek (typicky 3) → použij args.kwp_row_file
    d_body, d_site_map, d_kwp, d_row = _read_wide(
        args.eand_wide,
        sep=args.wide_sep,
        site_row_file=args.site_row_file,
        kwp_row_file=args.kwp_row_file,
    )

    ean_o_long = _wide_to_long(o_body, o_site_map, units=args.units)
    ean_d_long = _wide_to_long(d_body, d_site_map, units=args.units)

    safe_to_csv(ean_o_long, outroot, name="ean_o_long")
    safe_to_csv(ean_d_long, outroot, name="ean_d_long")

    rows = []
    seen = set()
    for e, s in d_site_map.items():
        rows.append({"ean": e, "site": s}); seen.add(e)
    for e, s in o_site_map.items():
        if e not in seen:
            rows.append({"ean": e, "site": s})
    site_map = pd.DataFrame(rows)
    safe_to_csv(site_map, outroot, name="site_map")

    if d_kwp:
        kwp_by_site = _build_kwp_by_site(d_site_map, d_kwp)
        safe_to_csv(kwp_by_site, outroot, name="kwp_by_site")

    print(f"[OK] ean_o_long uložen (data start: file row {o_row})")
    print(f"[OK] ean_d_long uložen (data start: file row {d_row})")
    print(f"[OK] site_map: {len(site_map)} záznamů")
    if d_kwp:
        print(f"[OK] kwp_by_site: {len(kwp_by_site)} site")

if __name__ == "__main__":
    main()

