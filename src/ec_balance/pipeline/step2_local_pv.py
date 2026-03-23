# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

import argparse
from pathlib import Path
import pandas as pd
from ..utils.sharing_lib import local_pairing, safe_to_csv

def _load_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["datetime"])

def _apply_site_map(df: pd.DataFrame, site_map: pd.DataFrame | None) -> pd.DataFrame:
    if site_map is None or site_map.empty:
        return df
    m = site_map.copy()
    if "site" not in m.columns or "site_group" not in m.columns:
        return df
    out = df.merge(m[["site","site_group"]], on="site", how="left")
    out["site"] = out["site_group"].fillna(out["site"])
    out = out.drop(columns=["site_group"], errors="ignore")
    return out

def _read_constraints(path: str | Path) -> pd.DataFrame:
    """
    Čte site_constraints.csv a normalizuje názvy sloupců.

    Podporované varianty:
      - site, allow_export_grid, allow_import_grid
      - site, allow_export, allow_charge_from_grid   (UI varianta)

    allow_* default = 1
    """
    if not path:
        return pd.DataFrame(columns=["site","allow_export_grid","allow_import_grid"])
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=["site","allow_export_grid","allow_import_grid"])
    m = pd.read_csv(p)
    # Excel/CZ někdy uloží vše do 1 sloupce se středníkem
    if m.shape[1] == 1:
        m = pd.read_csv(p, sep=";")
    if "site" not in m.columns and len(m.columns) >= 1:
        m = m.rename(columns={m.columns[0]: "site"})
    # UI názvy → interní názvy
    if "allow_export_grid" not in m.columns and "allow_export" in m.columns:
        m["allow_export_grid"] = m["allow_export"]
    if "allow_import_grid" not in m.columns and "allow_charge_from_grid" in m.columns:
        m["allow_import_grid"] = m["allow_charge_from_grid"]
    if "allow_export_grid" not in m.columns:
        m["allow_export_grid"] = 1
    if "allow_import_grid" not in m.columns:
        m["allow_import_grid"] = 1
    m["site"] = m["site"].astype(str).str.strip()
    for c in ["allow_export_grid","allow_import_grid"]:
        m[c] = pd.to_numeric(m[c], errors="coerce").fillna(1).astype(int)
    return m[["site","allow_export_grid","allow_import_grid"]]

def _apply_no_export(eand_after: pd.DataFrame, constraints: pd.DataFrame) -> pd.DataFrame:
    """
    Pokud allow_export_grid=0, vynuluje export_after_kwh a dopočte curtailed_kwh.
    """
    if constraints is None or constraints.empty:
        return eand_after
    if "export_after_kwh" not in eand_after.columns:
        return eand_after
    cons = constraints.set_index("site")
    allow = eand_after["site"].map(cons["allow_export_grid"]).fillna(1).astype(int)
    mask = allow.eq(0)
    if "curtailed_kwh" not in eand_after.columns:
        eand_after["curtailed_kwh"] = 0.0
    eand_after.loc[mask, "curtailed_kwh"] += pd.to_numeric(
        eand_after.loc[mask, "export_after_kwh"], errors="coerce"
    ).fillna(0.0)
    eand_after.loc[mask, "export_after_kwh"] = 0.0
    return eand_after

def main():
    ap = argparse.ArgumentParser(description="Krok 2 – lokální párování O↔D po objektu (site_group ze 2. řádku hlaviček)")
    ap.add_argument("--eano_long_csv", required=True)
    ap.add_argument("--eand_long_csv", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--pair_freq", default="H", help="časový bin pro párování: 'H', '30min', '15min', ...")
    ap.add_argument("--site_map_csv", default="", help="volitelně cesta k site_map.csv (jinak .\\csv\\site_map.csv)")
    ap.add_argument("--constraints_csv", default="", help="volitelně site_constraints.csv (site,allow_export_grid,allow_import_grid) – v kroku 2 se uplatní zákaz exportu (curtailment).")
    args = ap.parse_args()

    outroot = Path(args.outdir)
    eano_long = _load_csv(args.eano_long_csv)
    eand_long = _load_csv(args.eand_long_csv)

    # načti site_map vytvořený v kroku 1 z 2. řádku wide hlaviček
    sm_path = Path(args.site_map_csv) if args.site_map_csv else (outroot / "csv" / "site_map.csv")
    site_map = pd.read_csv(sm_path) if sm_path.exists() else None
    # constraints: buď explicitně přes --constraints_csv, nebo hledáme vedle outdir
    candidates = []
    if args.constraints_csv:
        candidates.append(Path(args.constraints_csv))
    else:
        candidates += [outroot / 'site_constraints.csv', outroot / 'csv' / 'site_constraints.csv']
    cons_path = next((p for p in candidates if p and p.exists()), Path(''))
    constraints = _read_constraints(cons_path)

    # přemapuj na site_group (název objektu ze 2. řádku)
    eano_long = _apply_site_map(eano_long, site_map)
    eand_long = _apply_site_map(eand_long, site_map)

    # pairing BEZ canonicalizace (respektuj přesně site_group z mapy)
    eano_after, eand_after, local_self = local_pairing(
        eano_long, eand_long, freq=args.pair_freq, use_canonical=False
    )

    # zákaz exportu => zmařená energie (curtailment)
    eand_after = _apply_no_export(eand_after, constraints)

    safe_to_csv(eano_after, outroot, name="eano_after_pv")
    safe_to_csv(eand_after, outroot, name="eand_after_pv")
    safe_to_csv(local_self, outroot, name="local_selfcons")

    sc_sum = float(pd.to_numeric(local_self["local_selfcons_kwh"], errors="coerce").fillna(0.0).sum())
    if sc_sum <= 0.0:
        print("[WARN] local_selfcons_kwh = 0. Zkontroluj:")
        print("  - že krok 1 vytvořil csv/site_map.csv ze 2. řádků hlaviček (a že sloupce O/D mají shodný text druhé řádky).")
        print("  - případně zkus jiný --pair_freq (např. '15min').")

if __name__ == "__main__":
    main()