# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

# -*- coding: utf-8 -*-
"""
Krok 7 – Site hourly report + heatmapy sdílení

Vstupy (typicky v csv_dir/csv):
  ean_o_long.csv             (datetime, site, value_kwh)
  ean_d_long.csv             (datetime, site, value_kwh)
  local_selfcons.csv         (datetime, site, local_selfcons_kwh)  [název sloupce se detekuje]
  allocations.csv            (datetime, from_site, to_site, shared_kwh)
  imp_wide.csv               (datetime, <site1>, <site2>, ...)
  exp_wide.csv               (datetime, <site1>, <site2>, ...)
  bat_local_by_site_hour.csv (datetime, site, own_stored_kwh, shared_stored_kwh, soc_kwh) [volitelně]
  allocations_batt_local.csv (datetime, from_site, to_site, shared_kwh) [volitelně pro heatmap_with_batt]

Výstupy (v outdir/csv nebo outdir):
  site_hourly_no_batt.csv
  site_hourly_with_batt.csv           (jen když existuje bat_local_by_site_hour.csv)
  heatmap_no_batt.csv                 (pivot from_site × to_site)
  heatmap_with_batt.csv               (step3 + batt allocations, pokud existují)
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

# volitelně čteme safe_to_csv ze sharing_lib, ale máme i fallback
try:
    from ..utils.sharing_lib import safe_to_csv
except Exception:
    def safe_to_csv(df: pd.DataFrame, outdir: Path, name: str) -> Path:
        outdir = Path(outdir)
        (outdir / "csv").mkdir(parents=True, exist_ok=True)
        p = outdir / "csv" / f"{name}.csv"
        df.to_csv(p, index=False)
        print(f"[OK] {name}: {p}")
        return p


def _read_csv(path: Path, parse_dt=True) -> pd.DataFrame:
    df = pd.read_csv(path)
    if parse_dt and "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.floor("h")
    return df


def _find_col(df: pd.DataFrame, prefer: list[str], contains: list[str]) -> str:
    for c in prefer:
        if c in df.columns:
            return c
    low = {c.lower(): c for c in df.columns}
    for key in contains:
        for lc, orig in low.items():
            if key in lc:
                return orig
    raise KeyError(f"Sloupec {prefer} / ~{contains} nenalezen v {list(df.columns)}")


def _sum_value_by_hour_site(df: pd.DataFrame, value_col: str, site_col="site") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", site_col, value_col])
    out = (
        df.groupby(["datetime", site_col], as_index=False)[value_col]
          .sum()
          .sort_values(["datetime", site_col])
          .reset_index(drop=True)
    )
    return out


def _wide_to_long_sites(df_wide: pd.DataFrame, value_name: str) -> pd.DataFrame:
    # očekává: datetime + sloupce site
    if df_wide.empty:
        return pd.DataFrame(columns=["datetime", "site", value_name])

    cols = [c for c in df_wide.columns if c != "datetime"]
    out = df_wide.melt(id_vars=["datetime"], value_vars=cols, var_name="site", value_name=value_name)
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce").dt.floor("h")
    out[value_name] = pd.to_numeric(out[value_name], errors="coerce").fillna(0.0)
    out = out.dropna(subset=["datetime"]).sort_values(["datetime", "site"]).reset_index(drop=True)
    return out


def _alloc_to_hourly_in_out(alloc: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Z allocations udělá hourly sum: shared_in_kwh (to_site) a shared_out_kwh (from_site)."""
    if alloc is None or alloc.empty:
        empty = pd.DataFrame(columns=["datetime", "site", "shared_in_kwh"])
        empty2 = pd.DataFrame(columns=["datetime", "site", "shared_out_kwh"])
        return empty, empty2

    req = {"datetime", "from_site", "to_site"}
    if not req.issubset(set(alloc.columns)):
        raise ValueError(f"allocations musí mít {req}, má {set(alloc.columns)}")
    val_col = "shared_kwh" if "shared_kwh" in alloc.columns else _find_col(alloc, [], ["shared"])

    alloc = alloc.copy()
    alloc["datetime"] = pd.to_datetime(alloc["datetime"], errors="coerce").dt.floor("h")
    alloc[val_col] = pd.to_numeric(alloc[val_col], errors="coerce").fillna(0.0)
    alloc = alloc.dropna(subset=["datetime"])

    shared_in = (
        alloc.groupby(["datetime", "to_site"], as_index=False)[val_col]
            .sum()
            .rename(columns={"to_site": "site", val_col: "shared_received_kwh"})
            .sort_values(["datetime", "site"])
            .reset_index(drop=True)
    )
    shared_out = (
        alloc.groupby(["datetime", "from_site"], as_index=False)[val_col]
            .sum()
            .rename(columns={"from_site": "site", val_col: "shared_sent_kwh"})
            .sort_values(["datetime", "site"])
            .reset_index(drop=True)
    )
    return shared_in, shared_out


def _build_heatmap(alloc: pd.DataFrame) -> pd.DataFrame:
    if alloc is None or alloc.empty:
        return pd.DataFrame()

    val_col = "shared_kwh" if "shared_kwh" in alloc.columns else _find_col(alloc, [], ["shared"])
    a = alloc.copy()
    a["datetime"] = pd.to_datetime(a["datetime"], errors="coerce")
    a[val_col] = pd.to_numeric(a[val_col], errors="coerce").fillna(0.0)

    hm = (
        a.groupby(["from_site", "to_site"], as_index=False)[val_col]
         .sum()
         .pivot(index="from_site", columns="to_site", values=val_col)
         .fillna(0.0)
         .reset_index()
    )
    # hezčí: seřaď sloupce podle abecedy (kromě from_site)
    cols = ["from_site"] + sorted([c for c in hm.columns if c != "from_site"])
    return hm[cols]


def _merge_hourly(base: pd.DataFrame, add: pd.DataFrame) -> pd.DataFrame:
    """Left merge na (datetime, site)."""
    if add is None or add.empty:
        return base
    return base.merge(add, on=["datetime", "site"], how="left")


def main():
    ap = argparse.ArgumentParser(description="Krok 7 – hourly report per site + heatmapy sdílení")
    ap.add_argument("--csv_dir", required=True, help="Složka s csv výstupy (typicky ...\\src\\csv nebo jen 'csv').")
    ap.add_argument("--outdir", required=True, help="Kam ukládat výstupy (typicky stejně jako csv_dir).")

    # volitelné explicitní cesty – pokud nezadáš, zkusí najít v csv_dir
    ap.add_argument("--ean_o_long_csv", default="")
    ap.add_argument("--ean_d_long_csv", default="")
    ap.add_argument("--local_selfcons_csv", default="")
    ap.add_argument("--allocations_csv", default="")
    ap.add_argument("--imp_wide_csv", default="")
    ap.add_argument("--exp_wide_csv", default="")
    ap.add_argument("--bat_local_by_site_hour_csv", default="")
    ap.add_argument("--allocations_batt_local_csv", default="")

    args = ap.parse_args()

    csv_dir = Path(args.csv_dir)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    def pick(explicit: str, default_name: str) -> Path:
        if explicit:
            return Path(explicit)
        return csv_dir / default_name

    p_o = pick(args.ean_o_long_csv, "ean_o_long.csv")
    p_d = pick(args.ean_d_long_csv, "ean_d_long.csv")
    p_sc = pick(args.local_selfcons_csv, "local_selfcons.csv")
    p_alloc = pick(args.allocations_csv, "allocations.csv")
    p_impw = pick(args.imp_wide_csv, "imp_wide.csv")
    p_expw = pick(args.exp_wide_csv, "exp_wide.csv")
    p_bat = pick(args.bat_local_by_site_hour_csv, "bat_local_by_site_hour.csv")
    p_alloc_bat = pick(args.allocations_batt_local_csv, "allocations_batt_local.csv")

    # --- načti základ ---
    if not p_o.exists():
        raise FileNotFoundError(f"Chybí {p_o} (čekám ean_o_long.csv z kroku 1)")
    if not p_d.exists():
        raise FileNotFoundError(f"Chybí {p_d} (čekám ean_d_long.csv z kroku 1)")
    if not p_sc.exists():
        raise FileNotFoundError(f"Chybí {p_sc} (čekám local_selfcons.csv z kroku 2)")
    if not p_alloc.exists():
        raise FileNotFoundError(f"Chybí {p_alloc} (čekám allocations.csv z kroku 3)")
    if not p_impw.exists():
        raise FileNotFoundError(f"Chybí {p_impw} (čekám imp_wide.csv z kroku 3)")
    if not p_expw.exists():
        raise FileNotFoundError(f"Chybí {p_expw} (čekám exp_wide.csv z kroku 3)")

    eano = _read_csv(p_o)
    eand = _read_csv(p_d)
    selfc = _read_csv(p_sc)
    alloc = _read_csv(p_alloc)
    imp_w = _read_csv(p_impw)
    exp_w = _read_csv(p_expw)

    # spotřeba / výroba
    o_val = _find_col(eano, ["value_kwh"], ["value", "kwh"])
    d_val = _find_col(eand, ["value_kwh"], ["value", "kwh"])
    cons = _sum_value_by_hour_site(eano, o_val).rename(columns={o_val: "consumption_kwh"})
    prod = _sum_value_by_hour_site(eand, d_val).rename(columns={d_val: "pv_production_kwh"})

    # selfcons
    sc_val = "local_selfcons_kwh" if "local_selfcons_kwh" in selfc.columns else _find_col(selfc, [], ["selfcons", "self", "local"])
    selfc = selfc.copy()
    selfc["datetime"] = pd.to_datetime(selfc["datetime"], errors="coerce").dt.floor("h")
    selfc[sc_val] = pd.to_numeric(selfc[sc_val], errors="coerce").fillna(0.0)
    selfc_h = (
        selfc.groupby(["datetime", "site"], as_index=False)[sc_val]
             .sum()
             .rename(columns={sc_val: "self_pv_consumption_kwh"})
             .sort_values(["datetime", "site"])
             .reset_index(drop=True)
    )

    # allocations in/out
    shared_in, shared_out = _alloc_to_hourly_in_out(alloc)

    # import/export ze sítě po sdílení
    imp_h = _wide_to_long_sites(imp_w, "import_grid_kwh")
    exp_h = _wide_to_long_sites(exp_w, "export_grid_kwh")

    # sjednocení indexu (datetime, site)
    # poskládáme "base grid" ze všech tabulek
    sites = sorted(
        set(cons["site"]).union(set(prod["site"]))
        .union(set(selfc_h["site"]))
        .union(set(shared_in["site"]))
        .union(set(shared_out["site"]))
        .union(set(imp_h["site"]))
        .union(set(exp_h["site"]))
    )
    times = pd.Index(sorted(
        set(cons["datetime"]).union(set(prod["datetime"]))
        .union(set(selfc_h["datetime"]))
        .union(set(shared_in["datetime"]))
        .union(set(shared_out["datetime"]))
        .union(set(imp_h["datetime"]))
        .union(set(exp_h["datetime"]))
    ))

    base = pd.MultiIndex.from_product([times, sites], names=["datetime", "site"]).to_frame(index=False)

    # merge všechno
    out_no = base.copy()
    for part in [cons, prod, selfc_h, shared_in, shared_out, imp_h, exp_h]:
        out_no = _merge_hourly(out_no, part)

    # doplň nuly
    for c in [
        "consumption_kwh", "pv_production_kwh", "self_pv_consumption_kwh",
        "shared_received_kwh", "shared_sent_kwh",
        "import_grid_kwh", "export_grid_kwh",
    ]:
        if c not in out_no.columns:
            out_no[c] = 0.0
        out_no[c] = pd.to_numeric(out_no[c], errors="coerce").fillna(0.0)

    # "přetok do sítě bez využití" – v režimu bez baterky je to prostě export_grid_kwh (po sdílení)
    out_no["spill_kwh"] = out_no["export_grid_kwh"]

    out_no = out_no.sort_values(["datetime", "site"]).reset_index(drop=True)
    safe_to_csv(out_no, outdir, "site_hourly_no_batt")

    # --- heatmap bez baterek ---
    hm_no = _build_heatmap(alloc)
    safe_to_csv(hm_no, outdir, "heatmap_no_batt")

    # --- varianta s baterkou (pokud existuje bat_local_by_site_hour.csv) ---
    if p_bat.exists():
        bat = _read_csv(p_bat)
        bat_cols = set(bat.columns)
        # normalize columns
        if "site" not in bat.columns:
            # fallback: najdi "site" podobné
            site_col = _find_col(bat, ["site"], ["site", "obj", "lokal"])
            bat = bat.rename(columns={site_col: "site"})
        # expected: own_stored_kwh, shared_stored_kwh, soc_kwh
        own_col = "own_stored_kwh" if "own_stored_kwh" in bat.columns else _find_col(bat, [], ["own_stored"])
        sh_col  = "shared_stored_kwh" if "shared_stored_kwh" in bat.columns else _find_col(bat, [], ["shared_stored"])
        soc_col = "soc_kwh" if "soc_kwh" in bat.columns else _find_col(bat, [], ["soc"])

        has_imp_after = "import_after_batt_kwh" in bat_cols
        has_exp_after = "export_after_batt_kwh" in bat_cols

        bat_h = bat[["datetime", "site", own_col, sh_col, soc_col]].copy()
        bat_h = bat_h.rename(columns={
            own_col: "bat_discharge_own_kwh",
            sh_col: "bat_discharge_shared_kwh",
            soc_col: "bat_soc_kwh",
        })
        for c in ["bat_discharge_own_kwh", "bat_discharge_shared_kwh", "bat_soc_kwh"]:
            bat_h[c] = pd.to_numeric(bat_h[c], errors="coerce").fillna(0.0)

        out_w = out_no.drop(columns=["spill_kwh"], errors="ignore").copy()
        out_w = _merge_hourly(out_w, bat_h)
        for c in ["bat_discharge_own_kwh", "bat_discharge_shared_kwh", "bat_soc_kwh"]:
            if c not in out_w.columns:
                out_w[c] = 0.0
            out_w[c] = pd.to_numeric(out_w[c], errors="coerce").fillna(0.0)

        # Pokud step4a dodává import/export po baterce, použijeme je pro variantu with_batt.
        # (site_hourly_no_batt zůstává po kroku 3)
        if has_imp_after or has_exp_after:
            cols_take = ["datetime", "site"]
            if has_imp_after:
                cols_take.append("import_after_batt_kwh")
            if has_exp_after:
                cols_take.append("export_after_batt_kwh")

            bat_ie = bat[cols_take].copy()
            bat_ie["datetime"] = pd.to_datetime(bat_ie["datetime"], errors="coerce").dt.floor("h")
            if has_imp_after:
                bat_ie["import_after_batt_kwh"] = pd.to_numeric(bat_ie["import_after_batt_kwh"], errors="coerce").fillna(0.0)
            if has_exp_after:
                bat_ie["export_after_batt_kwh"] = pd.to_numeric(bat_ie["export_after_batt_kwh"], errors="coerce").fillna(0.0)

            out_w = _merge_hourly(out_w, bat_ie)
            # přepiš import/export ve with_batt na "po baterce"
            if has_imp_after:
                out_w["import_grid_kwh"] = out_w["import_after_batt_kwh"]
            if has_exp_after:
                out_w["export_grid_kwh"] = out_w["export_after_batt_kwh"]

        # spill = export do sítě (dokud nemáme constraint "no export", spill==export)
        out_w["spill_kwh"] = out_w["export_grid_kwh"]

        out_w = out_w.sort_values(["datetime", "site"]).reset_index(drop=True)
        safe_to_csv(out_w, outdir, "site_hourly_with_batt")
    else:
        print(f"[WARN] {p_bat} nenalezen – přeskočeno site_hourly_with_batt.csv")

    # --- heatmap s baterkami: pokud existuje allocations_batt_local.csv, přičteme ---
    if p_alloc_bat.exists():
        alloc_bat = _read_csv(p_alloc_bat)
        # sjednotit na shared_kwh
        if "shared_kwh" not in alloc_bat.columns:
            val = _find_col(alloc_bat, [], ["shared"])
            alloc_bat = alloc_bat.rename(columns={val: "shared_kwh"})
        # concat a heatmap
        alloc_all = pd.concat([alloc[["datetime", "from_site", "to_site", "shared_kwh"]], alloc_bat[["datetime", "from_site", "to_site", "shared_kwh"]]],
                              ignore_index=True)
        hm_w = _build_heatmap(alloc_all)
        safe_to_csv(hm_w, outdir, "heatmap_with_batt")
    else:
        # fallback: stejná jako bez baterek, ale řekneme pravdu
        print(f"[WARN] {p_alloc_bat} nenalezen – heatmap_with_batt bude stejné jako bez baterek.")
        safe_to_csv(hm_no, outdir, "heatmap_with_batt")


if __name__ == "__main__":
    main()
