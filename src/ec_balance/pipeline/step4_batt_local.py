# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from ..utils.sharing_lib import safe_to_csv

def simulate_local_battery(
    import_after: pd.DataFrame,
    export_after: pd.DataFrame,
    *,
    cap_kwh: float,
    eta_c: float = 0.95,
    eta_d: float = 0.95
) -> pd.DataFrame:
    """
    Greedy simulace na úrovni site, hodina po hodině.
    Robustní vůči pandas 2.x, duplicitám timestampů i chybějícím hodinám.
    """
    # sjednocená, seřazená časová osa
    all_times = pd.DatetimeIndex(
        pd.Index(import_after["datetime"]).union(pd.Index(export_after["datetime"]))
    ).sort_values()

    sites = sorted(set(import_after["site"]).union(set(export_after["site"])))
    rows = []

    for site in sites:
        # Agregace po datetime => unikátní index
        imp_s = (
            import_after.loc[import_after["site"] == site, ["datetime", "import_after_kwh"]]
            .groupby("datetime", as_index=True)["import_after_kwh"].sum()
            .reindex(all_times, fill_value=0.0)
        )
        exp_s = (
            export_after.loc[export_after["site"] == site, ["datetime", "export_after_kwh"]]
            .groupby("datetime", as_index=True)["export_after_kwh"].sum()
            .reindex(all_times, fill_value=0.0)
        )

        # Na jistotu: numerika → numpy vektory zarovnané s all_times
        imp_vals = pd.to_numeric(imp_s, errors="coerce").fillna(0.0).to_numpy(dtype=float)
        exp_vals = pd.to_numeric(exp_s, errors="coerce").fillna(0.0).to_numpy(dtype=float)

        soc = 0.0
        energy_out = 0.0
        energy_in_shared = 0.0  # zatím neevidujeme zdroj nabíjení

        for exp_t, imp_t in zip(exp_vals, imp_vals):
            # nabíjení z lokálního přebytku
            if cap_kwh > 0:
                space = cap_kwh - soc
                if space > 0:
                    charge = exp_t * eta_c
                    if charge > space:
                        charge = space
                    soc += charge

            # vybíjení do lokální potřeby
            if soc > 0 and imp_t > 0 and eta_d > 0:
                can_dis = soc * eta_d
                dis = imp_t if imp_t < can_dis else can_dis
                soc -= dis / eta_d
                energy_out += dis

        eq_cycles = energy_out / cap_kwh if cap_kwh and cap_kwh > 0 else 0.0
        rows.append({
            "site": site,
            "cap_kwh": float(cap_kwh),
            "discharge_mwh": energy_out / 1000.0,
            "charge_shared_mwh": energy_in_shared / 1000.0,
            "eq_cycles": eq_cycles
        })

    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser(description="Krok 4 – lokální baterie: citlivost")
    ap.add_argument("--eano_after_pv_csv", required=True)
    ap.add_argument("--eand_after_pv_csv", required=True)
    ap.add_argument("--kwp_csv", required=True)  # držíme CLI kompatibilitu (zatím nevyužito)
    ap.add_argument("--outdir", required=True)
    # ceny sem nezasahují; necháváme je jen jako povinné CLI kvůli návaznosti pipeline
    ap.add_argument("--price_commodity_mwh", type=float, required=True)
    ap.add_argument("--price_distribution_mwh", type=float, required=True)
    ap.add_argument("--price_feed_in_mwh", type=float, required=True)
    ap.add_argument("--eta_c", type=float, default=0.95)
    ap.add_argument("--eta_d", type=float, default=0.95)
    ap.add_argument("--cap_kwh_list", default="0,5,10,15")
    args = ap.parse_args()

    eano_after = pd.read_csv(args.eano_after_pv_csv, parse_dates=["datetime"]).sort_values(["datetime", "site"])
    eand_after = pd.read_csv(args.eand_after_pv_csv, parse_dates=["datetime"]).sort_values(["datetime", "site"])

    caps = [float(x) for x in str(args.cap_kwh_list).split(",") if str(x).strip()]
    out_rows = []
    for cap in caps:
        sim = simulate_local_battery(eano_after, eand_after, cap_kwh=cap, eta_c=args.eta_c, eta_d=args.eta_d)
        out_rows.append(sim)
    sens = pd.concat(out_rows, ignore_index=True)

    outroot = Path(args.outdir)
    safe_to_csv(sens, outroot, name="local_sensitivity")

if __name__ == "__main__":
    main()
