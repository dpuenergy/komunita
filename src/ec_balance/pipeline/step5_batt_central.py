# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

import argparse
from pathlib import Path
import pandas as pd
from ..utils.sharing_lib import safe_to_csv

def simulate_central_battery(by_hour_after: pd.DataFrame, *, cap_kwh: float, eta_c: float = 0.95, eta_d: float = 0.95) -> pd.DataFrame:
    imp = by_hour_after.set_index("datetime")["import_residual_kwh"].fillna(0.0)
    exp = by_hour_after.set_index("datetime")["export_residual_kwh"].fillna(0.0)
    soc = 0.0
    energy_out = 0.0
    energy_in_shared = 0.0
    for t in imp.index:
        charge = min(exp.loc[t] * eta_c, cap_kwh - soc)
        soc += charge
        can_dis = soc * eta_d
        dis = min(imp.loc[t], can_dis)
        soc -= dis / max(eta_d, 1e-9)
        energy_out += dis
    eq_cycles = energy_out / cap_kwh if cap_kwh > 0 else 0.0
    return pd.DataFrame([{
        "cap_kwh": cap_kwh,
        "discharge_mwh": energy_out / 1000.0,
        "charge_shared_mwh": energy_in_shared / 1000.0,
        "eq_cycles": eq_cycles
    }])

def main():
    ap = argparse.ArgumentParser(description="Krok 5 – centrální baterie: citlivost")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--by_hour_csv", required=True)
    ap.add_argument("--kwp_csv", required=True)
    ap.add_argument("--eta_c", type=float, default=0.95)
    ap.add_argument("--eta_d", type=float, default=0.95)
    ap.add_argument("--cap_kwh_list", default="0,50,100,200")
    args = ap.parse_args()

    by_hour = pd.read_csv(args.by_hour_csv, parse_dates=["datetime"])
    caps = [float(x) for x in str(args.cap_kwh_list).split(",") if str(x).strip()]
    out_rows = []
    for cap in caps:
        sim = simulate_central_battery(by_hour, cap_kwh=cap, eta_c=args.eta_c, eta_d=args.eta_d)
        sim["site"] = "CENTRAL"
        out_rows.append(sim)
    sens = pd.concat(out_rows, ignore_index=True)

    outroot = Path(args.outdir)
    safe_to_csv(sens, outroot, name="central_sensitivity")

if __name__ == "__main__":
    main()
