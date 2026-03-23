# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

# -*- coding: utf-8 -*-
"""
Krok 5a – By-hour centrální baterie s prioritou own→community (charge i discharge)
Vstupy:
  --eano_after_pv_csv   .\csv\eano_after_pv.csv    (datetime, site, import_after_kwh)
  --eand_after_pv_csv   .\csv\eand_after_pv.csv    (datetime, site, export_after_kwh)
  --central_site        jméno site, kde je baterie
  --cap_kwh             kapacita baterie [kWh]
  --outdir              cílová složka (např. .\csv)
Parametry:
  --eta_c, --eta_d      default 0.95
Výstup:
  by_hour_after_bat_central.csv   (datetime, own_stored_kwh, shared_stored_kwh, soc_kwh)
"""
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

def _read(path):
    df = pd.read_csv(path)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.floor("h")
    return df

def _find_col(df, prefer, contains):
    for c in prefer:
        if c in df.columns: return c
    low = {c.lower(): c for c in df.columns}
    for key in contains:
        for lc, orig in low.items():
            if key in lc: return orig
    raise KeyError(f"Sloupec {prefer} / ~{contains} nenalezen")

def main():
    ap = argparse.ArgumentParser(description="S5a by-hour centrální baterie, own→community")
    ap.add_argument("--eano_after_pv_csv", required=True)
    ap.add_argument("--eand_after_pv_csv", required=True)
    ap.add_argument("--central_site", required=True)
    ap.add_argument("--cap_kwh", type=float, required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--eta_c", type=float, default=0.95)
    ap.add_argument("--eta_d", type=float, default=0.95)
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    eano = _read(args.eano_after_pv_csv)
    eand = _read(args.eand_after_pv_csv)

    site_col = "site" if "site" in eano.columns else _find_col(eano, [], ["site","object","lokal"])
    dt = "datetime"
    imp_col = _find_col(eano, ["import_after_kwh","import_kwh"], ["import","residual"])
    exp_col = _find_col(eand, ["export_after_kwh","export_kwh"], ["export","residual"])

    imp = eano[[dt, site_col, imp_col]].rename(columns={imp_col: "imp"}).copy()
    exp = eand[[dt, site_col, exp_col]].rename(columns={exp_col: "exp"}).copy()
    imp = imp.groupby([dt, site_col], as_index=False)["imp"].sum()
    exp = exp.groupby([dt, site_col], as_index=False)["exp"].sum()

    times = pd.Index(sorted(set(imp[dt]).union(set(exp[dt]))))
    sites = sorted(set(imp[site_col]).union(set(exp[site_col])))

    if args.central_site not in sites:
        raise SystemExit(f"--central_site '{args.central_site}' není v datech (sites: {sorted(sites)[:6]}...)")

    imp_by = {(r[dt], r[site_col]): float(r["imp"]) for _, r in imp.iterrows()}
    exp_by = {(r[dt], r[site_col]): float(r["exp"]) for _, r in exp.iterrows()}

    soc = 0.0; cap = float(args.cap_kwh)
    rows = []

    for t in times:
        imp_c = imp_by.get((t, args.central_site), 0.0)
        exp_c = exp_by.get((t, args.central_site), 0.0)
        # ostatní komunita (bez centra)
        imp_o = sum(imp_by.get((t, s), 0.0) for s in sites if s != args.central_site)
        exp_o = sum(exp_by.get((t, s), 0.0) for s in sites if s != args.central_site)

        own_dis = 0.0
        sh_dis  = 0.0

        # 1) charge z vlastní výroby (centrální site)
        room = max(0.0, cap - soc)
        if room > 0 and exp_c > 0:
            e_in = min(exp_c, room / args.eta_c)
            soc += e_in * args.eta_c
            exp_c -= e_in

        # 2) discharge do vlastní spotřeby (centrální site)
        if imp_c > 0 and soc > 0:
            deliverable = soc * args.eta_d
            d = min(imp_c, deliverable)
            own_dis = d
            soc -= d / args.eta_d
            imp_c -= d

        # 3) charge z komunity (přebytek ostatních + zbytek vlastního přebytku)
        pool_exp = exp_o + exp_c
        room = max(0.0, cap - soc)
        if pool_exp > 1e-12 and room > 1e-12:
            e_in = min(pool_exp, room / args.eta_c)
            soc += e_in * args.eta_c
            pool_exp -= e_in

        # 4) discharge do komunity (deficity ostatních)
        pool_imp = imp_o
        if pool_imp > 1e-12 and soc > 1e-12:
            deliverable = soc * args.eta_d
            d = min(pool_imp, deliverable)
            sh_dis = d
            soc -= d / args.eta_d

        rows.append({
            "datetime": t,
            "own_stored_kwh": own_dis,
            "shared_stored_kwh": sh_dis,
            "soc_kwh": soc,
        })

    out = pd.DataFrame(rows).sort_values("datetime")
    out.to_csv(outdir / "by_hour_after_bat_central.csv", index=False)
    print(f"[OK] {outdir / 'by_hour_after_bat_central.csv'}")

    # meta info pro ekonomiku a metriky
    pd.DataFrame([{"central_site": args.central_site, "cap_kwh": float(cap)}]).to_csv(outdir / "bat_central_meta.csv", index=False)
    print(f"[OK] {outdir / 'bat_central_meta.csv'}")

if __name__ == "__main__":
    main()
