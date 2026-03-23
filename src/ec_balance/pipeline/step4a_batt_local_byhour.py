# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

# -*- coding: utf-8 -*-
"""
Krok 4a – By-hour lokální baterie s prioritou own→community (charge i discharge)
Vstupy:
  --eano_after_pv_csv   .\csv\eano_after_pv.csv    (datetime, site, import_after_kwh)
  --eand_after_pv_csv   .\csv\eand_after_pv.csv    (datetime, site, export_after_kwh)
  --kwp_csv             .\csv\kwp_by_site.csv      (site, kwp)  [volitelné: pro odvození kapacit]
  --outdir              cílová složka (např. .\csv)

Parametry kapacity:
  --cap_kwh_per_kwp     [kWh/kWp], default 1.0 (pokud není --fixed_cap_kwh ani --cap_by_site_csv)
  --fixed_cap_kwh       jedna stejná kapacita pro všechny (přebíjí výpočet z KWP)
  --cap_by_site_csv     CSV se dvěma sloupci: site,cap_kwh (přebíjí ostatní)

Účinnosti:
  --eta_c, --eta_d      default 0.95

Výstupy:
  by_hour_after_bat_local.csv        (agregát po hodinách; zahrnuje jasné charge/discharge sloupce + legacy aliasy)
  bat_local_by_site_hour.csv         (detail po site; zahrnuje jasné charge/discharge sloupce + legacy aliasy)
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

def _cap_map(kvp_df, cap_by_site_csv, fixed_cap, cap_per_kwp, site_col):
    if cap_by_site_csv:
        m = _read(cap_by_site_csv)
        m = m.rename(columns={m.columns[0]: site_col, m.columns[1]: "cap_kwh"})
        return {r[site_col]: float(r["cap_kwh"]) for _, r in m.iterrows()}
    if fixed_cap is not None:
        return None  # použijeme jednu hodnotu
    if kvp_df is not None and "kwp" in kvp_df.columns:
        return {r[site_col]: float(r["kwp"]) * float(cap_per_kwp) for _, r in kvp_df.iterrows()}
    return {}

def _read_constraints(path: str) -> dict:
    """
    CSV: site,allow_export_grid,allow_import_grid
    allow_* default = 1
    Robustní: když má soubor 1 sloupec (Excel CZ), zkusí sep=';'.
    """
    if not path:
        return {}
    m = pd.read_csv(path)
    if m.shape[1] == 1:
        m = pd.read_csv(path, sep=";")
    if m.shape[1] < 1:
        return {}
    m = m.rename(columns={m.columns[0]: "site"})
    if "allow_export_grid" not in m.columns:
        m["allow_export_grid"] = 1
    if "allow_import_grid" not in m.columns:
        m["allow_import_grid"] = 1
    out = {}
    for _, r in m.iterrows():
        s = str(r["site"]).strip()
        out[s] = {
            "allow_export_grid": int(r["allow_export_grid"]),
            "allow_import_grid": int(r["allow_import_grid"]),
        }
    return out

def main():
    ap = argparse.ArgumentParser(description="S4a by-hour lokální baterie, own→community")
    ap.add_argument("--eano_after_pv_csv", required=True)
    ap.add_argument("--eand_after_pv_csv", required=True)
    ap.add_argument("--kwp_csv", required=False)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--cap_kwh_per_kwp", type=float, default=1.0)
    ap.add_argument("--fixed_cap_kwh", type=float, default=None)
    ap.add_argument("--cap_by_site_csv", default=None)
    ap.add_argument("--constraints_csv", default="", help="CSV: site,allow_export_grid,allow_import_grid")
    ap.add_argument("--eta_c", type=float, default=0.95)
    ap.add_argument("--eta_d", type=float, default=0.95)
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    eano = _read(args.eano_after_pv_csv)
    eand = _read(args.eand_after_pv_csv)
    kwp  = _read(args.kwp_csv) if args.kwp_csv else None

    site_col = "site" if "site" in eano.columns else _find_col(eano, [], ["site","object","lokal"])
    dt = "datetime"
    imp_col = _find_col(eano, ["import_after_kwh","import_kwh"], ["import","residual"])
    exp_col = _find_col(eand, ["export_after_kwh","export_kwh"], ["export","residual"])

    imp = eano[[dt, site_col, imp_col]].rename(columns={imp_col: "imp"}).copy()
    exp = eand[[dt, site_col, exp_col]].rename(columns={exp_col: "exp"}).copy()

    imp = imp.groupby([dt, site_col], as_index=False)["imp"].sum()
    exp = exp.groupby([dt, site_col], as_index=False)["exp"].sum()

    sites = sorted(set(imp[site_col]).union(set(exp[site_col])))
    times = pd.Index(sorted(set(imp[dt]).union(set(exp[dt]))))

    # Kapacity per site
    cap_map = _cap_map(kwp, args.cap_by_site_csv, args.fixed_cap_kwh, args.cap_kwh_per_kwp, site_col)

    # state vectors
    soc   = {s: 0.0 for s in sites}
    cap_s = {s: (float(args.fixed_cap_kwh) if args.fixed_cap_kwh is not None else float(cap_map.get(s, 0.0))) for s in sites}

    # fast lookup per hour
    imp_by = {(r[dt], r[site_col]): float(r["imp"]) for _, r in imp.iterrows()}
    exp_by = {(r[dt], r[site_col]): float(r["exp"]) for _, r in exp.iterrows()}

    rows_site = []
    rows_agg  = []
    constraints = _read_constraints(args.constraints_csv)

    for t in times:
        # snapshot per hour
        imp_t = {s: imp_by.get((t, s), 0.0) for s in sites}
        exp_t = {s: exp_by.get((t, s), 0.0) for s in sites}
        spill_t = {s: 0.0 for s in sites}
        own_dis = {s: 0.0 for s in sites}
        sh_dis  = {s: 0.0 for s in sites}
        own_ch  = {s: 0.0 for s in sites}   # charge z vlastních přetoků (vstup do baterie)
        pool_ch = {s: 0.0 for s in sites}   # charge ze společného poolu přetoků (vstup do baterie)
        # 1) charge z vlastních přetoků
        for s in sites:
            room = max(0.0, cap_s[s] - soc[s])
            if room > 0 and exp_t[s] > 0:
                e_in = min(exp_t[s], room / args.eta_c)
                soc[s] += e_in * args.eta_c
                exp_t[s] -= e_in
                own_ch[s] += e_in
        # 2) discharge do vlastní spotřeby
        for s in sites:
            if imp_t[s] > 0 and soc[s] > 0:
                deliverable = soc[s] * args.eta_d
                d = min(imp_t[s], deliverable)
                own_dis[s] = d
                soc[s] -= d / args.eta_d
                # kritické: vlastní krytí musí snížit reziduální import v OM
                imp_t[s] = max(0.0, imp_t[s] - d)

        # --- constraints po baterce ---
        # allow_export_grid=0 => export do sítě musí být 0 a jde do spill
        for s, c in constraints.items():
            if s in exp_t and c.get("allow_export_grid", 1) == 0:
                spill_t[s] += float(exp_t[s])
                exp_t[s] = 0.0
            if s in imp_t and c.get("allow_import_grid", 1) == 0:
                imp_t[s] = 0.0

        # 3) charge ze společných přetoků (pool)
        pool_exp = sum(exp_t.values())
        if pool_exp > 1e-12:
            rooms = {s: max(0.0, cap_s[s] - soc[s]) / max(args.eta_c, 1e-9) for s in sites}
            total_room = sum(rooms.values())  # v kWh "na vstupu" do nabíjení
            if total_room > 1e-12:
                used = 0.0
                # nabíjení baterek z pool exportu
                for s in sites:
                    share = pool_exp * (rooms[s] / total_room) if total_room > 0 else 0.0
                    e_in = min(share, rooms[s])
                    if e_in > 0:
                        soc[s] += e_in * args.eta_c
                        pool_ch[s] += e_in
                        used += e_in
                # tohle je klíčové: export, který sežraly baterky, musí pryč z exp_t (reziduum do sítě)
                used = min(used, pool_exp)
                if used > 0:
                    ratio_left = max(0.0, 1.0 - used / pool_exp)
                    for s in sites:
                        exp_t[s] *= ratio_left
        # 4) discharge do komunity (pool importu)
        pool_imp = sum(imp_t.values())
        if pool_imp > 1e-12:
            deliverable = {s: soc[s] * args.eta_d for s in sites}
            total_deliv = sum(deliverable.values())
            if total_deliv > 1e-12:
                supplied = 0.0
                for s in sites:
                    take = pool_imp * (deliverable[s] / total_deliv) if total_deliv > 0 else 0.0
                    d = min(take, deliverable[s])
                    if d > 0:
                        # zamezit same-hour komunitní recirkulaci přes tutéž baterii
                        # (ve stejné hodině neumožnit zároveň charge z poolu a discharge do poolu)
                        if pool_ch[s] > 1e-12:
                            continue
                        sh_dis[s] = d
                        soc[s] -= d / args.eta_d
                        supplied += d
                # tohle je klíčové: import, který pokryly baterky, musí pryč z imp_t (reziduum ze sítě)
                supplied = min(supplied, pool_imp)
                if supplied > 0:
                    ratio_left = max(0.0, 1.0 - supplied / pool_imp)
                    for s in sites:
                        imp_t[s] *= ratio_left

        # zápis řádků
        for s in sites:
            rows_site.append({
                "datetime": t, site_col: s,
                # --- jasné nové sloupce (doporučené) ---
                "batt_charge_from_local_export_kwh": own_ch[s],
                "batt_charge_from_pool_export_kwh": pool_ch[s],
                "batt_charge_total_kwh": own_ch[s] + pool_ch[s],
                "batt_discharge_own_kwh": own_dis[s],
                "batt_discharge_shared_kwh": sh_dis[s],
                "batt_discharge_total_kwh": own_dis[s] + sh_dis[s],
                "soc_kwh": soc[s],
                "import_after_batt_kwh": imp_t[s],
                "export_after_batt_kwh": exp_t[s],
                "curtailment_after_batt_kwh": spill_t[s],
                "spill_kwh": spill_t[s],
                # --- legacy aliasy (zpětná kompatibilita UI/pipeline) ---
                # Pozor: historicky matoucí názvy *_stored_kwh ve skutečnosti reprezentují VYBITÍ.
                "own_stored_kwh": own_dis[s],
                "shared_stored_kwh": sh_dis[s],
            })
        rows_agg.append({
            "datetime": t,
            # --- jasné nové sloupce (doporučené) ---
            "batt_charge_from_local_export_kwh": sum(own_ch.values()),
            "batt_charge_from_pool_export_kwh": sum(pool_ch.values()),
            "batt_charge_total_kwh": sum(own_ch.values()) + sum(pool_ch.values()),
            "batt_discharge_own_kwh": sum(own_dis.values()),
            "batt_discharge_shared_kwh": sum(sh_dis.values()),
            "batt_discharge_total_kwh": sum(own_dis.values()) + sum(sh_dis.values()),
            "soc_kwh": sum(soc.values()),  # agregovaný SoC (součet přes baterie)
            "import_after_batt_kwh": sum(imp_t.values()),
            "export_after_batt_kwh": sum(exp_t.values()),
            "curtailment_after_batt_kwh": sum(spill_t.values()),
            # --- legacy aliasy (zpětná kompatibilita UI/pipeline) ---
            "own_stored_kwh": sum(own_dis.values()),
            "shared_stored_kwh": sum(sh_dis.values()),
            "batt_charge_kwh": sum(own_ch.values()) + sum(pool_ch.values()),
            "batt_discharge_kwh": sum(own_dis.values()) + sum(sh_dis.values()),
        })

    by_site = pd.DataFrame(rows_site).sort_values(["datetime", site_col])
    agg    = pd.DataFrame(rows_agg).sort_values("datetime")

    by_site.to_csv(outdir / "bat_local_by_site_hour.csv", index=False)
    agg.to_csv(outdir / "by_hour_after_bat_local.csv", index=False)
    print(f"[OK] {outdir / 'by_hour_after_bat_local.csv'}")
    print(f"[OK] {outdir / 'bat_local_by_site_hour.csv'}")

if __name__ == "__main__":
    main()
