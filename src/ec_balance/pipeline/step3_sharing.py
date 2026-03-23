# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

import argparse
from pathlib import Path
from typing import List, Tuple
import pandas as pd
import numpy as np

def _read_constraints(path: str) -> dict:
    """
    Načte constraints pro objekty (OM) a sjednotí názvy sloupců.

    Podporované varianty:
      - site, allow_export_grid, allow_import_grid
      - site, allow_export, allow_charge_from_grid   (UI varianta)

    allow_* default = 1
    """
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    m = pd.read_csv(p)
    # Excel/CZ někdy uloží vše do 1 sloupce se středníkem
    if m.shape[1] == 1:
        m = pd.read_csv(p, sep=";")
    if m.shape[1] < 1:
        return {}
    # první sloupec bereme jako site
    if "site" not in m.columns:
        m = m.rename(columns={m.columns[0]: "site"})
    m["site"] = m["site"].astype(str).str.strip()

    # UI názvy → interní názvy
    if "allow_export_grid" not in m.columns and "allow_export" in m.columns:
        m["allow_export_grid"] = m["allow_export"]
    if "allow_import_grid" not in m.columns and "allow_charge_from_grid" in m.columns:
        m["allow_import_grid"] = m["allow_charge_from_grid"]

    if "allow_export_grid" not in m.columns:
        m["allow_export_grid"] = 1
    if "allow_import_grid" not in m.columns:
        m["allow_import_grid"] = 1

    m["allow_export_grid"] = pd.to_numeric(m["allow_export_grid"], errors="coerce").fillna(1).astype(int)
    m["allow_import_grid"] = pd.to_numeric(m["allow_import_grid"], errors="coerce").fillna(1).astype(int)

    out = {}
    for _, r in m.iterrows():
        s = str(r["site"]).strip()
        out[s] = {
            "allow_export_grid": int(r["allow_export_grid"]),
            "allow_import_grid": int(r["allow_import_grid"]),
        }
    return out

def _apply_no_export_for_sharing(eand_after: pd.DataFrame, constraints: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    allow_export_grid=0 => export_after_kwh=0 (tím pádem ani sdílení).
    Vrací i hodinové součty curtailed_kwh (Series index=datetime).
    """
    if constraints is None or "export_after_kwh" not in eand_after.columns:
        return eand_after, pd.Series(dtype=float)
    # constraints může být DataFrame nebo dict
    if isinstance(constraints, dict):
        if len(constraints) == 0:
            return eand_after, pd.Series(dtype=float)
        def allow_export(site: str) -> int:
            v = constraints.get(site, 1)
            if isinstance(v, dict):
                return int(v.get("allow_export_grid", 1))
            return int(v)
    else:
        if getattr(constraints, "empty", False):
            return eand_after, pd.Series(dtype=float)
        cons = constraints.set_index("site")
        def allow_export(site: str) -> int:
            return int(cons["allow_export_grid"].get(site, 1))
    allow = eand_after["site"].astype(str).map(allow_export).fillna(1).astype(int)
    mask = allow.eq(0)
    curtailed = pd.to_numeric(eand_after.loc[mask, "export_after_kwh"], errors="coerce").fillna(0.0)
    # uložíme do sloupce, ať to jde agregovat i po site
    if "curtailed_kwh" not in eand_after.columns:
        eand_after["curtailed_kwh"] = 0.0
    eand_after.loc[mask, "curtailed_kwh"] += curtailed
    eand_after.loc[mask, "export_after_kwh"] = 0.0
    curtailed_by_hour = eand_after.groupby("datetime")["curtailed_kwh"].sum()
    return eand_after, curtailed_by_hour

def _apply_no_import_for_sharing(eano_after: pd.DataFrame, constraints) -> Tuple[pd.DataFrame, pd.Series]:
    """
    allow_import_grid=0 => import_after_kwh=0 (tím pádem ani PŘÍJEM sdílení).
    Vrací i hodinové součty blocked_import_kwh (Series index=datetime).
    """
    if constraints is None or "import_after_kwh" not in eano_after.columns:
        return eano_after, pd.Series(dtype=float)
    if isinstance(constraints, dict):
        if len(constraints) == 0:
            return eano_after, pd.Series(dtype=float)
        def allow_import(site: str) -> int:
            v = constraints.get(site, {})
            if isinstance(v, dict):
                return int(v.get("allow_import_grid", 1))
            return int(v)
    else:
        cons = constraints.set_index("site")
        def allow_import(site: str) -> int:
            return int(cons["allow_import_grid"].get(site, 1))

    allow = eano_after["site"].astype(str).map(allow_import).fillna(1).astype(int)
    mask = allow.eq(0)
    if "blocked_import_kwh" not in eano_after.columns:
        eano_after["blocked_import_kwh"] = 0.0
    blocked = pd.to_numeric(eano_after.loc[mask, "import_after_kwh"], errors="coerce").fillna(0.0)
    eano_after.loc[mask, "blocked_import_kwh"] += blocked
    eano_after.loc[mask, "import_after_kwh"] = 0.0
    blocked_by_hour = eano_after.groupby("datetime")["blocked_import_kwh"].sum()
    return eano_after, blocked_by_hour

# volitelně čteme safe_to_csv ze sharing_lib, ale máme i fallback
try:
    from ..utils.sharing_lib import safe_to_csv
except Exception:
    def safe_to_csv(df: pd.DataFrame, outdir: Path, name: str) -> Path:
        outdir = Path(outdir); (outdir / "csv").mkdir(parents=True, exist_ok=True)
        p = outdir / "csv" / f"{name}.csv"; df.to_csv(p, index=False); print(f"[OK] {name}: {p}"); return p

def _read(path: str, cols_required=None) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    if cols_required:
        missing = [c for c in cols_required if c not in df.columns]
        if missing:
            raise ValueError(f"{path} chybí­ sloupce: {missing}")
    return df


def _sum_by_hour_site(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime","site",value_col])
    out = (
        df.groupby(["datetime","site"], as_index=False)[value_col]
          .sum()
          .sort_values(["datetime","site"])
          .reset_index(drop=True)
    )
    return out

def share_pool_degree_limited(
    eano_after: pd.DataFrame,
    eand_after: pd.DataFrame,
    *,
    max_recipients_per_from: int = 5,
    exclude_self: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Proporční­ sdí­lení­ po hodinách s omezení­m počtu pří­jemců na zdroj."""
    # agregace (robustně vůči duplicitám)
    imp = _sum_by_hour_site(eano_after, "import_after_kwh")
    exp = _sum_by_hour_site(eand_after, "export_after_kwh")

    sites = sorted(set(imp["site"].unique()) | set(exp["site"].unique()))
    I = imp.pivot(index="datetime", columns="site", values="import_after_kwh").reindex(columns=sites, fill_value=0.0).fillna(0.0)
    E = exp.pivot(index="datetime", columns="site", values="export_after_kwh").reindex(columns=sites, fill_value=0.0).fillna(0.0)

    idx = I.index.union(E.index)
    I = I.reindex(idx, fill_value=0.0).astype(float)
    E = E.reindex(idx, fill_value=0.0).astype(float)

    alloc_rows: List[Tuple[pd.Timestamp, str, str, float]] = []
    resI_frames = []
    resE_frames = []

    for ts in idx:
        irow = I.loc[ts].astype(float)
        erow = E.loc[ts].astype(float)
        total_I = float(irow.sum()); total_E = float(erow.sum())
        if total_I <= 0 or total_E <= 0:
            resI_frames.append(irow.to_frame().T); resE_frames.append(erow.to_frame().T)
            continue

        shared = min(total_I, total_E)

        # cí­lové pokrytí­ a nabí­dka (proporční­ k popt./nabí­dce)
        imp_share = (irow / total_I).fillna(0.0)
        exp_share = (erow / total_E).fillna(0.0)
        desired_cover = shared * imp_share       # kolik by ideálně dostal každý to_site
        supply_from   = shared * exp_share       # kolik by ideálně poslal každý from_site

        remaining_cover = desired_cover.copy()
        remaining_supply = supply_from.copy()

        # iteruj zdroje od největší­ nabí­dky
        for s_from in remaining_supply.sort_values(ascending=False).index:
            s_supply = float(remaining_supply[s_from])
            if s_supply <= 1e-12:
                continue
            # kandidáti: největší­ zbývají­cí­ poptávka, volitelně bez self
            cand = remaining_cover.copy()
            if exclude_self and s_from in cand.index:
                cand = cand.drop(index=s_from)
            cand = cand[cand > 1e-12].sort_values(ascending=False)
            if cand.empty:
                continue
            selected = cand.head(max_recipients_per_from)
            sel_total = float(selected.sum())
            if sel_total <= 1e-12:
                continue
            # rozděl s_supply proporcionálně na vybrané destinace
            for s_to, rem in selected.items():
                alloc = float(s_supply * (rem / sel_total))
                if alloc <= 0:
                    continue
                alloc_rows.append((ts, s_from, s_to, alloc))
                remaining_cover[s_to] = max(0.0, float(remaining_cover[s_to] - alloc))
            remaining_supply[s_from] = 0.0

        covered_by_site = (desired_cover - remaining_cover).clip(lower=0.0)
        contributed_by_site = (supply_from - remaining_supply).clip(lower=0.0)

        resI_frames.append((irow - covered_by_site).to_frame().T)
        resE_frames.append((erow - contributed_by_site).to_frame().T)

    I_res = pd.concat(resI_frames).sort_index()
    E_res = pd.concat(resE_frames).sort_index()

    imp_wide = I_res.reset_index().rename(columns={"index": "datetime"})
    exp_wide = E_res.reset_index().rename(columns={"index": "datetime"})
    allocations = pd.DataFrame(alloc_rows, columns=["datetime","from_site","to_site","shared_kwh"]).sort_values(["datetime","from_site","to_site"]).reset_index(drop=True)

    # souhrny
    pre_I = I.sum(axis=0).rename("import_local_kwh").to_frame()
    pre_E = E.sum(axis=0).rename("export_local_kwh").to_frame()
    post_I = I_res.sum(axis=0).rename("import_residual_kwh").to_frame()
    post_E = E_res.sum(axis=0).rename("export_residual_kwh").to_frame()
    rec_in = allocations.groupby("to_site")["shared_kwh"].sum().rename("shared_in_kwh").to_frame()
    rec_out= allocations.groupby("from_site")["shared_kwh"].sum().rename("shared_out_kwh").to_frame()

    by_site_after = (
        pd.concat([pre_I, pre_E, post_I, post_E, rec_in, rec_out], axis=1)
          .fillna(0.0)
          .reset_index().rename(columns={"index":"site"})
    )

    by_hour_after = (
        pd.DataFrame({
            "datetime": I.index,
            "import_local_kwh": I.sum(axis=1).values,
            "export_local_kwh": E.sum(axis=1).values,
            "import_residual_kwh": I_res.sum(axis=1).values,
            "export_residual_kwh": E_res.sum(axis=1).values,
        })
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    return imp_wide, exp_wide, by_site_after, by_hour_after, allocations

def main():
    ap = argparse.ArgumentParser(description="Krok 3 â€“ sdí­lení­ v komunitě (pool, degree-limit)")
    ap.add_argument("--eano_after_pv_csv", required=True)
    ap.add_argument("--eand_after_pv_csv", required=True)
    ap.add_argument("--local_selfcons_csv", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--price_commodity_mwh", type=float, required=True)
    ap.add_argument("--price_distribution_mwh", type=float, required=True)
    ap.add_argument("--price_feed_in_mwh", type=float, required=True)
    ap.add_argument("--mode", choices=["hybrid","proportional"], default="hybrid")
    # aliasy: --max_receivers (původní­) i --max_recipients (nový)
    ap.add_argument("--max_receivers", type=int, default=None, help="Max počet pří­jemců na jeden zdroj v hodině (alias).")
    ap.add_argument("--max_recipients", type=int, default=None, help="Max počet pří­jemců na jeden zdroj v hodině (alias).")
    ap.add_argument("--allow_self_pair", action="store_true", help="Povolit alokaci na tentýž objekt (default: NE).")
    ap.add_argument("--site_map_csv", default="", help="(Kompatibilita CLI â€“ nevyužito zde)")
    ap.add_argument("--constraints_csv", default="", help="Volitelně site_constraints.csv – allow_export_grid=0 zakáže sdílení (a přidá curtailed_kwh).")
    args = ap.parse_args()

    # vyber hodnotu limitu z aliasů
    max_rec = args.max_recipients if args.max_recipients is not None else (args.max_receivers if args.max_receivers is not None else 5)

    # načti vstupy (local_self zatí­m nevyuží­váme pří­mo â€“ je jen meta)
    eano_after = _read(args.eano_after_pv_csv, cols_required=["datetime","site","import_after_kwh"])
    eand_after = _read(args.eand_after_pv_csv, cols_required=["datetime","site","export_after_kwh"])
    _ = _read(args.local_selfcons_csv)  # pro kontrolu existuje

    constraints = _read_constraints(args.constraints_csv)
    eand_after, curtailed_by_hour = _apply_no_export_for_sharing(eand_after, constraints)
    eano_after, blocked_import_by_hour = _apply_no_import_for_sharing(eano_after, constraints)

    imp_wide, exp_wide, by_site_after, by_hour_after, allocations = share_pool_degree_limited(
        eano_after, eand_after,
        max_recipients_per_from=max_rec,
        exclude_self=(not args.allow_self_pair),
    )

    # Přidej curtailed do výsledků (po site i po hodinách), aby to bylo vidět v reportech
    if "curtailed_kwh" in eand_after.columns:
        curtailed_by_site = eand_after.groupby("site")["curtailed_kwh"].sum()
        by_site_after = by_site_after.merge(
            curtailed_by_site.rename("curtailed_kwh").reset_index(),
            on="site", how="left"
        ).fillna({"curtailed_kwh": 0.0})
        # blocked import (allow_import_grid=0)
        if 'blocked_import_kwh' in eano_after.columns:
            blocked_by_site = eano_after.groupby('site')['blocked_import_kwh'].sum()
            by_site_after = by_site_after.merge(
                blocked_by_site.rename('blocked_import_kwh').reset_index(),
                on='site', how='left'
            ).fillna({'blocked_import_kwh': 0.0})
    if isinstance(curtailed_by_hour, pd.Series) and not curtailed_by_hour.empty:
        by_hour_after["curtailed_kwh"] = by_hour_after["datetime"].map(curtailed_by_hour).fillna(0.0)
 
        if isinstance(blocked_import_by_hour, pd.Series) and not blocked_import_by_hour.empty:
            by_hour_after['blocked_import_kwh'] = by_hour_after['datetime'].map(blocked_import_by_hour).fillna(0.0)
    # --- aplikace constraints (export/import do sítě) ---
    # constraints už máme načtené výše
    # spill_wide = export, který je zakázaný a "přeteče do nicoty/curtailment"
    spill_wide = exp_wide.copy()
    for c in [col for col in spill_wide.columns if col != "datetime"]:
        spill_wide[c] = 0.0

    for s, c in constraints.items():
        if s in exp_wide.columns and c.get("allow_export_grid", 1) == 0:
            spill_wide[s] = exp_wide[s]
            exp_wide[s] = 0.0
        if s in imp_wide.columns and c.get("allow_import_grid", 1) == 0:
            imp_wide[s] = 0.0
    outroot = Path(args.outdir)
    safe_to_csv(by_site_after, outroot, name="by_site_after")
    safe_to_csv(by_hour_after, outroot, name="by_hour_after")
    safe_to_csv(allocations, outroot, name="allocations")
    safe_to_csv(imp_wide, outroot, name="imp_wide")
    safe_to_csv(exp_wide, outroot, name="exp_wide")
    safe_to_csv(spill_wide, outroot, name="spill_wide")

    print(f"[OK] Sharing hotovo. Limit pří­jemců = {max_rec}, self_pair = {args.allow_self_pair}")

if __name__ == "__main__":
    main()