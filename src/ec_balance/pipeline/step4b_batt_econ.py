# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba


import argparse
from pathlib import Path
import math

import pandas as pd

try:
    import numpy as np
except Exception:
    class _NP:
        @staticmethod
        def irr(cfs):
            # jednoduchý bisekční odhad IRR
            lo, hi = -0.99, 1.0
            for _ in range(100):
                mid = (lo + hi) / 2
                npv = sum(cf / ((1+mid)**i) for i, cf in enumerate(cfs))
                if npv > 0:
                    lo = mid
                else:
                    hi = mid
            return (lo + hi) / 2
    np = _NP()

def _pick_first(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def _read_csv(path: Path | str) -> pd.DataFrame | None:
    p = Path(path)
    if not p.exists():
        return None
    return pd.read_csv(p)

def _sum_discharge_kwh(df: pd.DataFrame | None) -> float:
    if df is None or df.empty:
        return 0.0
    # zkuste různé názvy
    cand = [
        "consumption_from_storage_kwh",
        "own_stored_kwh",
        "shared_stored_kwh",
        "discharge_kwh",
        "energy_out_kwh",
    ]
    total = 0.0
    present = [c for c in cand if c in df.columns]
    if present:
        # když máme own+shared, obě sečteme
        if "own_stored_kwh" in df.columns and "shared_stored_kwh" in df.columns:
            total += pd.to_numeric(df["own_stored_kwh"], errors="coerce").fillna(0.0).sum()
            total += pd.to_numeric(df["shared_stored_kwh"], errors="coerce").fillna(0.0).sum()
            present = [c for c in present if c not in ("own_stored_kwh","shared_stored_kwh")]
        for c in present:
            total += pd.to_numeric(df[c], errors="coerce").fillna(0.0).sum()
        return float(total)
    # poslední záchrana: když je tam "discharge_mwh", převeď na kWh
    if "discharge_mwh" in df.columns:
        return float(pd.to_numeric(df["discharge_mwh"], errors="coerce").fillna(0.0).sum() * 1000.0)
    return 0.0

def _estimate_cap_kwh(by_hour_df: pd.DataFrame | None, meta_df: pd.DataFrame | None) -> float:
    # 1) zkuste meta (centrální bat: cap_kwh)
    if meta_df is not None and not meta_df.empty:
        for name in ("cap_kwh","capacity_kwh"):
            if name in meta_df.columns:
                v = pd.to_numeric(meta_df[name], errors="coerce").fillna(0.0).max()
                if v > 0:
                    return float(v)
    # 2) zkuste SOC maximum
    if by_hour_df is not None and not by_hour_df.empty:
        soc_col = _pick_first(by_hour_df, ["soc_kwh","soc_kwh_sum","state_of_charge"])
        if soc_col is not None:
            v = pd.to_numeric(by_hour_df[soc_col], errors="coerce").fillna(0.0).max()
            if v > 0:
                return float(v)
    return 0.0

def _npv(rate: float, cashflows: list[float]) -> float:
    return float(sum(cf / ((1+rate)**i) for i, cf in enumerate(cashflows)))

def _irr(cashflows: list[float]) -> float | None:
    try:
        return float(np.irr(cashflows))
    except Exception:
        return None

def _econ_summary(energy_shift_kwh: float,
                  cap_kwh: float,
                  price_commodity_mwh: float,
                  price_distribution_mwh: float,
                  price_feed_in_mwh: float,
                  price_per_kwh: float,
                  fixed_cost: float,
                  years: int,
                  discount: float) -> dict:
    # roční úspora v Kč: (commodity + distribution - feed-in) * (shift_kwh/1000)
    delta = (price_commodity_mwh + price_distribution_mwh - price_feed_in_mwh)
    annual = float(delta * (energy_shift_kwh / 1000.0))
    capex = float(price_per_kwh * cap_kwh + fixed_cost)
    cashflows = [-capex] + [annual] * years
    npv = _npv(discount, cashflows)
    irr = _irr(cashflows)
    payback = (capex / annual) if annual > 1e-9 else math.inf
    return dict(
        cap_kwh_est=cap_kwh,
        energy_shifted_mwh=energy_shift_kwh / 1000.0,
        annual_savings_kcz=annual,
        capex_kcz=capex,
        npv_kcz=npv,
        irr=irr if irr is not None else float("nan"),
        simple_payback_years=payback,
    )

def main():
    ap = argparse.ArgumentParser(description="Ekonomika baterek – robustní jednoduchý výpočet")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--csv_subdir", default="csv")
    ap.add_argument("--price_commodity_mwh", type=float, required=True)
    ap.add_argument("--price_distribution_mwh", type=float, required=True)
    ap.add_argument("--price_feed_in_mwh", type=float, required=True)
    ap.add_argument("--project_years", type=int, default=15)
    ap.add_argument("--discount_rate", type=float, default=0.05)
    ap.add_argument("--central_price_per_kwh", type=float, default=0.0)
    ap.add_argument("--central_fixed_cost", type=float, default=0.0)
    ap.add_argument("--local_price_per_kwh", type=float, default=0.0)
    ap.add_argument("--local_fixed_cost", type=float, default=0.0)
    args = ap.parse_args()

    outroot = Path(args.outdir)
    csvdir = outroot / args.csv_subdir

    # Vstupy
    bh_local = _read_csv(csvdir / "by_hour_after_bat_local.csv")
    bh_central = _read_csv(csvdir / "by_hour_after_bat_central.csv")
    meta_central = _read_csv(csvdir / "bat_central_meta.csv")  # volitelně

    # Energetika
    local_kwh = _sum_discharge_kwh(bh_local)
    central_kwh = _sum_discharge_kwh(bh_central)

    # Kapacity
    cap_local = _estimate_cap_kwh(bh_local, None)
    cap_central = _estimate_cap_kwh(bh_central, meta_central)

    # Ekonomika
    econ_local = _econ_summary(
        energy_shift_kwh=local_kwh, cap_kwh=cap_local,
        price_commodity_mwh=args.price_commodity_mwh,
        price_distribution_mwh=args.price_distribution_mwh,
        price_feed_in_mwh=args.price_feed_in_mwh,
        price_per_kwh=args.local_price_per_kwh,
        fixed_cost=args.local_fixed_cost,
        years=args.project_years, discount=args.discount_rate,
    )
    econ_central = _econ_summary(
        energy_shift_kwh=central_kwh, cap_kwh=cap_central,
        price_commodity_mwh=args.price_commodity_mwh,
        price_distribution_mwh=args.price_distribution_mwh,
        price_feed_in_mwh=args.price_feed_in_mwh,
        price_per_kwh=args.central_price_per_kwh,
        fixed_cost=args.central_fixed_cost,
        years=args.project_years, discount=args.discount_rate,
    )

    # Výstupy
    csvdir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([econ_local]).to_csv(csvdir / "local_econ_best.csv", index=False)
    pd.DataFrame([econ_central]).to_csv(csvdir / "central_econ_best.csv", index=False)
    print(f"[OK] local_econ_best.csv → {csvdir / 'local_econ_best.csv'}")
    print(f"[OK] central_econ_best.csv → {csvdir / 'central_econ_best.csv'}")

if __name__ == "__main__":
    main()
