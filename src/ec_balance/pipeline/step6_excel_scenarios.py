# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

# -*- coding: utf-8 -*-
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

# jednotné sloupce pro by_hour
REQ_SCHEMA = [
    "datetime",
    "consumption",
    "import",
    "pv_production",
    "self_pv_consumption",
    "shared_sent_kwh",
    "shared_received_kwh",
    "own_pv_stored_kwh",
    "shared_pv_stored_kwh",
    "consumption_from_storage_kwh",
    "export",
    "cost_import_kcz",
    "cost_shared_dist_kcz",
    "revenue_export_kcz",
    "total_cost_kcz",
    "saving_sharing_kcz",
]

# ----------------- I/O pomocníci -----------------
def _load(csvdir: Path, name: str, parse_dt=True):
    p = csvdir / f"{name}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    if parse_dt and "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.floor("h")
    return df

def _sum_hour(df: pd.DataFrame, col: str, new: str):
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", new])
    x = df.copy()
    x["datetime"] = pd.to_datetime(x["datetime"], errors="coerce").dt.floor("h")
    x = x.groupby("datetime", as_index=False)[col].sum().rename(columns={col: new})
    return x.sort_values("datetime")

def _merge_time(a: pd.DataFrame | None, b: pd.DataFrame | None):
    if a is None or len(a) == 0:
        return b
    if b is None or len(b) == 0:
        return a
    a = a.copy()
    b = b.copy()
    a["datetime"] = pd.to_datetime(a["datetime"]).dt.floor("h")
    b["datetime"] = pd.to_datetime(b["datetime"]).dt.floor("h")
    out = pd.merge(a, b, on="datetime", how="outer")
    return out.sort_values("datetime").reset_index(drop=True)

def _ensure_schema(df: pd.DataFrame | None):
    if df is None:
        return pd.DataFrame(columns=REQ_SCHEMA)
    out = df.copy()
    for c in REQ_SCHEMA:
        if c not in out.columns:
            out[c] = 0.0 if c != "datetime" else pd.NaT
    return out[REQ_SCHEMA].sort_values("datetime").reset_index(drop=True)

# ----------------- Scénáře S1–S3 -----------------
def build_s1(ean_o_long, p_com_mwh, p_dist_mwh):
    cons = _sum_hour(ean_o_long, "value_kwh", "consumption")
    base = cons.copy()
    base["import"] = base["consumption"]
    base["pv_production"] = 0.0
    base["self_pv_consumption"] = 0.0
    base["export"] = 0.0
    base["shared_received_kwh"] = 0.0
    base["shared_sent_kwh"] = 0.0
    base["own_pv_stored_kwh"] = 0.0
    base["shared_pv_stored_kwh"] = 0.0
    base["consumption_from_storage_kwh"] = 0.0
    k_use = (p_com_mwh + p_dist_mwh) / 1000.0
    base["cost_import_kcz"] = base["import"] * k_use
    base["cost_shared_dist_kcz"] = 0.0
    base["revenue_export_kcz"] = 0.0
    base["total_cost_kcz"] = base["cost_import_kcz"]
    base["saving_sharing_kcz"] = 0.0
    return _ensure_schema(base)

def build_s2(eano_after_pv, eand_after_pv, local_self, p_com_mwh, p_dist_mwh, p_feed_mwh):
    imp = _sum_hour(eano_after_pv, "import_after_kwh", "import")
    exp = _sum_hour(eand_after_pv, "export_after_kwh", "export")
    selfc = _sum_hour(local_self, "local_selfcons_kwh", "self_pv_consumption")
    base = _merge_time(_merge_time(imp, exp), selfc)
    if base is None:
        base = pd.DataFrame(columns=["datetime"])
    base = base.fillna(0.0)
    
    # mapování názvů sloupců na standard 'import'
    if "import" not in base.columns:
        for cand in ["import_after_kwh", "import_kwh", "import_grid_kwh"]:
            if cand in base.columns:
                base["import"] = base[cand]
                break

    base["consumption"] = base["import"] + base["self_pv_consumption"]
    base["pv_production"] = base["self_pv_consumption"] + base["export"]
    base["shared_received_kwh"] = 0.0
    base["shared_sent_kwh"] = 0.0
    base["own_pv_stored_kwh"] = 0.0
    base["shared_pv_stored_kwh"] = 0.0
    base["consumption_from_storage_kwh"] = 0.0
    k_use = (p_com_mwh + p_dist_mwh) / 1000.0
    k_feed = p_feed_mwh / 1000.0
    base["cost_import_kcz"] = base["import"] * k_use
    base["cost_shared_dist_kcz"] = 0.0
    base["revenue_export_kcz"] = base["export"] * k_feed
    base["total_cost_kcz"] = base["cost_import_kcz"] - base["revenue_export_kcz"]
    base["saving_sharing_kcz"] = 0.0
    return _ensure_schema(base)

def build_s3(by_hour_after, allocations, ean_o_long, ean_d_long, local_self, p_com_mwh, p_dist_mwh, p_feed_mwh):
    if by_hour_after is None or by_hour_after.empty:
        raise ValueError("Chybí by_hour_after.csv (krok 3).")
    df = by_hour_after.rename(columns={
        "import_residual_kwh": "import",
        "export_residual_kwh": "export"
    })[["datetime", "import", "export"]].copy()
    selfc = _sum_hour(local_self, "local_selfcons_kwh", "self_pv_consumption")
    cons = _sum_hour(ean_o_long, "value_kwh", "consumption")
    prod = _sum_hour(ean_d_long, "value_kwh", "pv_production")
    df = _merge_time(df, selfc)
    df = _merge_time(df, cons)
    df = _merge_time(df, prod)
    df = df.fillna(0.0)
    if allocations is not None and not allocations.empty:
        sh = allocations.groupby("datetime", as_index=False)["shared_kwh"].sum()
        df = _merge_time(df, sh.rename(columns={"shared_kwh": "shared_received_kwh"}))
        df = _merge_time(df, sh.rename(columns={"shared_kwh": "shared_sent_kwh"}))
    else:
        df["shared_received_kwh"] = 0.0
        df["shared_sent_kwh"] = 0.0
    df["own_pv_stored_kwh"] = 0.0
    df["shared_pv_stored_kwh"] = 0.0
    df["consumption_from_storage_kwh"] = 0.0
    k_com = p_com_mwh / 1000.0
    k_dist = p_dist_mwh / 1000.0
    k_feed = p_feed_mwh / 1000.0
    df["cost_import_kcz"] = df["import"] * (k_com + k_dist)
    df["cost_shared_dist_kcz"] = df["shared_received_kwh"] * k_dist
    df["revenue_export_kcz"] = df["export"] * k_feed
    df["total_cost_kcz"] = df["cost_import_kcz"] + df["cost_shared_dist_kcz"] - df["revenue_export_kcz"]
    df["saving_sharing_kcz"] = df["shared_received_kwh"] * k_com
    return _ensure_schema(df)

# ----------------- Baterky: loadery & metriky -----------------
def _sum_cols(df, like_any, exclude_any=None):
    if exclude_any is None:
        exclude_any = []
    cols = []
    low = [c.lower() for c in df.columns]
    for i, c in enumerate(df.columns):
        lc = low[i]
        if any(k in lc for k in like_any) and not any(k in lc for k in exclude_any) and c != "datetime":
            cols.append(c)
    if not cols:
        return pd.Series(0.0, index=df.index)
    return df[cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).sum(axis=1)

def _pick_col(df, prefer):
    """Vyber první existující sloupec podle pořadí preferencí nebo heuristiky."""
    for c in prefer:
        if c in df.columns:
            return c
    low_map = {c.lower(): c for c in df.columns}
    for key in prefer:
        k = key.lower()
        for lc, orig in low_map.items():
            if all(tok in lc for tok in k.split("_")):
                return orig
    return None

def _load_battery_flows(path):
    """
    Vrátí DF s ['datetime','import','export'] (+ volitelně 'shared_received_kwh'])
    z by_hour_after_bat_*.csv, když jsou k dispozici.
    """
    p = Path(path)
    if not p.exists():
        return None
    df = pd.read_csv(p)
    if "datetime" not in df.columns or df.empty:
        return None
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.floor("h")
    df = df.dropna(subset=["datetime"])

    imp_col = _pick_col(df, [
        "import_after_bat_kwh","import_after_batt_kwh",
        "import_residual_after_bat","import_residual_kwh_after_bat",
        "import_after","import_residual","import"
    ])
    exp_col = _pick_col(df, [
        "export_after_bat_kwh","export_after_batt_kwh",
        "export_residual_after_bat","export_residual_kwh_after_bat",
        "export_after","export_residual","export"
    ])
    sh_col = _pick_col(df, [
        "shared_received_after_bat_kwh","shared_received_kwh_after_bat",
        "shared_received_after","shared_in_after",
        "shared_received_kwh","shared_in_kwh"
    ])

    cols = ["datetime"]
    if imp_col: cols.append(imp_col)
    if exp_col: cols.append(exp_col)
    if sh_col:  cols.append(sh_col)
    if len(cols) == 1:
        return None

    x = df[cols].copy()
    if imp_col: x.rename(columns={imp_col: "import"}, inplace=True)
    if exp_col: x.rename(columns={exp_col: "export"}, inplace=True)
    if sh_col:  x.rename(columns={sh_col: "shared_received_kwh"}, inplace=True)

    for c in ["import","export","shared_received_kwh"]:
        if c in x.columns:
            x[c] = pd.to_numeric(x[c], errors="coerce").fillna(0.0)
    return x.sort_values("datetime").reset_index(drop=True)

def _load_battery_by_hour(path, kind, eta_c=0.95, eta_d=0.95):
    """
    Vrátí: ['datetime','soc_kwh','own_pv_stored_kwh','shared_pv_stored_kwh','consumption_from_storage_kwh'].
    Název vstupů: own_stored_kwh, shared_stored_kwh, soc_kwh (resp. state_of_charge).
    Když chybí discharge, dopočte se ze SOC: soc_t = soc_{t-1} + eta_c*charge - discharge/eta_d.
    """
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        print(f"[WARN] Battery CSV nenalezeno: {p}")
        return None
    df = pd.read_csv(p)
    if df.empty or "datetime" not in df.columns:
        return None

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.floor("h")
    df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)

    # mapování názvů
    rename_map = {}
    if "own_stored_kwh" in df.columns:
        rename_map["own_stored_kwh"] = "own_pv_stored_kwh"
    if "shared_stored_kwh" in df.columns:
        rename_map["shared_stored_kwh"] = "shared_pv_stored_kwh"
    if "state_of_charge" in df.columns and "soc_kwh" not in df.columns:
        rename_map["state_of_charge"] = "soc_kwh"
    df = df.rename(columns=rename_map)

    def pick_like(prefer, fallback_contains):
        col = _pick_col(df, prefer)
        if col:
            return col
        for c in df.columns:
            lc = c.lower()
            if all(tok in lc for tok in fallback_contains):
                return c
        return None

    own_col = pick_like(["own_pv_stored_kwh","own_stored_kwh"], ["own","stored","kwh"])
    sh_col  = pick_like(["shared_pv_stored_kwh","shared_stored_kwh"], ["shared","stored","kwh"])
    soc_col = pick_like(["soc_kwh","state_of_charge"], ["soc"])

    out = pd.DataFrame({"datetime": df["datetime"]})
    out["own_pv_stored_kwh"] = pd.to_numeric(df[own_col], errors="coerce").fillna(0.0) if own_col else 0.0
    out["shared_pv_stored_kwh"] = pd.to_numeric(df[sh_col], errors="coerce").fillna(0.0) if sh_col else 0.0
    out["soc_kwh"] = pd.to_numeric(df[soc_col], errors="coerce").fillna(0.0) if soc_col else 0.0

    # discharge/consumption_from_storage_kwh
    disc_col = _pick_col(df, ["consumption_from_storage_kwh","discharged_kwh","discharge_kwh"])
    if disc_col:
        out["consumption_from_storage_kwh"] = pd.to_numeric(df[disc_col], errors="coerce").fillna(0.0)
    else:
        charge = out["own_pv_stored_kwh"] + out["shared_pv_stored_kwh"]
        soc = out["soc_kwh"]
        prev_soc = soc.shift(1).fillna(soc.iloc[0] if len(soc) else 0.0)
        raw = eta_d * (prev_soc + eta_c * charge - soc)
        out["consumption_from_storage_kwh"] = raw.clip(lower=0.0)

    out = out.groupby("datetime", as_index=False).sum().sort_values("datetime").reset_index(drop=True)
    return out

def _load_local_caps(csvdir: Path):
    p = csvdir / "bat_local_cap_by_site.csv"
    if p.exists():
        df = pd.read_csv(p)
        if {"site", "cap_kwh"}.issubset(df.columns):
            return float(pd.to_numeric(df["cap_kwh"], errors="coerce").fillna(0.0).sum())
    return None

def _load_central_meta(csvdir: Path):
    p = csvdir / "bat_central_meta.csv"
    if p.exists():
        df = pd.read_csv(p)
        if {"central_site", "cap_kwh"}.issubset(df.columns):
            return df.iloc[0]["central_site"], float(df.iloc[0]["cap_kwh"])
    return None, None

def battery_metrics(bh: pd.DataFrame | None, cap_total_kwh: float | None, eta_d=0.95):
    if bh is None or bh.empty:
        return {
            "efc": 0.0,
            "cycles_per_year": 0.0,
            "median_cycle_h": 0.0,
            "lifetime_years_at_5000": np.inf,
            "capacity_factor": 0.0,
        }
    discharge = pd.to_numeric(bh["consumption_from_storage_kwh"], errors="coerce").fillna(0.0)
    cap = float(cap_total_kwh or 0.0)
    if cap <= 0:
        cap = float(pd.to_numeric(bh.get("soc_kwh", pd.Series([0.0]))).max() or 0.0)
        if cap <= 0:
            cap = 1e-9
    efc = float(discharge.sum() / max(1e-9, cap))
    hours = max(1, len(bh))
    cycles_per_year = efc * (8760.0 / hours)
    soc = pd.to_numeric(bh.get("soc_kwh", pd.Series(0.0)), errors="coerce").fillna(0.0).to_numpy()
    mins = []
    for i in range(1, len(soc) - 1):
        if soc[i] <= soc[i - 1] and soc[i] <= soc[i + 1]:
            mins.append(i)
    cycle_lengths = []
    for j in range(len(mins) - 1):
        d = mins[j + 1] - mins[j]
        if d > 0:
            cycle_lengths.append(d)
    median_cycle_h = float(np.median(cycle_lengths)) if cycle_lengths else 0.0
    lifetime_years = (5000.0 / cycles_per_year) if cycles_per_year > 0 else np.inf
    cap_factor = float(discharge.sum() / (cap * 8760.0)) if cap > 0 else 0.0
    return {
        "efc": efc,
        "cycles_per_year": cycles_per_year,
        "median_cycle_h": median_cycle_h,
        "lifetime_years_at_5000": lifetime_years,
        "capacity_factor": cap_factor,
    }

# ----------------- Profily & osy -----------------
def _profiles(df: pd.DataFrame | None):
    if df is None or df.empty:
        return None, None, None
    x = df.copy()
    x["datetime"] = pd.to_datetime(x["datetime"]).dt.floor("h")
    x["month"] = x["datetime"].dt.month
    x["dow"] = x["datetime"].dt.dayofweek
    x["hour"] = x["datetime"].dt.hour
    cols = [c for c in ["consumption", "import", "pv_production", "self_pv_consumption", "export",
                        "shared_received_kwh", "own_pv_stored_kwh", "shared_pv_stored_kwh"] if c in x.columns]
    day_tabs, week_tabs, month_tabs = {}, {}, {}
    for m in sorted(x["month"].unique()):
        xm = x.loc[x["month"] == m].copy()
        day = xm.groupby("hour", as_index=False)[cols].mean()
        day.insert(0, "month", m)
        xm.loc[:, "hweek"] = xm["dow"] * 24 + xm["hour"]
        wk = xm.groupby("hweek", as_index=False)[cols].mean()
        wk.insert(0, "month", m)
        ms = xm.groupby("month", as_index=False)[cols].sum()
        day_tabs[m] = day
        week_tabs[m] = wk
        month_tabs[m] = ms
    return (
        pd.concat(day_tabs.values(), ignore_index=True) if day_tabs else None,
        pd.concat(week_tabs.values(), ignore_index=True) if week_tabs else None,
        pd.concat(month_tabs.values(), ignore_index=True) if month_tabs else None,
    )

def _axes_max(*dfs, cols=None):
    m = 0.0
    for df in dfs:
        if df is None or (hasattr(df, "empty") and df.empty):
            continue
        for c in (cols or getattr(df, "columns", [])):
            if c in ("datetime", "month", "hour", "hweek"):
                continue
            if c in df.columns:
                v = pd.to_numeric(df[c], errors="coerce").fillna(0.0).max()
                m = max(m, float(v))
    return (0, m * 1.05 if m > 0 else 1.0)

# ----------------- Excel výstupy -----------------
def _write_dashboard_finance(writer, name, df, day, week, month, allocations=None, bat_df=None, bat_metrics=None):
    wb = writer.book

    # by_hour sheet
    df.to_excel(excel_writer=writer, sheet_name="by_hour", index=False)

    # Dashboard
    ws = wb.add_worksheet("Dashboard")
    cols = ["consumption", "pv_production", "import", "self_pv_consumption", "shared_received_kwh", "export", "total_cost_kcz"]
    cols = [c for c in cols if c in df.columns]
    totals = {c: float(pd.to_numeric(df[c], errors="coerce").fillna(0.0).sum()) for c in cols}
    pd.DataFrame({
        "Metric": [
            "Consumption (kWh)",
            "PV production (kWh)",
            "Import (kWh)",
            "Self PV (kWh)",
            "Shared received (kWh)",
            "Export (kWh)",
            "Total cost (kCZ)",
        ],
        "Value": [
            totals.get("consumption", 0.0),
            totals.get("pv_production", 0.0),
            totals.get("import", 0.0),
            totals.get("self_pv_consumption", 0.0),
            totals.get("shared_received_kwh", 0.0),
            totals.get("export", 0.0),
            totals.get("total_cost_kcz", 0.0),
        ],
    }).to_excel(excel_writer=writer, sheet_name="Dashboard", startrow=0, startcol=0, index=False)

    # line chart (flows)
    n = len(df)
    ymin, ymax = _axes_max(df, cols=["consumption", "import", "pv_production"])
    lc = wb.add_chart({"type": "line"})
    lc.set_title({"name": f"{name}: Hourly flows"})
    lc.set_x_axis({"name": "Time"})
    lc.set_y_axis({"name": "kWh", "min": ymin, "max": ymax})
    cats = ["by_hour", 1, 0, n, 0]
    for cname in ["consumption", "import", "pv_production"]:
        if cname in df.columns:
            ci = df.columns.get_loc(cname)
            lc.add_series({
                "name": ["by_hour", 0, ci],
                "categories": cats,
                "values": ["by_hour", 1, ci, n, ci],
            })
    lc.set_legend({"position": "bottom"})
    ws.insert_chart(1, 6, lc, {"x_scale": 1.6, "y_scale": 1.2})

    # shares pie
    pie_tbl = pd.DataFrame({
        "Part": ["Self PV", "Shared PV", "Grid"],
        "kWh": [
            float(pd.to_numeric(df.get("self_pv_consumption", pd.Series(0.0))).sum()),
            float(pd.to_numeric(df.get("shared_received_kwh", pd.Series(0.0))).sum()),
            float(pd.to_numeric(df.get("import", pd.Series(0.0))).sum()),
        ],
    })
    pie_tbl.to_excel(excel_writer=writer, sheet_name="Dashboard", startrow=16, startcol=0, index=False)
    pie = wb.add_chart({"type": "doughnut"})
    pie.set_title({"name": "Shares of consumption"})
    pie.add_series({"categories": ["Dashboard", 17, 0, 19, 0], "values": ["Dashboard", 17, 1, 19, 1]})
    pie.set_hole_size(50)
    ws.insert_chart(16, 6, pie, {"x_scale": 1.0, "y_scale": 1.0})

    # Finance
    fin = df[["datetime", "cost_import_kcz", "cost_shared_dist_kcz", "revenue_export_kcz", "total_cost_kcz"]].copy()
    fin.to_excel(excel_writer=writer, sheet_name="Finance", index=False)
    wsf = writer.sheets["Finance"]
    ch = wb.add_chart({"type": "column", "subtype": "stacked"})
    ch.set_title({"name": "Costs per hour (stacked)"})
    ch.set_x_axis({"name": "Time"})
    ch.set_y_axis({"name": "kCZ"})
    for c in ["cost_import_kcz", "cost_shared_dist_kcz"]:
        ci = fin.columns.get_loc(c)
        ch.add_series({
            "name": ["Finance", 0, ci],
            "categories": ["Finance", 1, 0, len(fin), 0],
            "values": ["Finance", 1, ci, len(fin), ci],
        })
    ch2 = wb.add_chart({"type": "column"})
    ch2.set_title({"name": "Revenue from export per hour"})
    ci_rev = fin.columns.get_loc("revenue_export_kcz")
    ch2.add_series({
        "name": ["Finance", 0, ci_rev],
        "categories": ["Finance", 1, 0, len(fin), 0],
        "values": ["Finance", 1, ci_rev, len(fin), ci_rev],
    })
    wsf.insert_chart(1, 8, ch, {"x_scale": 1.2, "y_scale": 1.0})
    wsf.insert_chart(20, 8, ch2, {"x_scale": 1.2, "y_scale": 1.0})

    # profily
    def _insert_profiles(writer_inner, day_df, week_df, month_df):
        if day_df is not None:
            day_df.to_excel(excel_writer=writer_inner, sheet_name="profiles_day", index=False)
        if week_df is not None:
            week_df.to_excel(excel_writer=writer_inner, sheet_name="profiles_week", index=False)
        if month_df is not None:
            month_df.to_excel(excel_writer=writer_inner, sheet_name="profiles_month", index=False)
    _insert_profiles(writer, day, week, month)

    # Battery sheet
    if bat_df is not None and not bat_df.empty and "soc_kwh" in bat_df.columns:
        bat_df.to_excel(excel_writer=writer, sheet_name="battery", index=False)
        wsb = writer.sheets["battery"]
        wbk = writer.book
        nbat = len(bat_df)
        chs = wbk.add_chart({"type": "line"})
        chs.set_title({"name": "State of charge (kWh)"})
        chs.add_series({
            "name": ["battery", 0, 1],
            "categories": ["battery", 1, 0, nbat, 0],
            "values": ["battery", 1, 1, nbat, 1],
        })
        wsb.insert_chart(1, 3, chs, {"x_scale": 1.4, "y_scale": 1.0})
        if bat_metrics:
            pd.DataFrame([bat_metrics]).to_excel(
                excel_writer=writer, sheet_name="battery", startrow=0, startcol=4, index=False
            )

def _scenario_totals(name, df):
    s = df.sum(numeric_only=True)
    d = {"scenario": name}
    for k in [
        "consumption","pv_production","import","self_pv_consumption","shared_received_kwh","export",
        "cost_import_kcz","cost_shared_dist_kcz","revenue_export_kcz","total_cost_kcz","saving_sharing_kcz",
        "own_pv_stored_kwh","shared_pv_stored_kwh","consumption_from_storage_kwh",
    ]:
        d[k] = float(s.get(k, 0.0))
    return d

def _write_summary(outdir: Path, s_dict: dict, econ=None):
    df = pd.DataFrame([_scenario_totals(k, v) for k, v in s_dict.items()])
    p = outdir / "scenarios_summary.xlsx"
    with pd.ExcelWriter(p, engine="xlsxwriter") as xw:
        df.to_excel(excel_writer=xw, sheet_name="Summary", index=False)
        ws = xw.sheets["Summary"]; wb = xw.book
        n = len(df)

        ch = wb.add_chart({"type": "column"})
        ch.set_title({"name": "Total cost by scenario"})
        ci = df.columns.get_loc("total_cost_kcz")
        ch.add_series({
            "name": "total_cost_kcz",
            "categories": ["Summary", 1, 0, n, 0],
            "values": ["Summary", 1, ci, n, ci],
        })
        ws.insert_chart(1, 14, ch)

        ch2 = wb.add_chart({"type": "column", "subtype": "stacked"})
        ch2.set_title({"name": "Consumption coverage"})
        for c in ["self_pv_consumption", "shared_received_kwh", "import"]:
            if c in df.columns:
                ci = df.columns.get_loc(c)
                ch2.add_series({
                    "name": ["Summary", 0, ci],
                    "categories": ["Summary", 1, 0, n, 0],
                    "values": ["Summary", 1, ci, n, ci],
                })
        ws.insert_chart(18, 14, ch2)

        if econ is not None:
            econ.to_excel(excel_writer=xw, sheet_name="Economy", index=False)
    print(f"[OK] Summary → {p}")

# ----------------- Ekonomika -----------------
def discounted_cashflow(cashflows, rate):
    fac = np.array([(1.0 / (1.0 + rate) ** t) for t in range(len(cashflows))], dtype=float)
    return float(np.sum(np.array(cashflows, dtype=float) * fac))

def _npv(cashflows, rate):
    return discounted_cashflow(cashflows, rate)

def irr(cashflows):
    try:
        import numpy_financial as npf
        return float(npf.irr(np.array(cashflows, dtype=float)))
    except Exception:
        pass
    def f(r): return _npv(cashflows, r)
    lo, hi = -0.9, 1.0
    flo, fhi = f(lo), f(hi)
    if flo * fhi > 0:
        try_rates = np.linspace(-0.9, 1.0, 381)
        vals = np.array([f(r) for r in try_rates])
        idx = int(np.argmin(np.abs(vals)))
        return float(try_rates[idx])
    for _ in range(100):
        mid = (lo + hi) / 2.0
        fm = f(mid)
        if abs(fm) < 1e-6: return float(mid)
        if flo * fm < 0: hi, fhi = mid, fm
        else: lo, flo = mid, fm
    return float((lo + hi) / 2.0)

def discounted_payback(cashflows, rate):
    acc = 0.0
    for t, cf in enumerate(cashflows):
        acc += cf / ((1.0 + rate) ** t)
        if acc >= 0:
            return t
    return np.inf

def build_econ_rows(s3, s4, capex_kcz, years, rate):
    if s3 is None or s4 is None:
        return None
    sum3 = float(pd.to_numeric(s3["total_cost_kcz"], errors="coerce").fillna(0.0).sum())
    sum4 = float(pd.to_numeric(s4["total_cost_kcz"], errors="coerce").fillna(0.0).sum())
    factor = 8760.0 / max(1, len(s3))  # přepočet na rok
    yearly_saving = (sum3 - sum4) * factor
    cfs = [-capex_kcz] + [yearly_saving] * years
    return {
        "CAPEX_kCZ": float(capex_kcz),
        "yearly_saving_kCZ": yearly_saving,
        "payback_years": (capex_kcz / yearly_saving) if yearly_saving > 0 else np.inf,
        "discounted_payback_years": discounted_payback(cfs, rate),
        "NPV_kCZ": discounted_cashflow(cfs, rate),
        "IRR": irr(cfs),
    }

# ----------------- Hlavní -----------------
def main():
    ap = argparse.ArgumentParser(description="Scénáře 1–4b: sjednocené by_hour, finance, baterky, metriky a ekonomika.")
    ap.add_argument("--csv_dir", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--scenarios", default="s1,s2,s3,s4a,s4b")
    ap.add_argument("--price_commodity_mwh", type=float, required=True)
    ap.add_argument("--price_distribution_mwh", type=float, required=True)
    ap.add_argument("--price_feed_in_mwh", type=float, required=True)
    ap.add_argument("--by_hour_bat_local_csv", default="")
    ap.add_argument("--by_hour_bat_central_csv", default="")
    ap.add_argument("--local_price_per_kwh", type=float, default=0.0)
    ap.add_argument("--local_fixed_cost", type=float, default=0.0)
    ap.add_argument("--central_price_per_kwh", type=float, default=0.0)
    ap.add_argument("--central_fixed_cost", type=float, default=0.0)
    ap.add_argument("--project_years", type=int, default=15)
    ap.add_argument("--discount_rate", type=float, default=0.03)
    ap.add_argument("--batt_cycle_life", type=float, default=5000.0)
    ap.add_argument("--eta_c", type=float, default=0.95)  # pro dopočet discharge ze SOC
    ap.add_argument("--eta_d", type=float, default=0.95)
    args = ap.parse_args()

    csvdir = Path(args.csv_dir)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    ean_o_long = _load(csvdir, "ean_o_long")
    ean_d_long = _load(csvdir, "ean_d_long")
    eano_after = _load(csvdir, "eano_after_pv")
    eand_after = _load(csvdir, "eand_after_pv")
    local_self = _load(csvdir, "local_selfcons")
    by_hour_after = _load(csvdir, "by_hour_after")
    allocations = _load(csvdir, "allocations")

    scen = set(s.strip().lower() for s in args.scenarios.split(",") if s.strip())
    built = {}

    # S1
    if "s1" in scen:
        s1 = build_s1(ean_o_long, args.price_commodity_mwh, args.price_distribution_mwh)
        d1, w1, m1 = _profiles(s1)
        with pd.ExcelWriter(outdir / "scenario_1_grid_only.xlsx", engine="xlsxwriter") as xw:
            _write_dashboard_finance(xw, "S1 Grid-only", s1, d1, w1, m1)
        built["S1"] = s1

    # S2
    if "s2" in scen:
        s2 = build_s2(eano_after, eand_after, local_self,
                      args.price_commodity_mwh, args.price_distribution_mwh, args.price_feed_in_mwh)
        d2, w2, m2 = _profiles(s2)
        with pd.ExcelWriter(outdir / "scenario_2_local_pv.xlsx", engine="xlsxwriter") as xw:
            _write_dashboard_finance(xw, "S2 PV-only", s2, d2, w2, m2)
        built["S2"] = s2

    # S3
    if "s3" in scen:
        s3 = build_s3(by_hour_after, allocations, ean_o_long, ean_d_long, local_self,
                      args.price_commodity_mwh, args.price_distribution_mwh, args.price_feed_in_mwh)
        d3, w3, m3 = _profiles(s3)
        with pd.ExcelWriter(outdir / "scenario_3_sharing.xlsx", engine="xlsxwriter") as xw:
            _write_dashboard_finance(xw, "S3 Sharing", s3, d3, w3, m3, allocations=allocations)
        built["S3"] = s3
    else:
        s3 = None

    # bateriová data
    bh_local = _load_battery_by_hour(args.by_hour_bat_local_csv, "local", eta_c=args.eta_c, eta_d=args.eta_d) if args.by_hour_bat_local_csv else None
    bh_centr = _load_battery_by_hour(args.by_hour_bat_central_csv, "central", eta_c=args.eta_c, eta_d=args.eta_d) if args.by_hour_bat_central_csv else None
    flows4a = _load_battery_flows(args.by_hour_bat_local_csv) if args.by_hour_bat_local_csv else None
    flows4b = _load_battery_flows(args.by_hour_bat_central_csv) if args.by_hour_bat_central_csv else None

    cap_local = _load_local_caps(csvdir) or (float(bh_local["soc_kwh"].max()) if (bh_local is not None and "soc_kwh" in bh_local.columns) else None)
    _, cap_central = _load_central_meta(csvdir)
    if cap_central is None and bh_centr is not None and "soc_kwh" in bh_centr.columns:
        cap_central = float(bh_centr["soc_kwh"].max())

    econ_rows = []

    # S4a – Local battery
    if "s4a" in scen:
        base = built.get("S3", None)
        if base is None:
            base = build_s3(by_hour_after, allocations, ean_o_long, ean_d_long, local_self,
                            args.price_commodity_mwh, args.price_distribution_mwh, args.price_feed_in_mwh)
        s4a = base.copy()

        # 1) pokud bat-CSV obsahuje import/export po baterii, přepiš je
        if flows4a is not None:
            s4a = s4a.merge(flows4a, on="datetime", how="left", suffixes=("","_bat"))
            if "import_bat" in s4a.columns:
                s4a["import"] = s4a["import_bat"].fillna(s4a["import"]); s4a.drop(columns=["import_bat"], inplace=True)
            if "export_bat" in s4a.columns:
                s4a["export"] = s4a["export_bat"].fillna(s4a["export"]); s4a.drop(columns=["export_bat"], inplace=True)
            if "shared_received_kwh_bat" in s4a.columns:
                s4a["shared_received_kwh"] = s4a["shared_received_kwh_bat"].fillna(s4a.get("shared_received_kwh", 0.0))
                s4a.drop(columns=["shared_received_kwh_bat"], inplace=True)

        # 2) smaž nulové storage sloupce z base, ať nevznikne _x/_y
        for c in ["own_pv_stored_kwh", "shared_pv_stored_kwh", "consumption_from_storage_kwh", "soc_kwh"]:
            if c in s4a.columns:
                s4a.drop(columns=[c], inplace=True)

        # 3) přimerguj hodinové baterkové metriky
        if bh_local is not None:
            s4a = s4a.merge(
                bh_local[["datetime", "own_pv_stored_kwh", "shared_pv_stored_kwh", "consumption_from_storage_kwh"]],
                on="datetime", how="left"
            )

        # 4) koalescence na čísla
        for c in ["own_pv_stored_kwh", "shared_pv_stored_kwh", "consumption_from_storage_kwh"]:
            if c not in s4a.columns:
                s4a[c] = 0.0
            s4a[c] = pd.to_numeric(s4a[c], errors="coerce").fillna(0.0)

        # 5) FALLBACK: když jsme nenašli bat import/export, uprav z S3: snížíme import o discharge a export o charge
        if flows4a is None or (("import" in s4a.columns) and s4a["import"].equals(base["import"])):
            s4a["import"] = (s4a["import"] - s4a["consumption_from_storage_kwh"]).clip(lower=0.0)
        if flows4a is None or (("export" in s4a.columns) and s4a["export"].equals(base["export"])):
            s4a["export"] = (s4a["export"] - (s4a["own_pv_stored_kwh"] + s4a["shared_pv_stored_kwh"])).clip(lower=0.0)

        s4a = _ensure_schema(s4a)

        # 6) finance z nových import/export (sdílení dist necháváme podle 'shared_received_kwh', pokud máme)
        k_com = args.price_commodity_mwh / 1000.0
        k_dist = args.price_distribution_mwh / 1000.0
        k_feed = args.price_feed_in_mwh / 1000.0
        s4a["cost_import_kcz"] = s4a["import"] * (k_com + k_dist)
        s4a["cost_shared_dist_kcz"] = s4a.get("shared_received_kwh", 0.0) * k_dist
        s4a["revenue_export_kcz"] = s4a["export"] * k_feed
        s4a["total_cost_kcz"] = s4a["cost_import_kcz"] + s4a["cost_shared_dist_kcz"] - s4a["revenue_export_kcz"]

        d4a, w4a, m4a = _profiles(s4a)
        bm_local = battery_metrics(bh_local, float(cap_local or 0.0))
        with pd.ExcelWriter(outdir / "scenario_4a_batt_local.xlsx", engine="xlsxwriter") as xw:
            _write_dashboard_finance(xw, "S4a Local battery", s4a, d4a, w4a, m4a,
                                     allocations=allocations, bat_df=bh_local, bat_metrics=bm_local)
        built["S4a"] = s4a

        capex_local = float((cap_local or 0.0) * args.local_price_per_kwh + args.local_fixed_cost)
        if s3 is None:
            s3 = base
        econ = build_econ_rows(s3, s4a, capex_local, args.project_years, args.discount_rate)
        if econ:
            econ_rows.append({"scenario": "S4a vs S3", **econ})

    # S4b – Central battery
    if "s4b" in scen:
        base = built.get("S3", None)
        if base is None:
            base = build_s3(by_hour_after, allocations, ean_o_long, ean_d_long, local_self,
                            args.price_commodity_mwh, args.price_distribution_mwh, args.price_feed_in_mwh)
        s4b = base.copy()

        if flows4b is not None:
            s4b = s4b.merge(flows4b, on="datetime", how="left", suffixes=("","_bat"))
            if "import_bat" in s4b.columns:
                s4b["import"] = s4b["import_bat"].fillna(s4b["import"]); s4b.drop(columns=["import_bat"], inplace=True)
            if "export_bat" in s4b.columns:
                s4b["export"] = s4b["export_bat"].fillna(s4b["export"]); s4b.drop(columns=["export_bat"], inplace=True)
            if "shared_received_kwh_bat" in s4b.columns:
                s4b["shared_received_kwh"] = s4b["shared_received_kwh_bat"].fillna(s4b.get("shared_received_kwh", 0.0))
                s4b.drop(columns=["shared_received_kwh_bat"], inplace=True)

        for c in ["own_pv_stored_kwh", "shared_pv_stored_kwh", "consumption_from_storage_kwh", "soc_kwh"]:
            if c in s4b.columns:
                s4b.drop(columns=[c], inplace=True)

        if bh_centr is not None:
            s4b = s4b.merge(
                bh_centr[["datetime", "own_pv_stored_kwh", "shared_pv_stored_kwh", "consumption_from_storage_kwh"]],
                on="datetime", how="left"
            )

        for c in ["own_pv_stored_kwh", "shared_pv_stored_kwh", "consumption_from_storage_kwh"]:
            if c not in s4b.columns:
                s4b[c] = 0.0
            s4b[c] = pd.to_numeric(s4b[c], errors="coerce").fillna(0.0)

        if flows4b is None or (("import" in s4b.columns) and s4b["import"].equals(base["import"])):
            s4b["import"] = (s4b["import"] - s4b["consumption_from_storage_kwh"]).clip(lower=0.0)
        if flows4b is None or (("export" in s4b.columns) and s4b["export"].equals(base["export"])):
            s4b["export"] = (s4b["export"] - (s4b["own_pv_stored_kwh"] + s4b["shared_pv_stored_kwh"])).clip(lower=0.0)

        s4b = _ensure_schema(s4b)

        k_com = args.price_commodity_mwh / 1000.0
        k_dist = args.price_distribution_mwh / 1000.0
        k_feed = args.price_feed_in_mwh / 1000.0
        s4b["cost_import_kcz"] = s4b["import"] * (k_com + k_dist)
        s4b["cost_shared_dist_kcz"] = s4b.get("shared_received_kwh", 0.0) * k_dist
        s4b["revenue_export_kcz"] = s4b["export"] * k_feed
        s4b["total_cost_kcz"] = s4b["cost_import_kcz"] + s4b["cost_shared_dist_kcz"] - s4b["revenue_export_kcz"]

        d4b, w4b, m4b = _profiles(s4b)
        bm_c = battery_metrics(bh_centr, float(cap_central or 0.0))
        with pd.ExcelWriter(outdir / "scenario_4b_batt_central.xlsx", engine="xlsxwriter") as xw:
            _write_dashboard_finance(xw, "S4b Central battery", s4b, d4b, w4b, m4b,
                                     allocations=allocations, bat_df=bh_centr, bat_metrics=bm_c)
        built["S4b"] = s4b

        capex_central = float((cap_central or 0.0) * args.central_price_per_kwh + args.central_fixed_cost)
        if s3 is None:
            s3 = base
        econ = build_econ_rows(s3, s4b, capex_central, args.project_years, args.discount_rate)
        if econ:
            econ_rows.append({"scenario": "S4b vs S3", **econ})

    econ_df = pd.DataFrame(econ_rows) if econ_rows else None
    _write_summary(outdir, built, econ=econ_df)

if __name__ == "__main__":
    main()
