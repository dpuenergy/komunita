# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

def _load(csvdir: Path, name: str, parse_dt=True):
    p = csvdir / f"{name}.csv"
    if not p.exists(): return None
    if parse_dt:
        return pd.read_csv(p, parse_dates=["datetime"])
    return pd.read_csv(p)

def _sum_hour(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame(columns=["datetime", col])
    out = df.groupby("datetime", as_index=False)[col].sum()
    out["datetime"] = pd.to_datetime(out["datetime"])
    return out.sort_values("datetime").reset_index(drop=True)

def _hour_union(dfs):
    idx = None
    for df in dfs:
        if df is None or df.empty: continue
        idx = df["datetime"] if idx is None else idx.union(df["datetime"])
    if idx is None: return pd.DataFrame(columns=["datetime"])
    return pd.DataFrame({"datetime": pd.DatetimeIndex(idx).sort_values()})

def _merge_on_time(a, b):
    if a is None or a.empty: return b
    if b is None or b.empty: return a
    a = a.copy(); b = b.copy()
    a["datetime"] = pd.to_datetime(a["datetime"], errors="coerce")
    b["datetime"] = pd.to_datetime(b["datetime"], errors="coerce")
    out = pd.merge(a, b, on="datetime", how="outer")
    return out.sort_values("datetime").reset_index(drop=True)

def build_s1_grid_only(ean_o_long, price_comm_mwh, price_dist_mwh):
    cons = _sum_hour(ean_o_long, "value_kwh").rename(columns={"value_kwh":"consumption"})
    base = cons.copy()
    base["import"] = base["consumption"]
    base["pv_production"] = 0.0
    base["self_consumption"] = 0.0
    base["clear_export"] = 0.0
    base["shared_received_kwh"] = 0.0
    base["shared_sent_kwh"] = 0.0
    # finance (na řádek i roční sumář)
    price_use_kwh = (price_comm_mwh + price_dist_mwh) / 1000.0
    base["cost_kcz"] = base["import"] * price_use_kwh
    return base

def build_s2_local_pv(eano_after_pv, eand_after_pv, local_self, price_comm_mwh, price_dist_mwh, price_feed_mwh):
    imp = _sum_hour(eano_after_pv, "import_after_kwh").rename(columns={"import_after_kwh":"import"})
    exp = _sum_hour(eand_after_pv, "export_after_kwh").rename(columns={"export_after_kwh":"clear_export"})
    selfc = _sum_hour(local_self, "local_selfcons_kwh").rename(columns={"local_selfcons_kwh":"self_consumption"})
    base = _merge_on_time(_merge_on_time(imp, exp), selfc).fillna(0.0)
    base["consumption"] = base["import"] + base["self_consumption"]
    base["pv_production"] = base["self_consumption"] + base["clear_export"]
    base["shared_received_kwh"] = 0.0
    base["shared_sent_kwh"] = 0.0
    # finance
    price_use_kwh  = (price_comm_mwh + price_dist_mwh) / 1000.0
    price_feed_kwh =  price_feed_mwh / 1000.0
    base["cost_kcz"]      = base["import"] * price_use_kwh - base["clear_export"] * price_feed_kwh
    base["saving_pv_kcz"] = base["self_consumption"] * price_use_kwh
    base["revenue_feed_kcz"] = base["clear_export"] * price_feed_kwh
    return base

def build_s3_sharing(by_hour_after, allocations, ean_o_long, ean_d_long, local_self, price_comm_mwh, price_dist_mwh, price_feed_mwh):
    if by_hour_after is None or by_hour_after.empty:
        raise ValueError("Chybí by_hour_after.csv (krok 3).")
    df = by_hour_after.rename(columns={
        "import_residual_kwh": "import",
        "export_residual_kwh": "clear_export"
    })[["datetime","import","clear_export"]].copy()
    # self + původní O/D pro transparentnost
    selfc = _sum_hour(local_self, "local_selfcons_kwh").rename(columns={"local_selfcons_kwh":"self_consumption"})
    cons  = _sum_hour(ean_o_long, "value_kwh").rename(columns={"value_kwh":"consumption"})
    prod  = _sum_hour(ean_d_long, "value_kwh").rename(columns={"value_kwh":"pv_production"})
    df = _merge_on_time(df, selfc)
    df = _merge_on_time(df, cons)
    df = _merge_on_time(df, prod).fillna(0.0)
    # sdílení po hodinách – přijaté/odeslané
    if allocations is not None and not allocations.empty:
        sh_in  = allocations.groupby("datetime", as_index=False)["shared_kwh"].sum().rename(columns={"shared_kwh":"shared_received_kwh"})
        sh_out = allocations.groupby("datetime", as_index=False)["shared_kwh"].sum().rename(columns={"shared_kwh":"shared_sent_kwh"})
        df = _merge_on_time(df, sh_in)
        df = _merge_on_time(df, sh_out)
    else:
        df["shared_received_kwh"] = 0.0
        df["shared_sent_kwh"]     = 0.0
    # finance
    price_comm_kwh = price_comm_mwh / 1000.0
    price_dist_kwh = price_dist_mwh / 1000.0
    price_feed_kwh = price_feed_mwh / 1000.0
    # nakup: commodity+distribution pouze na import
    df["cost_import_kcz"] = df["import"] * (price_comm_kwh + price_dist_kwh)
    # sdílené: jen distribuce na přijaté sdílení
    df["cost_shared_dist_kcz"] = df["shared_received_kwh"] * price_dist_kwh
    df["revenue_feed_kcz"] = df["clear_export"] * price_feed_kwh
    df["cost_kcz"] = df["cost_import_kcz"] + df["cost_shared_dist_kcz"] - df["revenue_feed_kcz"]
    # úspory vs S2: komodita u sdílení
    df["saving_sharing_kcz"] = df["shared_received_kwh"] * price_comm_kwh
    return df

def _profiles_day_week_month(df: pd.DataFrame):
    """Vytvoř typické profily komunity: denní (24h) a týdenní (168h) pro každý měsíc + měsíční součty."""
    if df is None or df.empty:
        return None, None, None
    x = df.copy()
    x["datetime"] = pd.to_datetime(x["datetime"])
    x["month"] = x["datetime"].dt.month
    x["doy"] = x["datetime"].dt.dayofyear
    x["dow"] = x["datetime"].dt.dayofweek
    x["hour"] = x["datetime"].dt.hour

    cols = [c for c in ["consumption","import","pv_production","self_consumption","clear_export","shared_received_kwh","shared_sent_kwh"] if c in x.columns]

    # typický den v měsíci = průměr podle hour v daném měsíci
    day_tabs = {}
    week_tabs = {}
    month_tabs = {}
    for m in sorted(x["month"].unique()):
        xm = x[x["month"]==m]
        day = xm.groupby("hour", as_index=False)[cols].mean()
        day.insert(0,"month", m)
        # typický týden: průměr podle (dow,hour)
        xm["hweek"] = xm["dow"]*24 + xm["hour"]
        wk = xm.groupby("hweek", as_index=False)[cols].mean()
        wk.insert(0,"month", m)
        # měsíční součty
        ms = xm.groupby("month", as_index=False)[cols].sum()
        day_tabs[m] = day
        week_tabs[m]= wk
        month_tabs[m]= ms
    day_all = pd.concat(day_tabs.values(), ignore_index=True) if day_tabs else None
    week_all= pd.concat(week_tabs.values(), ignore_index=True) if week_tabs else None
    month_all=pd.concat(month_tabs.values(), ignore_index=True) if month_tabs else None
    return day_all, week_all, month_all

def _chart_axes_max(*dfs, cols=None):
    m = 0.0
    for df in dfs:
        if df is None or df.empty: continue
        for c in (cols or df.columns):
            if c == "datetime" or c == "month" or c == "hweek" or c == "hour": continue
            if c in df.columns:
                v = pd.to_numeric(df[c], errors="coerce").fillna(0.0).max()
                m = max(m, float(v))
    return (0, m*1.05 if m>0 else 1.0)

def _write_with_charts(xw, scenario_name: str, df: pd.DataFrame, day_prof: pd.DataFrame, week_prof: pd.DataFrame, month_prof: pd.DataFrame, top_links: pd.DataFrame = None):
    wb = xw.book
    # hlavní data
    if df is not None:
        df.to_excel(xw, sheet_name="by_hour", index=False)
    # KPI + grafy
    ws = wb.add_worksheet("Dashboard")
    # KPI
    cols = [c for c in ["consumption","pv_production","import","self_consumption","shared_received_kwh","clear_export"] if c in df.columns]
    total = {c: float(df[c].sum()/1000.0) for c in cols}
    kpi = pd.DataFrame({
        "Metric": [
            "Consumption (MWh)", "PV production (MWh)", "Import (MWh)",
            "Self-consumption (MWh)", "Shared received (MWh)", "Clear export (MWh)"
        ][:len(cols)],
        "Value": [total.get("consumption",0), total.get("pv_production",0), total.get("import",0),
                  total.get("self_consumption",0), total.get("shared_received_kwh",0), total.get("clear_export",0)][:len(cols)]
    })
    kpi.to_excel(xw, sheet_name="Dashboard", startrow=0, startcol=0, index=False)

    # průběhový graf
    if df is not None and not df.empty:
        n = len(df)
        chart = wb.add_chart({"type": "line"})
        chart.set_title({"name": f"{scenario_name}: Hourly flows"})
        chart.set_x_axis({"name": "Time"})
        ymin,ymax = _chart_axes_max(df, cols=["consumption","import","pv_production"])
        chart.set_y_axis({"name":"kWh","min":ymin,"max":ymax})
        cats = ["by_hour", 1, 0, n, 0]  # datetime
        for col_idx, series_name in enumerate(df.columns[1:4], start=1):
            chart.add_series({"name": ["by_hour", 0, col_idx],
                              "categories": cats,
                              "values": ["by_hour", 1, col_idx, n, col_idx]})
        chart.set_legend({"position":"bottom"})
        ws.insert_chart(1, 6, chart, {"x_scale": 1.6, "y_scale": 1.2})

    # koláč pokrytí (100% = spotřeba)
    if {"consumption","import","self_consumption","shared_received_kwh"}.issubset(df.columns):
        pie_tbl = pd.DataFrame({
            "Part": ["Own PV (self)","Shared PV","Purchased"],
            "kWh": [df["self_consumption"].sum(), df["shared_received_kwh"].sum(), df["import"].sum()]
        })
        pie_tbl.to_excel(xw, sheet_name="Dashboard", startrow=14, startcol=0, index=False)
        pie = wb.add_chart({"type":"doughnut"})
        pie.set_title({"name":"Shares of consumption"})
        pie.add_series({"categories": ["Dashboard", 15, 0, 17, 0],
                        "values":     ["Dashboard", 15, 1, 17, 1]})
        pie.set_hole_size(50)
        ws.insert_chart(14, 6, pie, {"x_scale":1.0,"y_scale":1.0})

    # profily
    if day_prof is not None:
        day_prof.to_excel(xw, sheet_name="profiles_day", index=False)
    if week_prof is not None:
        week_prof.to_excel(xw, sheet_name="profiles_week", index=False)
    if month_prof is not None:
        month_prof.to_excel(xw, sheet_name="profiles_month", index=False)

    # top links (sdílení) – pokud máme
    if top_links is not None and not top_links.empty:
        top_links.to_excel(xw, sheet_name="Links", index=False)
        ch = wb.add_chart({"type":"bar"})
        ch.set_title({"name":"Top shared links (from→to)"})
        n = len(top_links)
        ch.add_series({"name":"shared_kwh",
                       "categories":["Links",1,0,n,0],
                       "values":["Links",1,1,n,1]})
        ws.insert_chart(28, 6, ch, {"x_scale":1.2,"y_scale":1.0})

def main():
    ap = argparse.ArgumentParser(description="Scénáře 1–4b: samostatné Excely s grafy a profily (konsistentní osy).")
    ap.add_argument("--csv_dir", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--scenarios", default="s1,s2,s3,s4a,s4b", help="comma: s1,s2,s3,s4a,s4b")
    ap.add_argument("--price_commodity_mwh", type=float, required=True)
    ap.add_argument("--price_distribution_mwh", type=float, required=True)
    ap.add_argument("--price_feed_in_mwh", type=float, required=True)
    # volitelné hodinové výstupy baterek (pokud existují)
    ap.add_argument("--by_hour_bat_local_csv", default="")
    ap.add_argument("--by_hour_bat_central_csv", default="")
    args = ap.parse_args()

    csvdir = Path(args.csv_dir)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    # vstupy
    ean_o_long = _load(csvdir, "ean_o_long")
    ean_d_long = _load(csvdir, "ean_d_long")
    eano_after_pv = _load(csvdir, "eano_after_pv")
    eand_after_pv = _load(csvdir, "eand_after_pv")
    local_self = _load(csvdir, "local_selfcons")
    by_hour_after = _load(csvdir, "by_hour_after")
    allocations = _load(csvdir, "allocations")

    scen = set([s.strip().lower() for s in args.scenarios.split(",") if s.strip()])

    # S1
    if "s1" in scen:
        s1 = build_s1_grid_only(ean_o_long, args.price_commodity_mwh, args.price_distribution_mwh)
        d1,w1,m1 = _profiles_day_week_month(s1)
        with pd.ExcelWriter(outdir / "scenario_1_grid_only.xlsx", engine="xlsxwriter") as xw:
            # top links nedává smysl
            _write_with_charts(xw, "S1 Grid-only", s1, d1,w1,m1)

    # S2
    if "s2" in scen:
        s2 = build_s2_local_pv(eano_after_pv, eand_after_pv, local_self,
                               args.price_commodity_mwh, args.price_distribution_mwh, args.price_feed_in_mwh)
        d2,w2,m2 = _profiles_day_week_month(s2)
        with pd.ExcelWriter(outdir / "scenario_2_local_pv.xlsx", engine="xlsxwriter") as xw:
            _write_with_charts(xw, "S2 PV-only", s2, d2,w2,m2)

    # S3
    if "s3" in scen:
        s3 = build_s3_sharing(by_hour_after, allocations, ean_o_long, ean_d_long, local_self,
                              args.price_commodity_mwh, args.price_distribution_mwh, args.price_feed_in_mwh)
        d3,w3,m3 = _profiles_day_week_month(s3)
        # top links
        links = None
        if allocations is not None and not allocations.empty:
            pairs = allocations.groupby(["from_site","to_site"], as_index=False)["shared_kwh"].sum()
            pairs["pair"] = pairs["from_site"].astype(str) + " → " + pairs["to_site"].astype(str)
            links = pairs.sort_values("shared_kwh", ascending=False)[["pair","shared_kwh"]].head(20)
        with pd.ExcelWriter(outdir / "scenario_3_sharing.xlsx", engine="xlsxwriter") as xw:
            _write_with_charts(xw, "S3 Sharing", s3, d3,w3,m3, links)

    # S4a – lokální baterie (zatím placeholder bez hodinových toků, pokud nepřidáš CSV)
    if "s4a" in scen:
        bh_local = None
        if args.by_hour_bat_local_csv:
            p = Path(args.by_hour_bat_local_csv)
            if p.exists():
                bh_local = pd.read_csv(p, parse_dates=["datetime"])
        base = s3 if 's3' in locals() else build_s3_sharing(by_hour_after, allocations, ean_o_long, ean_d_long, local_self,
                                                            args.price_commodity_mwh, args.price_distribution_mwh, args.price_feed_in_mwh)
        if bh_local is not None:
            s4a = bh_local
        else:
            s4a = base.copy()
            # přidej „own_stored“ a „shared_stored“ (zatím 0 – dokud nebude dispatch)
            s4a["own_stored_kwh"] = 0.0
            s4a["shared_stored_kwh"] = 0.0
        d4a,w4a,m4a = _profiles_day_week_month(s4a)
        with pd.ExcelWriter(outdir / "scenario_4a_batt_local.xlsx", engine="xlsxwriter") as xw:
            _write_with_charts(xw, "S4a Local battery", s4a, d4a,w4a,m4a)

    # S4b – centrální baterie (analogicky)
    if "s4b" in scen:
        bh_cent = None
        if args.by_hour_bat_central_csv:
            p = Path(args.by_hour_bat_central_csv)
            if p.exists():
                bh_cent = pd.read_csv(p, parse_dates=["datetime"])
        base = s3 if 's3' in locals() else build_s3_sharing(by_hour_after, allocations, ean_o_long, ean_d_long, local_self,
                                                            args.price_commodity_mwh, args.price_distribution_mwh, args.price_feed_in_mwh)
        if bh_cent is not None:
            s4b = bh_cent
        else:
            s4b = base.copy()
            s4b["own_stored_kwh"] = 0.0
            s4b["shared_stored_kwh"] = 0.0
        d4b,w4b,m4b = _profiles_day_week_month(s4b)
        with pd.ExcelWriter(outdir / "scenario_4b_batt_central.xlsx", engine="xlsxwriter") as xw:
            _write_with_charts(xw, "S4b Central battery", s4b, d4b,w4b,m4b)

if __name__ == "__main__":
    main()
