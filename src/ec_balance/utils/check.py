# SPDX-License-Identifier: AGPL-3.0-or-later
import sys
import argparse
from pathlib import Path
import pandas as pd

def _fail(msg: str) -> None:
    print(f"[X] {msg}")
    sys.exit(2)

def _ok(msg: str) -> None:
    print(f"[OK] {msg}")

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        _fail(f"Soubor neexistuje: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        _fail(f"Nešlo číst CSV {path.name}: {e}")
    if "datetime" in df.columns:
        try:
            df["datetime"] = pd.to_datetime(df["datetime"])
        except Exception:
            _fail(f"{path.name}: sloupec 'datetime' nejde převést na datum/čas.")
    return df

def _has_any_energy_col(df: pd.DataFrame) -> bool:
    for c in df.columns:
        lc = str(c).lower()
        if lc.endswith("kwh") or lc.endswith("_mwh"):
            return True
    return False

def _ensure_cols(df: pd.DataFrame, required: list[str], src: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        cols = ", ".join(map(str, df.columns[:20]))
        _fail(f"{src}: chybí sloupce {missing}. Nalezené: {cols}")

def _check_after_step2(p: Path) -> None:
    for fname in ("eano_after_pv.csv", "eand_after_pv.csv", "local_selfcons.csv"):
        df = _read_csv(p / fname)
        _ensure_cols(df, ["datetime", "site"], fname)
        if not _has_any_energy_col(df):
            _fail(f"{fname}: nenašel jsem žádný *_kwh nebo *_mwh sloupec.")
        _ok(fname)

def _check_after_step3(p: Path) -> None:
    _read_csv(p / "by_site_after.csv"); _ok("by_site_after.csv")
    df = _read_csv(p / "by_hour_after.csv"); _ensure_cols(df, ["datetime"], "by_hour_after.csv"); _ok("by_hour_after.csv")
    df = _read_csv(p / "allocations.csv");   _ensure_cols(df, ["datetime"], "allocations.csv");   _ok("allocations.csv")

def _check_after_batt(p: Path) -> None:
    cand = {"consumption_from_storage_kwh","own_stored_kwh","shared_stored_kwh","discharge_kwh","energy_out_kwh"}
    dfL = _read_csv(p / "by_hour_after_bat_local.csv")
    dfC = _read_csv(p / "by_hour_after_bat_central.csv")
    if cand.isdisjoint(dfL.columns):
        _fail("by_hour_after_bat_local.csv: nenašel jsem žádný sloupec s vybíjením z baterky.")
    if cand.isdisjoint(dfC.columns):
        _fail("by_hour_after_bat_central.csv: nenašel jsem žádný sloupec s vybíjením z baterky.")
    _ok("by_hour_after_bat_local.csv"); _ok("by_hour_after_bat_central.csv")

def _check_after_econ(p: Path) -> None:
    _read_csv(p / "local_econ_best.csv");   _ok("local_econ_best.csv")
    _read_csv(p / "central_econ_best.csv"); _ok("central_econ_best.csv")

def main() -> None:
    ap = argparse.ArgumentParser(description="Sanity-check dat v out/csv.")
    ap.add_argument("--csv_dir", default="./out/csv")
    ap.add_argument("--stage", default="full",
                    choices=["after-step2","after-step3","after-batt","after-econ","full"])
    args = ap.parse_args()

    p = Path(args.csv_dir)
    if args.stage in ("after-step2","full"):  _check_after_step2(p)
    if args.stage in ("after-step3","full"):  _check_after_step3(p)
    if args.stage in ("after-batt","full"):   _check_after_batt(p)
    if args.stage in ("after-econ","full"):   _check_after_econ(p)

    print("[OK] Kontrola prošla.")

if __name__ == "__main__":
    main()
