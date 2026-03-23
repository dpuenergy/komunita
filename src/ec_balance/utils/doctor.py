# SPDX-License-Identifier: AGPL-3.0-or-later
import sys, os, platform, shutil, subprocess
from pathlib import Path

OK = "[OK]"
X  = "[X ]"
I  = "[i ]"

def _print_ok(msg): print(f"{OK} {msg}")
def _print_x(msg):  print(f"{X} {msg}")
def _print_i(msg):  print(f"{I} {msg}")

def _which(cmd: str) -> bool:
    return shutil.which(cmd) is not None

def _import_ok(module: str) -> bool:
    try:
        __import__(module)
        return True
    except Exception:
        return False

def main():
    strict = "--strict" in sys.argv

    root = Path(__file__).resolve().parents[2]
    csvdir = root / "out" / "csv"

    # Základ
    _print_ok(f"OS: {platform.system()} {platform.release()} ({platform.machine()})")
    _print_ok(f"Python: {platform.python_version()}  venv={'ON' if os.environ.get('VIRTUAL_ENV') else 'OFF'}")

    # Nástroje
    for tool in ("git", "gh", "pre-commit"):
        if _which(tool):
            _print_ok(f"binary: {tool} OK")
        else:
            _print_x(f"binary: {tool} NOT FOUND")

    # Git remote
    try:
        r = subprocess.run(["git","remote","get-url","origin"],
                           cwd=root, capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            _print_ok(f"git remote origin: {r.stdout.strip()}")
        else:
            _print_x("git remote origin: nelze zjistit")
    except Exception:
        _print_x("git remote origin: chyba")

    # Balíček a importy
    try:
        import importlib.metadata as im
        ver = im.version("ec-balance")
        _print_ok(f"package ec-balance: {ver}")
    except Exception:
        _print_x("package ec-balance: nenalezen (pip install -e . ?)")

    steps = [
        "ec_balance.pipeline.step1_wide_to_long",
        "ec_balance.pipeline.step2_local_pv",
        "ec_balance.pipeline.step3_sharing",
        "ec_balance.pipeline.step4a_batt_local_byhour",
        "ec_balance.pipeline.step4b_batt_econ",
        "ec_balance.pipeline.step5a_batt_central_byhour",
        "ec_balance.pipeline.step5_batt_central",
        "ec_balance.pipeline.step6_excel_scenarios",
    ]
    bad = 0
    for m in steps:
        if _import_ok(m):
            _print_ok(f"import: {m}")
        else:
            _print_x(f"import: {m}")
            bad += 1

    # Výstupy
    if csvdir.exists():
        count = len(list(csvdir.glob("*.csv")))
        _print_ok(f"out/csv existuje ({count} CSV)")
    else:
        _print_i("out/csv neexistuje (zatím bez výstupů)")

    if strict and (bad > 0 or not _which("git")):
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
