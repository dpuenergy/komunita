# tools/run_ui.py
# Streamlit UI for ec_balance pipeline (minimal wrapper around step*.py)
from __future__ import annotations

import json
import os
import shutil
import shlex
import subprocess
import sys
import re
import traceback
import time

import numpy as np
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Union

import pandas as pd


# -------------------------
# UI helpers (no impact on compute logic)
# -------------------------
from pathlib import Path as _Path


# --- UI text & metrics mapping (generated from app_komunita.xlsx) ---
_UI_LABEL_MAP = {
    "+ Nový běh": "+ Založit nový výpočet",
    "+ Nový projekt": "+ Založit nový projekt",
    "1. Vstupy a běh projektu": "Základní informace o projektu",
    "Analyzovat umístění (siting)": "Spustit analýzu umístění centrální baterie",
    "Aplikovat jen když kWp > 0": "Navrhovat baterii jen na OM s výkonem kWp > 0",
    "Běh": "Pojmenování výpočtu",
    "Celková instalovaná kapacita baterek – lokální (kWh)": "Celková instalovaná kapacita baterií – lokální (kWh)",
    "Celková instalovaná kapacita baterie – centrální (kWh)": "Celková instalovaná kapacita baterie – centrální (kWh)",
    "Celkové výsledky (průběžně)": "Průběžné výsledky",
    "Celkový instalovaný výkon FVE (kWp)": "Celkový instalovaný výkon výroben (kWp)",
    "Cena distribuce (Kč/MWh)": "Cena distribuční složky elektřiny (Kč/MWh)",
    "Cena komodity (Kč/MWh)": "Cena silové elektřiny (Kč/MWh)",
    "Ceny po OM (společné vs individuální)": "Nastavení individuálních jednotkových cen",
    "Ceny, sdílení a alokační pravidla": "Referenční jednotkové ceny",
    "Data k načtení": "Data k načtení",
    "Constraints": "Nastavení možnosti dodávky do sítě a dobíjení baterie ze sítě",
    "Curtailment (MWh)": "Zmařená elektřina z vlastních výroben (MWh)",
    "Diskontní sazba": "Diskontní sazba - zadávat v %",
    "Distribuce": "Distribuční složka",
    "Doba projektu (roky)": "Doba projektu (roky) - výchozí hodnota 20",
    "Ekonomika baterek": "Ekonomika projektu",
    "Energetická komunita – pipeline (UI)": "Komunitní energetika by DPU ENERGY",
    "Export (MWh)": "Přetoky elektřiny z vlastních výroben (MWh)",
    "Fáze 1 – Příprava dat (WIDE → LONG, mapování OM)": "Krok 1 - Nahrání dat pro výpočet",
    "Fáze 2 – Bilance bez baterií": "Krok 2 - Bilance výroby a spotřeby bez baterií a sdílení",
    "Fáze 2 – Komunitní sdílení bez baterií": "Krok 3 – Komunitní sdílení bez baterií",
    "Fáze 2 – Pairing v OM (lokální bilance bez baterií)": "Krok 2 - Bilance výroby a spotřeby bez baterií a sdílení",
    "Fáze 3A – Lokální baterie v OM": "Krok 4B - Lokální baterie",
    "Fáze 3A – Lokální baterie v OM (citlivost, bez sdílení)": "Krok 4B - Varianty lokálních baterií",
    "Fáze 3B - Jedna komunitní baterie v OM": "Krok 4A - Centrální baterie pro komunitu",
    "Import (MWh)": "Nákup elektřiny ze sítě (MWh)",
    "Import dat z předchozích výpočtů": "Import dat z předchozích výpočtů",
    "Jak zadat varianty kapacit? (ad 5)": "Nastavení kapacit baterií pro analýzu",
    "Jednotky množství elektrické energie": "Jednotky množství elektrické energie",
    "Jednotný seznam pro všechna místa": "Společné",
    "Kapacita centrální baterie (kWh)": "Navrhovaná kapacita centrální baterie (kWh)",
    "Komodita": "Silová elektřina",
    "Komunitní: cena Kč/kWh": "Centrální baterie - variabilní cena Kč/kWh (výchozí hodnota 11 000 Kč/kWh)",
    "Komunitní: fixní náklad Kč": "Centrální baterie - fixní složka ceny (výchozí hodnota 700 000)",
    "Krok spotřeby a výroby": "Krok spotřeby a výroby",
    "Lokální: cena Kč/kWh": "Lokální baterie - variabilní cena Kč/kWh (výchozí hodnota 12 500 Kč/kWh)",
    "Lokální: fixní náklad Kč": "Lokální baterie - fixní složka ceny (výchozí hodnota 200 000)",
    "Max kWh na 1 kWp": "Limit kWh na 1 kWp",
    "Max. příjemců": "Maximální počet příjemců sdílené elektřiny v daném okně",
    "Metika": "Metrika",
    "Místa, pro která spočítat citlivost": "Výběr míst pro umístění lokálních baterií.",
    "Nabití lokálních baterií z vlastní FVE (MWh)": "Nabití elektřiny do baterie v 1 OM (MWh)",
    "Nabití lokálních baterií ze sdílení (MWh)": "Nabití elektřiny do baterie v jiném OM (MWh)",
    "Nahraj EANd WIDE CSV (výroba)": "Nahrát soubor(y) s hodinovými daty o výrobě",
    "Nahraj EANo WIDE CSV (spotřeba)": "Nahrát soubor(y) s hodinovými daty o spotřebě",
    "Nastavení výpočtu": "Nastavení výpočtu",
    "Načíst data z předchozích výpočtů": "Načíst data z předchozích výpočtů",
    "Importovat data včetně nastavení výpočtu": "Importovat data včetně nastavení výpočtu",
    "Importovat zvolená data z vybraného výpočtu": "Importovat zvolená data z vybraného výpočtu",
    "OM": "Název odběrného místa",
    "OM pro analýzu umístění (siting)": "Seznam odběrných míst pro posouzení umístění centrální baterie",
    "Objem elektřiny uložené do baterie v jiném odběrném místě": "Objem elektřiny uložené do baterie v jiném odběrném místě (MWh)",
    "Objem elektřiny uložené lokálně do baterie": "Objem elektřiny uložené lokálně do baterie (MWh)",
    "Objem nakupované elektřiny po instalaci FVE": "Nákup elektřiny ze sítě po instalaci vlastních výroben (MWh)",
    "Objem nakupované elektřiny po instalaci baterie v centrálním provedení": "Nákup elektřiny ze sítě po instalaci baterie v centrálním provedení (MWh)",
    "Objem nakupované elektřiny po instalaci baterie v lokálním provedení": "Nákup elektřiny ze sítě po instalaci baterií v lokálním provedení (MWh)",
    "Objem nakupované elektřiny po zavedení sdílení": "Nákup elektřiny ze sítě  po zavedení sdílení (MWh)",
    "Objem ořezané/zmařené elektřiny": "Zmařená elektřina po instalaci vlastních výroben (MWh)",
    "Objem ořezané/zmařené elektřiny po instalaci baterie v centrálním provedení": "Zmařená elektřina elektřiny po instalaci baterie v centrálním provedení (MWh)",
    "Objem ořezané/zmařené elektřiny po instalaci baterie v lokálním provedení": "Zmařená elektřina elektřiny po instalaci baterií v lokálním provedení (MWh)",
    "Objem ořezané/zmařené elektřiny po zavedení sdílení": "Zmařená elektřina elektřiny po zavedení sdílení (MWh)",
    "Objem přetoků z FVE": "Přetoky elektřiny z vlastních výroben (MWh)",
    "Objem přetoků z FVE po instalaci baterie v centrálním provedení": "Objem přetoků elektřiny z vlastních výroben po instalaci baterie v centrálním provedení (MWh)",
    "Objem přetoků z FVE po instalaci baterie v lokálním provedení": "Objem přetoků elektřiny z vlastních výroben po instalaci baterií v lokálním provedení (MWh)",
    "Objem přetoků z FVE po zavedení sdílení": "Objem přetoků elektřiny z vlastních výroben po zavedení sdílení (MWh)",
    "Objem sdílené elektřiny po instalaci baterie v centrálním provedení": "Sdílení elektřiny po instalaci baterie v centrálním provedení (MWh)",
    "Objem sdílené elektřiny po instalaci baterie v lokálním provedení": "Sdílení elektřiny po instalaci baterií v lokálním provedení (MWh)",
    "Objem sdílené elektřiny po zavedení sdílení": "Sdílení elektřiny po zavedení sdílení (MWh)",
    "Oddělovač v csv souborech spotřeby a výroby": "Oddělovač v csv souborech spotřeby a výroby",
    "Porovnání scénářů (rychlé)": "Srovnání scénářů výpočtu",
    "Projekt": "Název projektu",
    "Přehled EANd (výroba) – z LONG CSV": "Přehled výroben elektřiny",
    "Přehled EANo (spotřeba) – z LONG CSV": "Přehled odběrných míst",
    "Načíst varianty": "Načíst varianty",
    "Původní objem nakupované elektřiny": "Nákup elektřiny ze sítě (MWh)",
    "Roční komunitní přebytek (vč. curtailmentu):": "Roční přetoky v komunitě včetně ořezané energie:",
    "Sdílení (MWh)": "Sdílení elektřiny (MWh)",
    "Souhrn KPI": "Klíčové ukazatele",
    "Spotřeba elektřiny": "Celková spotřeba elektřiny (MWh)",
    "Spustit Krok 2 (lokální PV)": "Spustit Krok 2",
    "Spustit Krok 3 (sdílení)": "Spustit Krok 3",
    "Spustit krok 4 (citlivost)": "Spustit Krok 4B",
    "Tady si můžeš připravit podvarianty výroben (aktivní/neetivní, kWp override) a pak znovu spustit Krok 2/3. Změny se ukládají do aktivní varianty (patch).": "Postup pro variantní výpočet výkonů výroben: Založit variantu v sidebaru a v ní nastavit aktivní/neaktivní výrobnu a výkony aktivních výroben. Ve variantě výchozí není možné varianty výroben zadávat.",
    "Uložit cap_list_by_site.csv": "Uložit varianty kapacit pro výpočet",
    "Uložit site_map.csv pro Krok 2": "Potvrdit párování spotřeby a výroby",
    "Uložit tento detail jako variantu baterie": "Založit pro dané umístění baterie novou variantu výpočtu.",
    "Uplatnit limit max kWh/kWp": "Uplatnit limit kWh baterie na kWp výkonu",
    "Varianta": "Výběr varianty",
    "Varianty importu nastavení": "Varianty importu nastavení",
    "Varianty výroben (PV) – správa v rámci běhu": "Variantní zadání výkonu výroben",
    "Vlastní spotřeba FVE (MWh)": "Využití vlastní vyrobené elektřiny (MWh)",
    "Data ve vybraném zdrojovém výpočtu": "Data ve vybraném zdrojovém výpočtu",
    "Vyber OM pro detailní průběh": "Vybrat odběrné místo pro umístění centrální baterie",
    "Vybrat data k načtení": "Vybrat data k načtení",
    "Vytvořit / přepsat site_constraints.csv": "Potvrdit nastavení přetoků a odběru ze sítě",
    "Vytvořit detailní by-hour pro vybrané OM": "Spustit Krok 4A",
    "Využití elektřiny z FVE": "Využití vlastní vyrobené elektřiny (MWh)",
    "Využití elektřiny z FVE po instalaci baterie v centrálním provedení": "Využití vlastní vyrobené elektřiny po instalaci baterie v centrálním provedení (MWh)",
    "Využití elektřiny z FVE po instalaci baterie v lokálním provedení": "Využití vlastní vyrobené elektřiny po instalaci baterií v lokálním provedení (MWh)",
    "Využití elektřiny z FVE po zavedení sdílení": "Využití vlastní vyrobené elektřiny po zavedení sdílení (MWh)",
    "Výchozí seznam kapacit (kWh)": "Výchozí seznam kapacit k posouzení (kWh) - oddělovat čárkou",
    "Výkup": "Prodej přetoků",
    "Výkup (Kč/MWh)": "Cena za prodej přetoků (Kč/MWh)",
    "Výroba FVE (MWh)": "Celková výroba vlastní elektřiny (MWh)",
    "Výroba elektřiny z FVE": "Celková výroba vlastní elektřiny (MWh)",
    "Zadej varianty kapacit pro každé místo zvlášť (odděl čárkou).": "Zadat varianty kapacit pro každé místo individuálně. Kapacity oddělovat čárkou.",
    "Zdrojový výpočet pro import": "Zdrojový výpočet pro import",
    "Zdrojový projekt pro import": "Zdrojový projekt pro import",
    "Zvlášť pro každé místo": "Individuální",
    "aktivní": "Aktivní výrobna",
    "allow_export_grid": "Povolit přetoky",
    "allow_import_grid": "Povolit dobíjení baterie ze sítě",
    "base": "výchozí",
    "central_siting_analysis.csv": "Výstup analýzy umístění centrální baterie",
    "datetime (u měsíčních přehledů)": "měsíc",
    "ean": "Označení",
    "eand_ean": "Označení místa výroby",
    "eano_ean": "Označení místa spotřeby",
    "hybrid": "hybridní",
    "kWp override": "Nově navržený výkon výrobny",
    "MWh / kWh (dodržet velká a malá písmena)": "MWh / kWh (dodržet velká a malá písmena)",
    "nová položka": "Fotovoltaická elektrárna - fixní složka ceny (výchozí hodnota 0 Kč)",
    "proportional": "proporční",
    "site": "Název odběrného místa",
    "site_group": "skrýt sloupec",
    "total_kwh": "Objem elektřiny",
    "varianty kapacit (kwh)": "Varianty kapacit (kWh)",
    "Účinnost nabíjení ηc": "Účinnost nabíjení η_c (dolní index c, zadávat v %)",
    "Účinnost vybíjení ηd": "Účinnost nabíjení η_d (dolní index d, zadávat v %)",
    "Pořadové číslo řádku s instalovaným výkonem výrobny v csv souboru": "Pořadové číslo řádku s instalovaným výkonem výrobny v csv souboru",
    "Pořadové číslo řádku s názvem objektu v csv souboru": "Pořadové číslo řádku s názvem objektu v csv souboru",
    "Projekt": "Název projektu",
    "Běh": "Název výpočtu",
    "Použít projekt": "Založit nový projekt",
    "Název nového běhu": "Název nového výpočtu",
    "Nový běh": "Založit nový výpočet",
    "Umístění výstupu": "Umístění výstupu",
    "Složka výstupů": "Složka výstupů",
    "+ Založit novou variantu…": "+ Založit novou variantu…",
    "Název nové varianty": "Název nové varianty",
    "Založit novou variantu": "Založit novou variantu",
}

_UI_REMOVE_SET = {
    "Celkové sdílení mezi OM (MWh)",
    "Celkové využití FVE v komunitě (MWh)",
    "Debug demand po sdílení (OM): suma=1,215,599.1 kWh; nenulové řádky=207803; unikátní OM=28",
    "Debug průnik hodin (demand vs export): 8760",
    "Debug surplus: suma=94,590.3 kWh; nenulové hodiny export=1482, curtailed=0",
    "FVE curtailment (MWh)",
    "FVE export (MWh)",
    "FVE sdílení (MWh)",
    "Fáze 1 – Nastavení objektů (omezení + lokální baterie)",
    "Import po OM (pro vybíjení baterie) beru z: derived from eano_after_pv.csv:import_after_kwh - allocations.csv:shared_kwh grouped by to_site",
    "Komunitní baterie je fyzicky v jednom OM. Tady ji nastavíš v aktivní variantě (patch) a pak znovu spustíš scénář 3B.",
    "Komunitní přebytek/export: export_residual_kwh (kwh); curtailment: curtailed_kwh",
    "Lokální bateriový posun (MWh)",
    "Nabití baterie z vlastní FVE (MWh)",
    "Nabití baterie ze sdílené energie (MWh)",
    "Nabití baterií (MWh)",
    "Nabití baterií z komunitních přetoků (MWh)",
    "Nabití centrální baterie z přetoků (MWh)",
    "Nastavení, které ovlivňuje bateriové scénáře a ekonomiku. Logika výpočtů se tím nemění, jen parametry.",
    "Nechat dočasné výstupy citlivosti (debug)",
    "Nákup ze sítě (MWh)",
    "Návrh kapacity pro 300 cyklů/rok:",
    "Ořezaná / zmařená energie (MWh)",
    "Pozn.: Citlivost počítáme nezávisle po místě (ostatní mají cap=0). Bruteforce kombinací přes všechna místa roste exponenciálně (např. 4^10 = 1 048 576 běhů) a nedává smysl.",
    "Přesun energie přes baterie mezi OM (MWh)",
    "Přetok do sítě (MWh)",
    "Přímá vlastní spotřeba FVE (MWh)",
    "Přímé sdílení FVE (MWh)",
    "Přímé sdílení elektřiny (MWh)",
    "Přímé sdílení mezi OM (MWh)",
    "Přímé využití FVE v komunitě (bez baterií) (MWh)",
    "Sdílení přes baterie mezi OM (MWh)",
    "Tip: Můžeš mít společný výkup, ale individuální distribuci – přepínače jsou pro každou složku zvlášť.",
    "Tip: kWp override nech prázdné = použije se původní kWp. aktivní=False = výrobna se pro variantu vypíná.",
    "Upravuješ parametry baterií v aktivní variantě (patch). Potom spusť scénář 3A znovu. Změny se ukládají jen do varianty – base zůstává čistý",
    "Varianta komunitní baterie (3B)",
    "Varianty lokálních baterií (3A)",
    "Vybití baterií (MWh)",
}

def _ui_label(label: str) -> str:
    """Return display label for UI. If label is not mapped, return original."""
    if label is None:
        return ""
    return _UI_LABEL_MAP.get(label, label)

def _w(label: str) -> str:
    """Widget/heading label wrapper."""
    return _ui_label(label)

def _ui_is_removed(name: str) -> bool:
    return name in _UI_REMOVE_SET

def _ui_dedup_columns(cols):
    seen = {}
    out = []
    for c in cols:
        base = c
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base} ({seen[base]+1})")
    return out

def _ui_prepare_df_columns(df):
    """Prepare DataFrame for display: rename columns via UI map, drop removed, deduplicate."""
    import pandas as _pd
    if df is None:
        return df
    if not isinstance(df, _pd.DataFrame):
        return df
    cols = [str(c) for c in df.columns]
    new_cols = []
    for c in cols:
        lbl = _ui_label(c)
        if _ui_is_removed(lbl) or _ui_is_removed(c):
            lbl = None
        new_cols.append(lbl)
    # drop removed (None) columns
    keep_idx = [i for i,lbl in enumerate(new_cols) if lbl is not None]
    df2 = df.iloc[:, keep_idx].copy() if keep_idx else df.copy()
    df2.columns = _ui_dedup_columns([new_cols[i] for i in keep_idx]) if keep_idx else _ui_dedup_columns([_ui_label(c) for c in cols])
    return df2

def _ui_prepare_kpi_df(df):
    """Rename KPI metric names and drop removed ones."""
    import pandas as _pd
    if df is None:
        return df
    d = df.copy()
    # common columns: 'Metrika' or 'Metika'
    for col in ("Metrika", "Metika", "Metric"):
        if col in d.columns:
            mask = ~d[col].astype(str).isin(_UI_REMOVE_SET)
            d = d.loc[mask].copy()
            d[col] = d[col].astype(str).map(lambda x: _UI_LABEL_MAP.get(x, x))
            break
    d.columns = _ui_dedup_columns([_UI_LABEL_MAP.get(str(c), str(c)) for c in d.columns])
    return d

def _ui_prepare_scen_df(df):
    """Drop removed metric columns, rename remaining, ensure unique."""
    if df is None:
        return df
    d = df.copy()
    # keep first column 'Scénář' always
    keep_cols = []
    for c in d.columns:
        if str(c) == "Scénář":
            keep_cols.append(c)
        elif str(c) in _UI_REMOVE_SET:
            continue
        else:
            keep_cols.append(c)
    d = d[keep_cols].copy()
    new_cols = []
    for c in d.columns:
        if str(c) == "Scénář":
            new_cols.append("Scénář")
        else:
            new_cols.append(_UI_LABEL_MAP.get(str(c), str(c)))
    d.columns = _ui_dedup_columns(new_cols)
    return d

def _ui_status_icon(run_path: object, required: list[str], optional: list[str] | None = None) -> str:
    """Return a small status icon for a UI section based on presence of output files in the run folder.

    Accepts:
      - str / Path-like run directory
      - RunPaths (uses .run_dir)
    ✅ = all required present
    🟡 = some present (or any optional present) but not all required
    ⚠️ = nothing present
    """
    # Support passing RunPaths directly (common in this UI)
    if hasattr(run_path, "run_dir"):
        run_path = getattr(run_path, "run_dir")

    try:
        rp = _Path(run_path)  # type: ignore[arg-type]
    except TypeError:
        # Last-resort fallback: stringify
        rp = _Path(str(run_path))

    req_hits = [(rp / f).exists() for f in required]
    opt_hits = [(rp / f).exists() for f in (optional or [])]

    if required and all(req_hits):
        return "✅"
    if any(req_hits) or any(opt_hits):
        return "🟡"
    return "⚠️"


def _ui_section_title(base: str, run_path: object, required: list[str], optional: list[str] | None = None) -> str:
    return f"{_ui_status_icon(run_path, required, optional)} {base}"
    return f"{_ui_status_icon(run_path, required, optional)} {base}"
import streamlit as st

# --- Output location override (UI-only; does not change compute logic) ---
def get_runs_root() -> Path:
    """Return base folder for runs (can be overridden from UI)."""
    try:
        val = st.session_state.get("_runs_root_override")
    except Exception:
        val = None
    if val:
        try:
            return Path(val)
        except Exception:
            return RUNS_ROOT
    return RUNS_ROOT

import altair as alt

# ----------------------------
# Paths
# ----------------------------
APP_ROOT = Path(__file__).resolve().parents[1]            # .../app_komunita
SRC_DIR = APP_ROOT / "src"                               # .../app_komunita/src
PIPELINE_ROOT = APP_ROOT / "src" / "ec_balance" / "pipeline"
RUNS_ROOT = APP_ROOT / "runs"

# ----------------------------
# Helpers
# ----------------------------
@dataclass
class RunPaths:
    run_dir: Path
    csv_dir: Path
    logs_dir: Path

def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def build_env() -> dict:
    """Env for subprocesses so that src-layout imports (src/ec_balance) work."""
    env = os.environ.copy()
    src = str(SRC_DIR)
    cur = env.get("PYTHONPATH", "")
    if cur:
        parts = cur.split(os.pathsep)
        if src not in parts:
            env["PYTHONPATH"] = src + os.pathsep + cur
    else:
        env["PYTHONPATH"] = src
    return env

def ensure_run_dirs(project_name: str, run_name: Optional[str] = None) -> RunPaths:
    """Create a new run folder under the project.

    If run_name is provided, it is sanitized and used as the run directory name.
    If the name already exists, a numeric suffix is appended to keep runs unique.
    """
    base = (run_name or now_stamp()).strip()
    # sanitize for filesystem (Windows-friendly)
    base = re.sub(r"[\\/:*?\"<>|]", "_", base)
    base = re.sub(r"\s+", "_", base).strip("_")
    if not base:
        base = now_stamp()
    run_dir = get_runs_root() / project_name / base
    # keep unique
    if run_dir.exists():
        k = 2
        while (get_runs_root() / project_name / f"{base}_{k}").exists():
            k += 1
        run_dir = get_runs_root() / project_name / f"{base}_{k}"
    csv_dir = run_dir / "csv"
    logs_dir = run_dir / "logs"
    csv_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    return RunPaths(run_dir=run_dir, csv_dir=csv_dir, logs_dir=logs_dir)



def runpaths_from_existing_run(run_dir: Path) -> RunPaths:
    """Build RunPaths for an existing run directory (no creation)."""
    csv_dir = run_dir / "csv"
    logs_dir = run_dir / "logs"
    # Be tolerant: runs created by older versions may miss dirs
    csv_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    return RunPaths(run_dir=run_dir, csv_dir=csv_dir, logs_dir=logs_dir)
def run_cmd(cmd: Union[str, list[str]], *, cwd: Path, log_path: Path) -> bool:
    """Run a command and write stdout/stderr to a log file. Works on Windows."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as f:
        f.write(f"[CMD] {cmd}\n\n")

        if isinstance(cmd, str):
            # shell=True -> Windows handles quoting inside the string
            p = subprocess.run(cmd, cwd=str(cwd), shell=True, text=True, capture_output=True)
        else:
            p = subprocess.run(cmd, cwd=str(cwd), shell=False, text=True, capture_output=True)

        f.write(p.stdout or "")
        if p.stderr:
            f.write("\n--- STDERR ---\n")
            f.write(p.stderr)

    if p.returncode != 0:
        st.error(f"Chyba (rc={p.returncode}). Log: {log_path}")
        return False

    st.success(f"Hotovo ✅ Log: {log_path}")
    return True

def save_upload(uploaded_file, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(uploaded_file.getbuffer())


# -------------------------
# Run-bound inputs & config (UI only; keeps compute logic intact)
# -------------------------
def _run_root(rp: object) -> Path:
    """Return the run root directory as Path for a RunPaths or path-like."""
    if hasattr(rp, "run_dir"):
        return Path(getattr(rp, "run_dir"))
    return Path(rp)  # type: ignore[arg-type]

def _inputs_dir(rp: object) -> Path:
    d = _run_root(rp) / "_inputs"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _config_dir(rp: object) -> Path:
    d = _run_root(rp) / "_config"
    d.mkdir(parents=True, exist_ok=True)
    return d

def save_upload_into_run(uploaded_file, rp: object, filename: str, *, also_copy_to: Path | None = None) -> Path:
    """Save an uploaded file into RUN/_inputs and optionally copy to legacy location."""
    dest = _inputs_dir(rp) / filename
    save_upload(uploaded_file, dest)
    if also_copy_to is not None:
        also_copy_to.parent.mkdir(parents=True, exist_ok=True)
        also_copy_to.write_bytes(dest.read_bytes())
    return dest



def load_inputs_from_run_to_legacy(rp: object, *, filenames: list[str] | None = None) -> list[Path]:
    """Copy files from RUN/_inputs back into legacy CSV directory used by existing steps.

    This enables 'loading inputs from a run' without changing compute logic.
    Returns list of copied destination paths.
    """
    src_dir = _inputs_dir(rp)
    dest_dir = _run_root(rp) / "csv"
    # Prefer rp.csv_dir when available
    if hasattr(rp, "csv_dir"):
        try:
            dest_dir = Path(getattr(rp, "csv_dir"))
        except Exception:
            pass
    copied: list[Path] = []
    if filenames is None:
        files = [p for p in src_dir.iterdir() if p.is_file()]
    else:
        files = [src_dir / fn for fn in filenames]
    dest_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        if not f.exists() or not f.is_file():
            continue
        dest = dest_dir / f.name
        dest.write_bytes(f.read_bytes())
        copied.append(dest)
    return copied


def load_inputs_from_run(src_run: Path | object, dest_rp: object, *, filenames: list[str] | None = None) -> list[Path]:
    """Copy files from src_run/_inputs into dest run (_inputs + legacy csv dir)."""
    src_dir = _inputs_dir(src_run)
    dest_inputs = _inputs_dir(dest_rp)
    dest_csv = _run_root(dest_rp) / 'csv'
    dest_csv.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []
    if filenames is None:
        files = [p for p in src_dir.iterdir() if p.is_file()]
    else:
        files = [src_dir / fn for fn in filenames]
    for f in files:
        if not f.exists() or not f.is_file():
            continue
        d1 = dest_inputs / f.name
        d1.write_bytes(f.read_bytes())
        d2 = dest_csv / f.name
        d2.write_bytes(f.read_bytes())
        copied.append(d1)
    return copied

def list_run_inputs(rp: object) -> list[dict]:
    """List files stored in RUN/_inputs with basic metadata for UI."""
    d = _inputs_dir(rp)
    out = []
    for f in sorted(d.iterdir()):
        if not f.is_file():
            continue
        st_mtime = datetime.fromtimestamp(f.stat().st_mtime)
        out.append({"soubor": f.name, "velikost_kb": round(f.stat().st_size/1024, 1), "změněno": st_mtime.strftime("%Y-%m-%d %H:%M:%S")})
    return out





def list_run_results(run_dir: Path | object) -> list[dict]:
    """List candidate result files in a run (RUN/csv) with metadata for UI."""
    base = _run_root(run_dir)
    csv_dir = base / "csv"
    out = []
    if not csv_dir.exists():
        return out
    for f in sorted(csv_dir.iterdir()):
        if not f.is_file():
            continue
        if f.suffix.lower() not in [".csv", ".json", ".parquet"]:
            continue
        st_mtime = datetime.fromtimestamp(f.stat().st_mtime)
        out.append({
            "soubor": f.name,
            "velikost_kb": round(f.stat().st_size / 1024, 1),
            "změněno": st_mtime.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return out


def load_results_from_run(src_run: Path | object, dest_rp: object, *, filenames: list[str] | None = None) -> list[Path]:
    """Copy result files from src RUN/csv into destination run's csv dir.
    This allows viewing results from another run without recomputation.
    """
    src_base = _run_root(src_run)
    src_csv = src_base / "csv"
    dest_csv = _run_root(dest_rp) / "csv"
    if hasattr(dest_rp, "csv_dir"):
        try:
            dest_csv = Path(getattr(dest_rp, "csv_dir"))
        except Exception:
            pass
    dest_csv.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []
    if not src_csv.exists():
        return copied
    if filenames is None:
        files = [p for p in src_csv.iterdir() if p.is_file() and p.suffix.lower() in [".csv", ".json", ".parquet"]]
    else:
        files = [src_csv / fn for fn in filenames]
    for f in files:
        if not f.exists() or not f.is_file():
            continue
        dest = dest_csv / f.name
        dest.write_bytes(f.read_bytes())
        copied.append(dest)
    return copied



def _copy_selected_files(src_dir: Path, dst_dir: Path, filenames: list[str]) -> tuple[int, list[str]]:
    """Copy selected files (by name) from src_dir to dst_dir.

    Returns (count_copied, missing_or_failed).
    On Windows you can hit WinError 32 if a file is temporarily locked by another process
    (e.g., antivirus/indexer/another Streamlit run). We retry and use an atomic replace.
    """
    src_dir = Path(src_dir)
    dst_dir = Path(dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    missing: list[str] = []

    def _is_locked_error(err: BaseException) -> bool:
        if isinstance(err, PermissionError):
            return True
        if isinstance(err, OSError) and getattr(err, "winerror", None) == 32:
            return True
        return False

    for fn in filenames:
        p = src_dir / fn
        if not p.exists():
            missing.append(fn)
            continue

        dst = dst_dir / fn
        tmp = dst.with_suffix(dst.suffix + ".tmp")

        ok = False
        for attempt in range(6):
            try:
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except Exception:
                        pass
                # copy to temp first (avoid partially-written dst)
                shutil.copy2(p, tmp)
                # atomic replace into destination
                os.replace(tmp, dst)
                ok = True
                break
            except BaseException as e:
                # cleanup tmp if it exists
                try:
                    if tmp.exists():
                        tmp.unlink()
                except Exception:
                    pass

                if _is_locked_error(e) and attempt < 5:
                    time.sleep(0.2 * (attempt + 1))
                    continue

                # treat as failed copy, but don't crash the whole UI flow
                missing.append(f"{fn} (copy failed: {type(e).__name__})")
                ok = False
                break

        if ok:
            copied += 1

    return copied, missing



# ----------------------------
# Battery snapshots (lightweight "variant tagging" for results)
# ----------------------------
def _copy_one_file_atomic(src: Path, dst: Path, attempts: int = 6) -> bool:
    """Copy src -> dst with retries (Windows lock safe) and atomic replace."""
    src = Path(src)
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)

    def _is_locked_error(err: BaseException) -> bool:
        if isinstance(err, PermissionError):
            return True
        if isinstance(err, OSError) and getattr(err, "winerror", None) == 32:
            return True
        return False

    tmp = dst.with_suffix(dst.suffix + ".tmp")
    for attempt in range(attempts):
        try:
            if tmp.exists():
                try:
                    tmp.unlink()
                except Exception:
                    pass
            shutil.copy2(src, tmp)
            os.replace(tmp, dst)
            return True
        except Exception as e:
            if _is_locked_error(e) and attempt < attempts - 1:
                time.sleep(0.15 * (attempt + 1))
                continue
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            return False
    return False

def _battery_snapshots_dir(run_dir: Path) -> Path:
    return Path(run_dir) / "_variants" / "_battery_snapshots"

def list_battery_snapshots(run_dir: Path) -> list[str]:
    d = _battery_snapshots_dir(run_dir)
    if not d.exists():
        return []
    return sorted([p.name for p in d.iterdir() if p.is_dir()])

def save_battery_snapshot(run_dir: Path, name: str, payload: dict, files: list[Path] | None = None) -> Path:
    run_dir = Path(run_dir)
    out_dir = _battery_snapshots_dir(run_dir) / name
    out_dir.mkdir(parents=True, exist_ok=True)

    meta = dict(payload or {})
    meta.setdefault("name", name)
    meta.setdefault("created_at", datetime.now().isoformat(timespec="seconds"))
    (out_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    if files:
        dst_files = out_dir / "files"
        dst_files.mkdir(parents=True, exist_ok=True)
        copied, failed = [], []
        for f in files:
            fp = Path(f)
            if not fp.exists():
                failed.append(fp.name)
                continue
            ok = _copy_one_file_atomic(fp, dst_files / fp.name)
            (copied if ok else failed).append(fp.name)
        (out_dir / "files_index.json").write_text(
            json.dumps({"copied": copied, "missing_or_failed": failed}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return out_dir
def list_projects() -> list[str]:
    """List available project directories under RUNS_ROOT (sorted)."""
    try:
        if not get_runs_root().exists():
            return []
        projects = [p.name for p in RUNS_ROOT.iterdir() if p.is_dir()]
        projects.sort()
        return projects
    except Exception:
        return []



def list_project_runs(project_name: str) -> list[Path]:
    """Return existing run directories for the given project (newest first)."""
    try:
        base = RUNS_ROOT / str(project_name)
        if not base.exists():
            return []
        runs = [p for p in base.iterdir() if p.is_dir()]
        runs.sort(key=lambda p: p.name, reverse=True)
        return runs
    except Exception:
        return []

def run_dir_label(p: Path) -> str:
    """Human-friendly label for a run dir."""
    return p.name

def _cfg_path(rp: object) -> Path:
    return _config_dir(rp) / "run_config.json"

def save_cfg_into_run(rp: object, cfg: dict) -> Path:
    """Persist numeric/settings inputs for this run. No impact on compute steps."""
    path = _cfg_path(rp)
    payload = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "cfg": cfg,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path

def maybe_load_cfg_from_run(rp: object, cfg: dict) -> dict:
    """Load cfg from RUN/_config/run_config.json when switching runs."""
    try:
        path = _cfg_path(rp)
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            loaded = data.get("cfg", {}) if isinstance(data, dict) else {}
            if isinstance(loaded, dict):
                cfg.update(loaded)
    except Exception:
        # Never break UI because of config file issues
        pass
    return cfg


# --- Variants (config sub-scenarios) -----------------------------------------
def _run_dir_from_rp(rp: object) -> Path:
    """Best-effort resolve run directory from rp (supports RunPaths or Path/str)."""
    if rp is None:
        return Path()
    if isinstance(rp, (str, Path)):
        return Path(rp)
    # RunPaths-like
    if hasattr(rp, "run_dir"):
        return Path(getattr(rp, "run_dir"))
    # fallback
    return Path(str(rp))

def _variants_dir(run_dir: Path) -> Path:
    return Path(run_dir) / "_variants"

def _run_state_path(run_dir: Path) -> Path:
    return Path(run_dir) / "_config" / "run_state.json"

def load_run_state(run_dir: Path) -> dict:
    """Load small UI state for the run (active_variant etc.)."""
    try:
        p = _run_state_path(run_dir)
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

def save_run_state(run_dir: Path, state: dict) -> None:
    try:
        p = _run_state_path(run_dir)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(state or {})
        payload["saved_at"] = datetime.now().isoformat(timespec="seconds")
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # never break UI
        pass

def list_run_variants(run_dir: Path) -> list[str]:
    """List variant names under RUN/_variants (excluding hidden)."""
    vdir = _variants_dir(run_dir)
    if not vdir.exists():
        return []
    out: list[str] = []
    for p in vdir.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        if name.startswith("."):
            continue
        out.append(name)
    out.sort()
    return out

def _unique_variant_name(dst_run_dir: Path, desired: str) -> str:
    desired = desired.strip() or "variant"
    vdir = _variants_dir(dst_run_dir)
    vdir.mkdir(parents=True, exist_ok=True)
    cand = desired
    i = 2
    while (vdir / cand).exists():
        cand = f"{desired}_{i}"
        i += 1
    return cand

def copy_variant_from_run(src_run_dir: Path, dst_run_dir: Path, variant_name: str) -> str | None:
    """Copy a variant directory from src to dst. Returns the new variant name in dst."""
    src_run_dir = Path(src_run_dir)
    dst_run_dir = Path(dst_run_dir)
    src_v = _variants_dir(src_run_dir) / variant_name
    if not src_v.exists() or not src_v.is_dir():
        return None
    new_name = _unique_variant_name(dst_run_dir, variant_name)
    dst_v = _variants_dir(dst_run_dir) / new_name
    dst_v.parent.mkdir(parents=True, exist_ok=True)
    # copytree requires dst not exist
    shutil.copytree(src_v, dst_v)
    return new_name

def detect_active_variant(src_run_dir: Path) -> str | None:
    """Best-effort detect active variant from run_state.json or run_config.json."""
    src_run_dir = Path(src_run_dir)
    stt = load_run_state(src_run_dir)
    av = stt.get("active_variant")
    if isinstance(av, str) and av.strip():
        return av.strip()
    # try run_config.json
    try:
        cfgp = src_run_dir / "_config" / "run_config.json"
        if cfgp.exists():
            data = json.loads(cfgp.read_text(encoding="utf-8"))
            cfgd = data.get("cfg", {}) if isinstance(data, dict) else {}
            av2 = cfgd.get("active_variant")
            if isinstance(av2, str) and av2.strip():
                return av2.strip()
    except Exception:
        pass
    return None


def load_variant_patch(run_dir: Path, variant: str) -> dict:
    """Load variant patch.json under RUN/_variants/<variant>/patch.json (best-effort)."""
    try:
        p = _variants_dir(run_dir) / variant / "patch.json"
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

def save_variant_patch(run_dir: Path, variant: str, patch: dict) -> Path:
    """Save variant patch.json under RUN/_variants/<variant>/patch.json (creates dirs)."""
    vdir = _variants_dir(run_dir) / variant
    vdir.mkdir(parents=True, exist_ok=True)
    p = vdir / "patch.json"
    payload = dict(patch or {})
    payload["saved_at"] = datetime.now().isoformat(timespec="seconds")
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return p

def get_active_variant(run_dir: Path, cfg: dict) -> str:
    """Return active variant name; 'base' if none."""
    av = None
    try:
        stt = load_run_state(run_dir)
        av = stt.get("active_variant")
    except Exception:
        av = None
    if not isinstance(av, str) or not av.strip():
        av = cfg.get("active_variant") if isinstance(cfg, dict) else None
    if isinstance(av, str) and av.strip():
        return av.strip()
    return "base"

def set_active_variant(run_dir: Path, cfg: dict, variant: str) -> None:
    """Persist active variant into run_state.json and cfg."""
    variant = (variant or "").strip() or "base"
    try:
        stt = load_run_state(run_dir)
        stt["active_variant"] = variant
        save_run_state(run_dir, stt)
    except Exception:
        pass
    try:
        cfg["active_variant"] = variant
    except Exception:
        pass

def _slug(value: str) -> str:
    """Filesystem-safe slug for site names."""
    import re
    import unicodedata

    s = "" if value is None else str(value)
    # normalize accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.strip().lower()
    # replace non-alnum with underscore
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "site"


def read_csv_auto(path: Path) -> pd.DataFrame:
    """
    Robust CSV reader:
    - auto-detect separator (, ; tab) using sep=None + engine='python'
    - handle BOM (utf-8-sig)
    """
    return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")

def _as_series(df: pd.DataFrame, col_name: str) -> pd.Series:
    """Get a column as Series, even if duplicate column names produce a DataFrame."""
    col = df[col_name]
    if isinstance(col, pd.DataFrame):
        col = col.iloc[:, 0]
    return col

def _pick_col(cols: Iterable[str], prefer: list[str]) -> Optional[str]:
    cols = [str(c) for c in cols]
    for p in prefer:
        for c in cols:
            if p.lower() in c.lower():
                return c
    return None

def load_ean_list(long_csv_path: Path) -> pd.DataFrame:
    """
    Returns a standardized table:
      ean | site | total_kwh (if available)
    """
    df = read_csv_auto(long_csv_path)
    # drop completely empty columns (common with ';;;;')
    df = df.dropna(axis=1, how="all")
    # drop pandas Unnamed columns
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")].copy()

    ean_col = _pick_col(df.columns, ["ean", "eano", "eand"])
    site_col = _pick_col(df.columns, ["site", "objekt", "misto", "om", "name"])
    val_col = _pick_col(df.columns, ["kwh", "mwh", "value", "import", "export", "cons"])

    if ean_col is None:
        # fallback: first non-datetime column
        ean_col = str(df.columns[0])

    if site_col is None and len(df.columns) >= 2:
        site_col = str(df.columns[1])

    ean_s = _as_series(df, ean_col).astype(str).str.strip()
    # pojistka: schovej "Unnamed" a prázdné EANy (vznikají z prázdných hlaviček ve WIDE)
    ean_s = ean_s[~ean_s.str.match(r"(?i)^(unnamed)[:\s]") & (ean_s != "")].copy()

    site_s = _as_series(df, site_col).astype(str).str.strip() if site_col else pd.Series([""] * len(df))

    out = pd.DataFrame({"ean": ean_s, "site": site_s})
    # compute totals if a numeric column exists
    if val_col and val_col in df.columns:
        vals = pd.to_numeric(_as_series(df, val_col), errors="coerce")
        out["total_kwh"] = vals
        out = out.groupby(["ean", "site"], as_index=False)["total_kwh"].sum().sort_values("total_kwh", ascending=False)
    else:
        out = out.drop_duplicates().sort_values(["site", "ean"])

    return out.reset_index(drop=True)


def load_sites_from_any(rp: object) -> list[str]:
    """Best-effort helper to discover available site names for pickers.

    Works with either a RunPaths instance (preferred) or a path-like/string run directory.
    Tries (in order):
      1) structured long/output CSVs that already contain a `site` column
      2) site_map.csv (if present)
      3) wide input CSVs (e.g., eano_wide/eand_wide) via a simple heuristic row scan

    This is intentionally UI-only convenience (no impact on model math).
    """
    try:
        run_dir = getattr(rp, 'run_dir', rp)
        run_dir = Path(run_dir)
    except Exception:
        run_dir = Path(str(rp))

    # Some projects keep CSVs under run_dir/csv and inputs under run_dir/_inputs
    search_dirs = [run_dir, run_dir / "csv", run_dir / "_inputs"]

    # 1) Prefer sources with explicit `site` column
    candidates = [
        # inputs / early steps
        "ean_o_long.csv",
        "ean_d_long.csv",
        "site_map.csv",
        # step outputs (common names)
        "eano_after_pv.csv",
        "eand_after_pv.csv",
        "by_hour_after.csv",
        "by_hour_after_bat_local.csv",
        "by_hour_after_bat_central.csv",
        # per-site hourlies
        "bat_local_by_site_hour.csv",
    ]

    sites: set[str] = set()

    def _add_sites_from_df(df: pd.DataFrame) -> None:
        if "site" in df.columns:
            vals = df["site"].astype(str).str.strip()
            for v in vals.unique().tolist():
                if v and str(v).lower() != "nan":
                    sites.add(str(v))

    for name in candidates:
        for d in search_dirs:
            p = d / name
            if not p.exists():
                continue
            try:
                df = read_csv_auto(p)
            except Exception:
                continue
            try:
                _add_sites_from_df(df)
            except Exception:
                pass

    # 2) If site_map exists but has different column naming, try to extract robustly
    if not sites:
        for d in search_dirs:
            p = d / "site_map.csv"
            if not p.exists():
                continue
            try:
                df = read_csv_auto(p)
            except Exception:
                continue
            for col in ("site", "site_group", "eano_site", "eand_site"):
                if col in df.columns:
                    vals = df[col].astype(str).str.strip()
                    for v in vals.unique().tolist():
                        if v and str(v).lower() != "nan":
                            sites.add(str(v))

    # 3) As a last resort, infer sites from wide input CSVs (pre-step1 runs)
    if not sites:
        wide_names = [
            "eano_wide.csv", "eand_wide.csv",
            "ean_o_wide.csv", "ean_d_wide.csv",
            "EANo_wide.csv", "EANd_wide.csv",
        ]

        seps_to_try = [";", ",", "\t"]

        def _read_head(path: Path, sep: str) -> pd.DataFrame:
            # read a small head without interpreting headers; tolerate ragged rows
            return pd.read_csv(path, sep=sep, header=None, nrows=12, engine="python")

        def _infer_from_wide(path: Path) -> None:
            # try to find a row that looks like: [*, site1, site2, site3, ...]
            best_row = None
            best_count = 0
            for sep in seps_to_try:
                try:
                    dfh = _read_head(path, sep)
                except Exception:
                    continue
                for r in range(min(len(dfh), 12)):
                    row = dfh.iloc[r, :].tolist()
                    # drop first cell (often datetime / label)
                    row_vals = [x for x in row[1:] if x is not None and str(x).strip() not in ("", "nan", "NaN")]
                    # require at least 2 non-empty candidates
                    if len(row_vals) > best_count:
                        best_count = len(row_vals)
                        best_row = row_vals
            if best_row and best_count >= 2:
                for v in best_row:
                    sv = str(v).strip()
                    if sv and sv.lower() not in ("nan", "none"):
                        sites.add(sv)

        for name in wide_names:
            for d in search_dirs:
                p = d / name
                if p.exists():
                    _infer_from_wide(p)
            if sites:
                break

    return sorted(sites)
def ensure_site_map_has_group(site_map: pd.DataFrame) -> pd.DataFrame:
    if "site_group" not in site_map.columns:
        site_map = site_map.copy()
        site_map["site_group"] = ""
    return site_map

def load_site_map(path: Path) -> pd.DataFrame:
    df = read_csv_auto(path)
    df = df.dropna(axis=1, how="all")
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")].copy()
    return ensure_site_map_has_group(df)

def infer_default_pairs(eano: pd.DataFrame, eand: pd.DataFrame) -> pd.DataFrame:
    """
    Heuristic pairing: same 'site' -> pair first matching EANo/EANd for that site.
    Output columns:
      site_group | site | eano_ean | eand_ean | eano_site | eand_site
    """
    rows = []
    # group by site label
    for site, g_o in eano.groupby("site"):
        g_d = eand[eand["site"] == site]
        if g_d.empty:
            # still create rows with empty eand
            for e in g_o["ean"].unique():
                rows.append({"site_group": site, "site": site, "eano_ean": e, "eand_ean": ""})
        else:
            # zip unique lists
            o_list = list(g_o["ean"].unique())
            d_list = list(g_d["ean"].unique())
            n = max(len(o_list), len(d_list))
            for i in range(n):
                rows.append({
                    "site_group": site,
                    "site": site,
                    "eano_ean": o_list[i] if i < len(o_list) else "",
                    "eand_ean": d_list[i] if i < len(d_list) else "",
                })
    if not rows:
        return pd.DataFrame(columns=["site_group", "site", "eano_ean", "eand_ean"])
    return pd.DataFrame(rows)

def to_site_map_csv(df_pairs: pd.DataFrame) -> pd.DataFrame:
    # normalize columns and write a compact site_map
    out = df_pairs.copy()
    for c in ["site_group", "site", "eano_ean", "eand_ean"]:
        if c not in out.columns:
            out[c] = ""
    out = out[["site_group", "site", "eano_ean", "eand_ean"]].copy()
    out["site_group"] = out["site_group"].astype(str).str.strip()
    out["site"] = out["site"].astype(str).str.strip()
    out["eano_ean"] = out["eano_ean"].astype(str).str.strip()
    out["eand_ean"] = out["eand_ean"].astype(str).str.strip()
    # drop fully empty rows
    out = out[~((out["site_group"] == "") & (out["site"] == "") & (out["eano_ean"] == "") & (out["eand_ean"] == ""))]
    return out.reset_index(drop=True)


# ----------------------------
# Overview / reporting helpers
# ----------------------------

_ENERGY_COL_CANDIDATES = [
    "kwh", "value_kwh", "energy_kwh", "import_kwh", "export_kwh",
    "mwh", "value_mwh", "energy_mwh",
]

def _first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _ensure_datetime_col(df: pd.DataFrame, col: str = "datetime") -> pd.DataFrame:
    if col in df.columns and not np.issubdtype(df[col].dtype, np.datetime64):
        df = df.copy()
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def _sum_numeric(series) -> float:
    """Robustní součet numeriky (Series/DataFrame/list/ndarray).

    Některé CSV / rename kroky mohou vrátit DataFrame (např. při duplicitním názvu sloupce).
    V takovém případě sečti všechny jeho sloupce, ať souhrn nespadne.
    """
    if series is None:
        return 0.0
    if isinstance(series, pd.DataFrame):
        total = 0.0
        # Pozor na duplicitní názvy sloupců: series[c] by vrátilo DataFrame znovu.
        # Proto iterujeme po pozici, ať vždy dostaneme 1D Series.
        for i in range(series.shape[1]):
            s = pd.to_numeric(series.iloc[:, i], errors="coerce")
            total += float(np.nan_to_num(s).sum())
        return total
    if isinstance(series, (list, tuple, np.ndarray)):
        s = pd.to_numeric(pd.Series(series), errors="coerce")
        return float(np.nan_to_num(s).sum())
    s = pd.to_numeric(series, errors="coerce")
    return float(np.nan_to_num(s).sum())

def _load_hourly_from_long(path: Path, value_col_candidates: list[str] | None = None) -> pd.DataFrame | None:
    if not path.exists():
        return None
    df = read_csv_auto(path)
    if "datetime" not in df.columns:
        return None
    df = _ensure_datetime_col(df, "datetime")
    if value_col_candidates is None:
        value_col_candidates = _ENERGY_COL_CANDIDATES + ["value"]
    vcol = _first_existing_col(df, value_col_candidates)
    if vcol is None:
        # pick first numeric col that isn't datetime/site-ish
        num_cols = [c for c in df.columns if c not in ("datetime", "site", "ean", "ean_id", "site_group")]
        num_cols = [c for c in num_cols if pd.api.types.is_numeric_dtype(df[c]) or df[c].dtype == object]
        vcol = num_cols[0] if num_cols else None
    if vcol is None:
        return None
    df[vcol] = pd.to_numeric(df[vcol], errors="coerce")
    out = df.groupby("datetime", as_index=False)[vcol].sum().rename(columns={vcol: "value_kwh"})
    return out

def _load_hourly_aggregate(path: Path) -> pd.DataFrame | None:
    """Loads already-aggregated by-hour CSVs (no site)."""
    if not path.exists():
        return None
    df = read_csv_auto(path)
    if "datetime" not in df.columns:
        return None
    df = _ensure_datetime_col(df, "datetime")
    return df

def _monthly(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series(dtype=float)
    d = _ensure_datetime_col(df, "datetime")
    s = pd.to_numeric(_as_series(d, col), errors="coerce").fillna(0.0)
    return s.groupby(d["datetime"].dt.to_period("M")).sum().astype(float)

def _safe_get(df: pd.DataFrame | None, col: str, default: float = 0.0) -> float:
    if df is None or df.empty or col not in df.columns:
        return default
    return _sum_numeric(df[col])

def _derive_charge_from_soc(df: pd.DataFrame | None) -> float:
    if df is None or df.empty or "soc_kwh" not in df.columns:
        return 0.0
    s = pd.to_numeric(_as_series(df, "soc_kwh"), errors="coerce").fillna(0.0)
    ds = s.diff().fillna(0.0)
    return float(ds.clip(lower=0.0).sum())

def _derive_discharge_from_soc(df: pd.DataFrame | None) -> float:
    if df is None or df.empty or "soc_kwh" not in df.columns:
        return 0.0
    s = pd.to_numeric(_as_series(df, "soc_kwh"), errors="coerce").fillna(0.0)
    ds = s.diff().fillna(0.0)
    ch = pd.to_numeric(_as_series(df, "batt_charge_kwh"), errors="coerce").fillna(0.0) if "batt_charge_kwh" in df.columns else pd.Series(0.0, index=df.index)
    if "batt_discharge_kwh" in df.columns:
        d = pd.to_numeric(_as_series(df, "batt_discharge_kwh"), errors="coerce").fillna(0.0)
        return float(d.clip(lower=0.0).sum())
    # ΔSOC = charge - discharge  => discharge = charge - ΔSOC
    return float((ch - ds).clip(lower=0.0).sum())

def _to_mwh(x_kwh: float) -> float:
    return float(x_kwh) / 1000.0

def compute_overview(rp: "RunPaths") -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, pd.Series]]:
    """
    Returns:
      kpi_df: rows = metrics, cols = scenarios
      hourly: scenario -> hourly dataframe with normalized columns where possible
      monthly: metric_key -> dataframe series per scenario (built later)
    """
    scenarios = {
        "Původní": {},
        "Po FVE": {},
        "Po sdílení": {},
        "Po bat. centrální": {},
        "Po bat. lokální": {},
    }

    # ---- Load key inputs ----
    # Baseline consumption: prefer ean_o_long.csv (raw), fallback to eano_after_pv.csv (import after pv) if raw missing
    p_eano_long = rp.csv_dir / "ean_o_long.csv"
    base = _load_hourly_from_long(p_eano_long)
    if base is None:
        # last resort: sum of import_after_kwh after PV as "consumption proxy"
        base = _load_hourly_from_long(rp.csv_dir / "eano_after_pv.csv", ["import_after_kwh"])
    if base is not None:
        scenarios["Původní"]["cons_kwh"] = base.rename(columns={"value_kwh": "cons_kwh"})
        scenarios["Původní"]["import_kwh"] = scenarios["Původní"]["cons_kwh"][["datetime", "cons_kwh"]].rename(columns={"cons_kwh": "import_kwh"})

    # After PV
    pv_import = _load_hourly_from_long(rp.csv_dir / "eano_after_pv.csv", ["import_after_kwh"])
    pv_export = _load_hourly_from_long(rp.csv_dir / "eand_after_pv.csv", ["export_after_kwh"])
    pv_curt = _load_hourly_from_long(rp.csv_dir / "eand_after_pv.csv", ["curtailed_kwh"])
    pv_self = _load_hourly_from_long(rp.csv_dir / "local_selfcons.csv", ["selfcons_kwh", "local_selfcons_kwh", "selfcons_after_kwh"])
    scenarios["Po FVE"]["import_kwh"] = pv_import.rename(columns={"value_kwh": "import_kwh"}) if pv_import is not None else None
    scenarios["Po FVE"]["export_kwh"] = pv_export.rename(columns={"value_kwh": "export_kwh"}) if pv_export is not None else None
    scenarios["Po FVE"]["curtailed_kwh"] = pv_curt.rename(columns={"value_kwh": "curtailed_kwh"}) if pv_curt is not None else None
    scenarios["Po FVE"]["selfcons_kwh"] = pv_self.rename(columns={"value_kwh": "selfcons_kwh"}) if pv_self is not None else None

    # After sharing (aggregated)
    by_after = _load_hourly_aggregate(rp.csv_dir / "by_hour_after.csv")
    # Normalize possible column names
    if by_after is not None:
        # Try to map columns
        col_map = {}
        if "import_residual_kwh" in by_after.columns: col_map["import_residual_kwh"] = "import_kwh"
        if "import_after_share_kwh" in by_after.columns and "import_kwh" not in col_map.values(): col_map["import_after_share_kwh"] = "import_kwh"
        if "import_after_kwh" in by_after.columns and "import_kwh" not in col_map.values(): col_map["import_after_kwh"] = "import_kwh"

        if "export_residual_kwh" in by_after.columns: col_map["export_residual_kwh"] = "export_kwh"
        if "export_after_share_kwh" in by_after.columns and "export_kwh" not in col_map.values(): col_map["export_after_share_kwh"] = "export_kwh"
        if "export_after_kwh" in by_after.columns and "export_kwh" not in col_map.values(): col_map["export_after_kwh"] = "export_kwh"

        if "curtailed_kwh" in by_after.columns: col_map["curtailed_kwh"] = "curtailed_kwh"
        if "shared_kwh" in by_after.columns: col_map["shared_kwh"] = "shared_kwh"
        if "shared_after_kwh" in by_after.columns and "shared_kwh" not in col_map.values(): col_map["shared_after_kwh"] = "shared_kwh"
        norm = by_after.rename(columns=col_map).copy()
        # Pokud by_hour_after.csv neobsahuje shared_kwh, dopočteme ho z allocations.csv (když existuje)
        if "shared_kwh" not in norm.columns:
            try:
                p_alloc2 = None
                for name in ["allocations.csv", "share_allocations.csv", "sharing_allocations.csv", "allocations_long.csv"]:
                    p = rp.csv_dir / name
                    if p.exists() and p.is_file():
                        p_alloc2 = p
                        break
                if p_alloc2 is not None:
                    a = read_csv_auto(p_alloc2)
                    # datetime
                    if "datetime" in a.columns:
                        a["datetime"] = pd.to_datetime(a["datetime"], errors="coerce")
                    elif "t" in a.columns:
                        a["datetime"] = pd.to_datetime(a["t"], errors="coerce")
                    elif "time" in a.columns:
                        a["datetime"] = pd.to_datetime(a["time"], errors="coerce")
                    elif "timestamp" in a.columns:
                        a["datetime"] = pd.to_datetime(a["timestamp"], errors="coerce")
                    a = a.dropna(subset=["datetime"]).copy()

                    # shared col: vezmeme první smysluplný shared*kwh (ale ne "stored")
                    shared_col = None
                    for c in a.columns:
                        cl = str(c).lower()
                        if ("shared" in cl) and ("kwh" in cl) and ("stored" not in cl):
                            shared_col = c
                            break
                    if shared_col is not None:
                        a[shared_col] = pd.to_numeric(_as_series(a, shared_col), errors="coerce").fillna(0.0)
                        shared_ts = a.groupby("datetime")[shared_col].sum().sort_index()

                        norm = norm.copy()
                        if "datetime" in norm.columns:
                            norm["datetime"] = pd.to_datetime(norm["datetime"], errors="coerce")
                            norm["shared_kwh"] = shared_ts.reindex(norm["datetime"], fill_value=0.0).to_numpy()
            except Exception:
                pass

        scenarios["Po sdílení"]["by_hour"] = norm

    # After local battery (aggregated)
    bh_local = _load_hourly_aggregate(rp.csv_dir / "by_hour_after_bat_local.csv")
    if bh_local is not None:
        # expected cols can vary by version -> robust mapping
        col_map = {}
        for c in ["import_after_batt_kwh", "import_after_bat_local_kwh", "import_after_battery_kwh", "import_after_kwh", "import_kwh"]:
            if c in bh_local.columns:
                col_map[c] = "import_kwh"
                break
        for c in ["export_after_batt_kwh", "export_after_bat_local_kwh", "export_after_battery_kwh", "export_after_kwh", "export_kwh"]:
            if c in bh_local.columns:
                col_map[c] = "export_kwh"
                break
        for c in ["curtailment_after_batt_kwh", "curtailed_after_batt_kwh", "curtailed_after_kwh", "curtailed_kwh"]:
            if c in bh_local.columns:
                col_map[c] = "curtailed_kwh"
                break
        # --- LOKÁLNÍ BATERIE: preferuj nové jednoznačné sloupce ---
        # charge (vstup do baterií)
        for c in ["batt_charge_from_local_export_kwh", "batt_charge_own_kwh", "charge_own_kwh"]:
            if c in bh_local.columns:
                col_map[c] = "batt_charge_own_kwh"
                break
        for c in ["batt_charge_from_pool_export_kwh", "batt_charge_shared_kwh", "charge_shared_kwh"]:
            if c in bh_local.columns:
                col_map[c] = "batt_charge_shared_kwh"
                break
        if "batt_charge_total_kwh" in bh_local.columns:
            col_map["batt_charge_total_kwh"] = "batt_charge_kwh"
        elif "batt_charge_kwh" in bh_local.columns:
            col_map["batt_charge_kwh"] = "batt_charge_kwh"
        elif "charge_kwh" in bh_local.columns and "batt_charge_own_kwh" not in col_map.values() and "batt_charge_shared_kwh" not in col_map.values():
            col_map["charge_kwh"] = "batt_charge_own_kwh"
        # discharge (výstup z baterií) – staré *_stored_kwh jsou historicky matoucí aliasy discharge
        for c in ["batt_discharge_own_kwh", "own_stored_kwh", "local_stored_kwh"]:
            if c in bh_local.columns:
                col_map[c] = "batt_discharge_own_kwh"
                break
        for c in ["batt_discharge_shared_kwh", "shared_stored_kwh"]:
            if c in bh_local.columns:
                col_map[c] = "batt_discharge_shared_kwh"
                break
        for c in ["batt_discharge_total_kwh", "discharge_kwh", "batt_discharge_kwh"]:
            if c in bh_local.columns:
                col_map[c] = "batt_discharge_kwh"
                break
        for c in ["soc_kwh", "battery_soc_kwh"]:
            if c in bh_local.columns:
                col_map[c] = "soc_kwh"
                break
        norm = bh_local.rename(columns=col_map).copy()
        # některé verze by-hour lokální baterie nevrací curtailment; použij baseline po FVE jako fallback (lepší než 0)
        if ("curtailed_kwh" not in norm.columns):
            try:
                _cur = None
                if scenarios["Po sdílení"].get("by_hour") is not None and "curtailed_kwh" in scenarios["Po sdílení"]["by_hour"].columns:
                    _cur = scenarios["Po sdílení"]["by_hour"][["datetime", "curtailed_kwh"]].copy()
                elif scenarios["Po FVE"].get("curtailed_kwh") is not None:
                    _cur = scenarios["Po FVE"]["curtailed_kwh"][["datetime", "curtailed_kwh"]].copy()
                if _cur is not None:
                    norm = norm.merge(_cur, on="datetime", how="left")
            except Exception:
                pass
        scenarios["Po bat. lokální"]["by_hour"] = norm

    # After central battery (aggregated)
    bh_central = _load_hourly_aggregate(rp.csv_dir / "by_hour_after_bat_central.csv")
    if bh_central is not None:
        col_map = {}
        if "import_after_batt_kwh" in bh_central.columns: col_map["import_after_batt_kwh"] = "import_kwh"
        if "export_after_batt_kwh" in bh_central.columns: col_map["export_after_batt_kwh"] = "export_kwh"
        if "curtailed_kwh" in bh_central.columns: col_map["curtailed_kwh"] = "curtailed_kwh"
        if "curtailed_after_batt_kwh" in bh_central.columns: col_map["curtailed_after_batt_kwh"] = "curtailed_kwh"
        if "charge_kwh" in bh_central.columns: col_map["charge_kwh"] = "batt_charge_kwh"
        if "batt_charge_kwh" in bh_central.columns: col_map["batt_charge_kwh"] = "batt_charge_kwh"
        if "discharge_kwh" in bh_central.columns: col_map["discharge_kwh"] = "batt_discharge_kwh"
        if "batt_discharge_kwh" in bh_central.columns: col_map["batt_discharge_kwh"] = "batt_discharge_kwh"
        norm = bh_central.rename(columns=col_map).copy()
        scenarios["Po bat. centrální"]["by_hour"] = norm

    # ---- Build hourly per scenario with best-known columns ----
    hourly = {}

    # Baseline
    if "import_kwh" in scenarios["Původní"]:
        df = scenarios["Původní"]["import_kwh"].copy()
        df = df.rename(columns={"import_kwh": "import_kwh"})
        hourly["Původní"] = df

    # After PV
    df = None
    if scenarios["Po FVE"].get("import_kwh") is not None:
        df = scenarios["Po FVE"]["import_kwh"].rename(columns={"import_kwh": "import_kwh"}).copy()
    if df is not None:
        for part_key, out_col in [("export_kwh", "export_kwh"), ("curtailed_kwh", "curtailed_kwh"), ("selfcons_kwh", "selfcons_kwh")]:
            part = scenarios["Po FVE"].get(part_key)
            if part is not None:
                df = df.merge(part[["datetime", out_col]].rename(columns={out_col: out_col}), on="datetime", how="left")
        hourly["Po FVE"] = df

    # After sharing
    if scenarios["Po sdílení"].get("by_hour") is not None:
        hourly["Po sdílení"] = scenarios["Po sdílení"]["by_hour"][["datetime"] + [c for c in ("import_kwh","export_kwh","curtailed_kwh","shared_kwh") if c in scenarios["Po sdílení"]["by_hour"].columns]].copy()

    # After local batt
    if scenarios["Po bat. lokální"].get("by_hour") is not None:
        h = scenarios["Po bat. lokální"]["by_hour"].copy()
        cols = ["datetime"] + [c for c in ("import_kwh","export_kwh","curtailed_kwh","batt_charge_kwh","batt_discharge_kwh","batt_charge_own_kwh","batt_charge_shared_kwh","batt_discharge_own_kwh","batt_discharge_shared_kwh","soc_kwh") if c in h.columns]
        hourly["Po bat. lokální"] = h[cols].copy()

    # After central batt
    if scenarios["Po bat. centrální"].get("by_hour") is not None:
        h = scenarios["Po bat. centrální"]["by_hour"].copy()
        cols = ["datetime"] + [c for c in ("import_kwh","export_kwh","curtailed_kwh","batt_charge_kwh","batt_discharge_kwh","soc_kwh") if c in h.columns]
        hourly["Po bat. centrální"] = h[cols].copy()

    # ---- Totals / KPI table ----
    def tot(scen: str, col: str) -> float:
        df = hourly.get(scen)
        if df is None or col not in df.columns:
            return float("nan")
        # Duplicity názvů po rename (např. alias + kanonický sloupec) nesmí nafouknout KPI.
        # Pro KPI bereme první výskyt kanonického sloupce; detailní rozpad se řeší explicitně.
        return _sum_numeric(_as_series(df, col))

    # PV generation: best from PV stage
    pv_gen_kwh = float("nan")
    if "Po FVE" in hourly:
        pv_self_kwh = _safe_get(hourly["Po FVE"], "selfcons_kwh", 0.0)
        pv_exp_kwh = _safe_get(hourly["Po FVE"], "export_kwh", 0.0)
        pv_cur_kwh = _safe_get(hourly["Po FVE"], "curtailed_kwh", 0.0)
        if (pv_self_kwh + pv_exp_kwh + pv_cur_kwh) > 0:
            pv_gen_kwh = pv_self_kwh + pv_exp_kwh + pv_cur_kwh

    def pv_used(scen: str) -> float:
        if np.isfinite(pv_gen_kwh):
            exp = tot(scen, "export_kwh")
            cur = tot(scen, "curtailed_kwh")
            if np.isfinite(exp) and np.isfinite(cur):
                return pv_gen_kwh - exp - cur
        return float("nan")

    # Sharing volumes: Step3 peer-to-peer sharing + battery-related cross-OM transfers (pokud je umíme odhadnout)
    shared_after = tot("Po sdílení", "shared_kwh")

    # Definice "Sdílení" / "FVE sdílení" ve scénářích:
    # pouze přímé peer-to-peer sdílení (allocations/shared_kwh).
    # Toky přes baterie vykazujeme samostatně, aby se nedvojily v bilanci FVE.
    shared_after_local = shared_after
    shared_after_central = shared_after


    # Lokální baterie – rozpad nabití (vlastní FVE vs ze sdílení); bezpečné fallbacky
    local_stored_own_kwh = tot("Po bat. lokální", "batt_charge_own_kwh")
    local_stored_shared_kwh = tot("Po bat. lokální", "batt_charge_shared_kwh")
    local_stored_total_kwh = tot("Po bat. lokální", "batt_charge_kwh")
    if not np.isfinite(local_stored_own_kwh):
        local_stored_own_kwh = float("nan")
    if not np.isfinite(local_stored_shared_kwh):
        local_stored_shared_kwh = 0.0
    if (not np.isfinite(local_stored_own_kwh)) and np.isfinite(local_stored_total_kwh):
        local_stored_own_kwh = max(0.0, local_stored_total_kwh - float(local_stored_shared_kwh))
    if (not np.isfinite(local_stored_own_kwh)) and np.isfinite(local_stored_total_kwh):
        local_stored_own_kwh = local_stored_total_kwh
    if not np.isfinite(local_stored_own_kwh):
        local_stored_own_kwh = 0.0
    if not np.isfinite(local_stored_shared_kwh):
        local_stored_shared_kwh = 0.0

    central_charge_kwh = tot("Po bat. centrální", "batt_charge_kwh")
    if not np.isfinite(central_charge_kwh):
        central_charge_kwh = _derive_charge_from_soc(hourly.get("Po bat. centrální"))

    # Installed capacities
    pv_kwp_total = float("nan")
    p_kwp = rp.csv_dir / "kwp_by_site.csv"
    if p_kwp.exists():
        kwp_df = read_csv_auto(p_kwp)
        for c in ("kwp", "kWp", "kwp_total", "power_kwp"):
            if c in kwp_df.columns:
                pv_kwp_total = float(pd.to_numeric(_as_series(kwp_df, c), errors="coerce").fillna(0.0).sum())
                break

    batt_local_cap_total = float("nan")
    p_bl = rp.csv_dir / "bat_local_cap_by_site.csv"
    if p_bl.exists():
        dfb = read_csv_auto(p_bl)
        if "cap_kwh" in dfb.columns:
            batt_local_cap_total = float(pd.to_numeric(_as_series(dfb, "cap_kwh"), errors="coerce").fillna(0.0).sum())

    batt_central_cap = float("nan")
    p_bc = rp.csv_dir / "central_batt_config.csv"
    if p_bc.exists():
        dfc = read_csv_auto(p_bc)
        if "cap_kwh" in dfc.columns:
            batt_central_cap = float(pd.to_numeric(_as_series(dfc, "cap_kwh"), errors="coerce").fillna(0.0).iloc[0])
    if not np.isfinite(batt_central_cap) and "Po bat. centrální" in hourly and "soc_kwh" in hourly["Po bat. centrální"].columns:
        batt_central_cap = float(pd.to_numeric(_as_series(hourly["Po bat. centrální"], "soc_kwh"), errors="coerce").fillna(0.0).max())

    metrics = [
        ("spotreba_mwh", "Spotřeba elektřiny", _to_mwh(tot("Původní", "import_kwh"))),
        ("import0_mwh", "Původní objem nakupované elektřiny", _to_mwh(tot("Původní", "import_kwh"))),
        ("import_pv_mwh", "Objem nakupované elektřiny po instalaci FVE", _to_mwh(tot("Po FVE", "import_kwh"))),
        ("import_share_mwh", "Objem nakupované elektřiny po zavedení sdílení", _to_mwh(tot("Po sdílení", "import_kwh"))),
        ("import_central_mwh", "Objem nakupované elektřiny po instalaci baterie v centrálním provedení", _to_mwh(tot("Po bat. centrální", "import_kwh"))),
        ("import_local_mwh", "Objem nakupované elektřiny po instalaci baterie v lokálním provedení", _to_mwh(tot("Po bat. lokální", "import_kwh"))),

        ("pv_gen_mwh", "Výroba elektřiny z FVE", _to_mwh(pv_gen_kwh) if np.isfinite(pv_gen_kwh) else float("nan")),
        ("pv_used_mwh", "Využití elektřiny z FVE", _to_mwh(pv_used("Po FVE"))),
        ("pv_used_share_mwh", "Využití elektřiny z FVE po zavedení sdílení", _to_mwh(pv_used("Po sdílení"))),
        ("pv_used_central_mwh", "Využití elektřiny z FVE po instalaci baterie v centrálním provedení", _to_mwh(pv_used("Po bat. centrální"))),
        ("pv_used_local_mwh", "Využití elektřiny z FVE po instalaci baterie v lokálním provedení", _to_mwh(pv_used("Po bat. lokální"))),

        ("shared_mwh", "Objem sdílené elektřiny po zavedení sdílení", _to_mwh(shared_after) if np.isfinite(shared_after) else float("nan")),
        ("shared_central_mwh", "Objem sdílené elektřiny po instalaci baterie v centrálním provedení", _to_mwh(shared_after_central) if np.isfinite(shared_after_central) else float("nan")),
        ("shared_local_mwh", "Objem sdílené elektřiny po instalaci baterie v lokálním provedení", _to_mwh(shared_after_local) if np.isfinite(shared_after_local) else float("nan")),

        ("curt_mwh", "Objem ořezané/zmařené elektřiny", _to_mwh(tot("Po FVE", "curtailed_kwh"))),
        ("curt_share_mwh", "Objem ořezané/zmařené elektřiny po zavedení sdílení", _to_mwh(tot("Po sdílení", "curtailed_kwh"))),
        ("curt_central_mwh", "Objem ořezané/zmařené elektřiny po instalaci baterie v centrálním provedení", _to_mwh(tot("Po bat. centrální", "curtailed_kwh"))),
        ("curt_local_mwh", "Objem ořezané/zmařené elektřiny po instalaci baterie v lokálním provedení", _to_mwh(tot("Po bat. lokální", "curtailed_kwh"))),

        ("export_mwh", "Objem přetoků z FVE", _to_mwh(tot("Po FVE", "export_kwh"))),
        ("export_share_mwh", "Objem přetoků z FVE po zavedení sdílení", _to_mwh(tot("Po sdílení", "export_kwh"))),
        ("export_central_mwh", "Objem přetoků z FVE po instalaci baterie v centrálním provedení", _to_mwh(tot("Po bat. centrální", "export_kwh"))),
        ("export_local_mwh", "Objem přetoků z FVE po instalaci baterie v lokálním provedení", _to_mwh(tot("Po bat. lokální", "export_kwh"))),

        ("batt_local_store_mwh", "Objem elektřiny uložené lokálně do baterie", _to_mwh(local_stored_own_kwh)),
        ("batt_other_store_mwh", "Objem elektřiny uložené do baterie v jiném odběrném místě", _to_mwh(local_stored_shared_kwh)),

        ("pv_kwp", "Celkový instalovaný výkon FVE (kWp)", pv_kwp_total),
        ("batt_central_cap_kwh", "Celková instalovaná kapacita baterie – centrální (kWh)", batt_central_cap),
        ("batt_local_cap_kwh", "Celková instalovaná kapacita baterek – lokální (kWh)", batt_local_cap_total),
    ]

    # Build table with scenarios where relevant
    # We'll present as a single column "Hodnota" (since metrics already scenario-specific),
    # and also show a compact scenario matrix for import/export/curtail/share.
    kpi_rows = [{"Metika": name, "Hodnota": val} for _, name, val in metrics]
    kpi_df = pd.DataFrame(kpi_rows)

    # Scenario matrix (energy MWh)
    scen_df = pd.DataFrame({
        "Původní": {
            "Import (MWh)": _to_mwh(tot("Původní","import_kwh")),
            "Export (MWh)": 0.0,
            "Curtailment (MWh)": 0.0,
            "Sdílení (MWh)": 0.0,
            "Výroba FVE (MWh)": 0.0,
            "Vlastní spotřeba FVE (MWh)": 0.0,
            "FVE export (MWh)": 0.0,
            "FVE curtailment (MWh)": 0.0,
            "FVE sdílení (MWh)": 0.0,
        },
        "Po FVE": {
            "Import (MWh)": _to_mwh(tot("Po FVE","import_kwh")),
            "Export (MWh)": _to_mwh(tot("Po FVE","export_kwh")),
            "Curtailment (MWh)": _to_mwh(tot("Po FVE","curtailed_kwh")),
            "Sdílení (MWh)": 0.0,
            "Výroba FVE (MWh)": _to_mwh(pv_gen_kwh) if np.isfinite(pv_gen_kwh) else float("nan"),
            "Vlastní spotřeba FVE (MWh)": _to_mwh(pv_used("Po FVE")),
            "FVE export (MWh)": _to_mwh(tot("Po FVE","export_kwh")),
            "FVE curtailment (MWh)": _to_mwh(tot("Po FVE","curtailed_kwh")),
            "FVE sdílení (MWh)": 0.0,
        },
        "Po sdílení": {
            "Import (MWh)": _to_mwh(tot("Po sdílení","import_kwh")),
            "Export (MWh)": _to_mwh(tot("Po sdílení","export_kwh")),
            "Curtailment (MWh)": _to_mwh(tot("Po sdílení","curtailed_kwh")),
            "Sdílení (MWh)": _to_mwh(shared_after) if np.isfinite(shared_after) else float("nan"),
            "Výroba FVE (MWh)": _to_mwh(pv_gen_kwh) if np.isfinite(pv_gen_kwh) else float("nan"),
            "Vlastní spotřeba FVE (MWh)": _to_mwh(pv_used("Po sdílení")),
            "FVE export (MWh)": _to_mwh(tot("Po sdílení","export_kwh")),
            "FVE curtailment (MWh)": _to_mwh(tot("Po sdílení","curtailed_kwh")),
            "FVE sdílení (MWh)": _to_mwh(shared_after) if np.isfinite(shared_after) else float("nan"),
        },
        "Po bat. centrální": {
            "Import (MWh)": _to_mwh(tot("Po bat. centrální","import_kwh")),
            "Export (MWh)": _to_mwh(tot("Po bat. centrální","export_kwh")),
            "Curtailment (MWh)": _to_mwh(tot("Po bat. centrální","curtailed_kwh")),
            "Sdílení (MWh)": _to_mwh(shared_after_central) if np.isfinite(shared_after_central) else float("nan"),
            "Výroba FVE (MWh)": _to_mwh(pv_gen_kwh) if np.isfinite(pv_gen_kwh) else float("nan"),
            "Vlastní spotřeba FVE (MWh)": _to_mwh(pv_used("Po bat. centrální")),
            "FVE export (MWh)": _to_mwh(tot("Po bat. centrální","export_kwh")),
            "FVE curtailment (MWh)": _to_mwh(tot("Po bat. centrální","curtailed_kwh")),
            "FVE sdílení (MWh)": _to_mwh(shared_after_central) if np.isfinite(shared_after_central) else float("nan"),
        },
        "Po bat. lokální": {
            "Import (MWh)": _to_mwh(tot("Po bat. lokální","import_kwh")),
            "Export (MWh)": _to_mwh(tot("Po bat. lokální","export_kwh")),
            "Curtailment (MWh)": _to_mwh(tot("Po bat. lokální","curtailed_kwh")),
            "Sdílení (MWh)": _to_mwh(shared_after_local) if np.isfinite(shared_after_local) else float("nan"),
            "Výroba FVE (MWh)": _to_mwh(pv_gen_kwh) if np.isfinite(pv_gen_kwh) else float("nan"),
            "Vlastní spotřeba FVE (MWh)": _to_mwh(pv_used("Po bat. lokální")),
            "FVE export (MWh)": _to_mwh(tot("Po bat. lokální","export_kwh")),
            "FVE curtailment (MWh)": _to_mwh(tot("Po bat. lokální","curtailed_kwh")),
            "FVE sdílení (MWh)": _to_mwh(shared_after_local) if np.isfinite(shared_after_local) else float("nan"),
        },
    }).T

    # Doplňkové scénářové ukazatele (oddělené od přímého sdílení, aby se nemíchaly toky přes baterii)
    try:
        scen_df["Nabití lokálních baterií z vlastní FVE (MWh)"] = 0.0
        scen_df["Nabití lokálních baterií ze sdílení (MWh)"] = 0.0
        scen_df["Nabití centrální baterie z přetoků (MWh)"] = 0.0
        if "Po bat. lokální" in scen_df.index:
            scen_df.loc["Po bat. lokální", "Nabití lokálních baterií z vlastní FVE (MWh)"] = _to_mwh(local_stored_own_kwh)
            scen_df.loc["Po bat. lokální", "Nabití lokálních baterií ze sdílení (MWh)"] = _to_mwh(local_stored_shared_kwh)
        if "Po bat. centrální" in scen_df.index and np.isfinite(central_charge_kwh):
            scen_df.loc["Po bat. centrální", "Nabití centrální baterie z přetoků (MWh)"] = _to_mwh(central_charge_kwh)
        scen_df["Přímé sdílení FVE (MWh)"] = scen_df.get("FVE sdílení (MWh)")
        scen_df["Přímé sdílení mezi OM (MWh)"] = scen_df.get("FVE sdílení (MWh)")
    except Exception:
        pass

    # Zajisti konzistentní FVE rozpad ve scénářové tabulce (bez přestřelení nad výrobu).
    # "FVE sdílení" zde reprezentuje pouze přímé peer-to-peer sdílení (nikoli bateriové přesuny).
    fve_cols = {"Výroba FVE (MWh)", "FVE export (MWh)", "FVE curtailment (MWh)", "FVE sdílení (MWh)"}
    if fve_cols.issubset(set(scen_df.columns)):
        for c in list(fve_cols):
            scen_df[c] = pd.to_numeric(_as_series(scen_df, c), errors="coerce")
        _gen = scen_df["Výroba FVE (MWh)"].fillna(0.0)
        _exp = scen_df["FVE export (MWh)"].fillna(0.0)
        _cur = scen_df["FVE curtailment (MWh)"].fillna(0.0)
        _shr = scen_df["FVE sdílení (MWh)"].fillna(0.0).clip(lower=0.0)
        _used = (_gen - _exp - _cur).clip(lower=0.0)
        _shr = np.minimum(_shr, _used)
        scen_df["FVE sdílení (MWh)"] = _shr
        # Reziduální dopočet (bez bateriových toků) ponecháme jako pracovní hodnotu,
        # ale pro bateriové scénáře níže přepíšeme "Vlastní spotřeba FVE" na pairing baseline.
        _own_fve_direct = (_used - _shr).clip(lower=0.0)
        scen_df["Vlastní spotřeba FVE (MWh)"] = _own_fve_direct
        scen_df["Přímá vlastní spotřeba FVE (MWh)"] = _own_fve_direct

        # Krok 4a/4b běží ze stavu po pairingu, proto přímá vlastní spotřeba FVE se po baterii
        # nemá měnit reziduálním výpočtem z after-battery exportu/importu. Drž ji na baseline "Po FVE".
        if "Po FVE" in scen_df.index:
            _pairing_own = pd.to_numeric(_as_series(scen_df, "Vlastní spotřeba FVE (MWh)"), errors="coerce")
            try:
                _pairing_own_val = float(_pairing_own.loc["Po FVE"])
            except Exception:
                _pairing_own_val = np.nan
            if np.isfinite(_pairing_own_val):
                for _sc in ["Po bat. centrální", "Po bat. lokální"]:
                    if _sc in scen_df.index:
                        scen_df.loc[_sc, "Vlastní spotřeba FVE (MWh)"] = _pairing_own_val
                        scen_df.loc[_sc, "Přímá vlastní spotřeba FVE (MWh)"] = _pairing_own_val

    # Sjednocené porovnávací metriky (stejné pro lokální i centrální variantu)
    try:
        for _c in ["Import (MWh)", "Export (MWh)", "Curtailment (MWh)", "Výroba FVE (MWh)", "FVE export (MWh)", "FVE curtailment (MWh)", "FVE sdílení (MWh)"]:
            if _c in scen_df.columns:
                scen_df[_c] = pd.to_numeric(_as_series(scen_df, _c), errors="coerce")
        # Hlavní metriky (česky)
        if "Import (MWh)" in scen_df.columns:
            scen_df["Nákup ze sítě (MWh)"] = scen_df["Import (MWh)"]
        if "Export (MWh)" in scen_df.columns:
            scen_df["Přetok do sítě (MWh)"] = scen_df["Export (MWh)"]
        if "Curtailment (MWh)" in scen_df.columns:
            scen_df["Ořezaná / zmařená energie (MWh)"] = scen_df["Curtailment (MWh)"]
        # Přímé sdílení = pouze peer-to-peer (allocations), drž konzistentně i starý sloupec Sdílení
        _direct_share = None
        if "Přímé sdílení FVE (MWh)" in scen_df.columns:
            _direct_share = pd.to_numeric(_as_series(scen_df, "Přímé sdílení FVE (MWh)"), errors="coerce")
        elif "FVE sdílení (MWh)" in scen_df.columns:
            _direct_share = pd.to_numeric(_as_series(scen_df, "FVE sdílení (MWh)"), errors="coerce")
        elif "Sdílení (MWh)" in scen_df.columns:
            _direct_share = pd.to_numeric(_as_series(scen_df, "Sdílení (MWh)"), errors="coerce")
        if _direct_share is not None:
            scen_df["Přímé sdílení mezi OM (MWh)"] = _direct_share
            scen_df["Přímé sdílení elektřiny (MWh)"] = _direct_share
            # "Sdílení (MWh)" dopočítáme níže jako celkové mezimístní sdílení (přímé + přes baterii)

        # Celkové využití FVE v komunitě = výroba - export - curtailment
        if {"Výroba FVE (MWh)", "FVE export (MWh)", "FVE curtailment (MWh)"}.issubset(scen_df.columns):
            _gen2 = pd.to_numeric(_as_series(scen_df, "Výroba FVE (MWh)"), errors="coerce")
            _exp2 = pd.to_numeric(_as_series(scen_df, "FVE export (MWh)"), errors="coerce")
            _cur2 = pd.to_numeric(_as_series(scen_df, "FVE curtailment (MWh)"), errors="coerce")
            scen_df["Celkové využití FVE v komunitě (MWh)"] = (_gen2 - _exp2 - _cur2).clip(lower=0.0)
            scen_df["Přímé využití FVE v komunitě (bez baterií) (MWh)"] = scen_df["Celkové využití FVE v komunitě (MWh)"]

        # Nabití / vybití baterií (sjednocené)
        _local_charge = 0.0
        _local_discharge = 0.0
        _central_charge = 0.0
        _central_discharge = 0.0

        try:
            _lc = tot("Po bat. lokální", "batt_charge_kwh")
            if np.isfinite(_lc):
                _local_charge = float(_lc)
            else:
                _lco = tot("Po bat. lokální", "batt_charge_own_kwh")
                _lcs = tot("Po bat. lokální", "batt_charge_shared_kwh")
                if np.isfinite(_lco): _local_charge += float(_lco)
                if np.isfinite(_lcs): _local_charge += float(_lcs)
        except Exception:
            pass
        try:
            _ld = tot("Po bat. lokální", "batt_discharge_kwh")
            if np.isfinite(_ld):
                _local_discharge = float(_ld)
            else:
                _ldo = tot("Po bat. lokální", "batt_discharge_own_kwh")
                _lds = tot("Po bat. lokální", "batt_discharge_shared_kwh")
                if np.isfinite(_ldo): _local_discharge += float(_ldo)
                if np.isfinite(_lds): _local_discharge += float(_lds)
                if _local_discharge <= 0:
                    _local_discharge = float(_derive_discharge_from_soc(hourly.get("Po bat. lokální")))
        except Exception:
            try:
                _local_discharge = float(_derive_discharge_from_soc(hourly.get("Po bat. lokální")))
            except Exception:
                pass
        try:
            _cc = tot("Po bat. centrální", "batt_charge_kwh")
            if np.isfinite(_cc):
                _central_charge = float(_cc)
            elif np.isfinite(central_charge_kwh):
                _central_charge = float(central_charge_kwh)
        except Exception:
            if np.isfinite(central_charge_kwh):
                _central_charge = float(central_charge_kwh)
        try:
            _cd = tot("Po bat. centrální", "batt_discharge_kwh")
            if np.isfinite(_cd):
                _central_discharge = float(_cd)
        except Exception:
            pass

        scen_df["Nabití baterií (MWh)"] = 0.0
        scen_df["Vybití baterií (MWh)"] = 0.0
        scen_df["Sdílení přes baterie mezi OM (MWh)"] = 0.0
        scen_df["Přesun energie přes baterii mezi OM (MWh)"] = 0.0  # zpětná kompatibilita
        scen_df["Nabití baterií z vlastní FVE (MWh)"] = 0.0
        scen_df["Nabití baterií ze sdílené energie (MWh)"] = 0.0
        scen_df["Nabití baterií z komunitních přetoků (MWh)"] = 0.0
        scen_df["Lokální bateriový posun (MWh)"] = 0.0  # uloženo a spotřebováno ve stejném OM (není sdílení)

        if "Po bat. lokální" in scen_df.index:
            _local_own_charge = float(local_stored_own_kwh) if np.isfinite(local_stored_own_kwh) else 0.0
            _local_shared_charge = float(local_stored_shared_kwh) if np.isfinite(local_stored_shared_kwh) else 0.0
            _local_dis_own = tot("Po bat. lokální", "batt_discharge_own_kwh")
            _local_dis_shared = tot("Po bat. lokální", "batt_discharge_shared_kwh")
            _local_dis_own = float(_local_dis_own) if np.isfinite(_local_dis_own) else 0.0
            _local_dis_shared = float(_local_dis_shared) if np.isfinite(_local_dis_shared) else 0.0
            scen_df.loc["Po bat. lokální", "Nabití baterií (MWh)"] = _to_mwh(_local_charge)
            scen_df.loc["Po bat. lokální", "Vybití baterií (MWh)"] = _to_mwh(_local_discharge)
            scen_df.loc["Po bat. lokální", "Nabití baterií z vlastní FVE (MWh)"] = _to_mwh(_local_own_charge)
            scen_df.loc["Po bat. lokální", "Nabití baterií ze sdílené energie (MWh)"] = _to_mwh(_local_shared_charge)
            # Mezímístní sdílení přes lokální baterie = discharge do komunitního poolu (ne charge do baterií)
            scen_df.loc["Po bat. lokální", "Sdílení přes baterie mezi OM (MWh)"] = _to_mwh(_local_dis_shared)
            # Lokální bateriový posun (nesdílený) = discharge do vlastní spotřeby
            scen_df.loc["Po bat. lokální", "Lokální bateriový posun (MWh)"] = _to_mwh(_local_dis_own)
        if "Po bat. centrální" in scen_df.index:
            scen_df.loc["Po bat. centrální", "Nabití baterií (MWh)"] = _to_mwh(_central_charge)
            scen_df.loc["Po bat. centrální", "Vybití baterií (MWh)"] = _to_mwh(_central_discharge)
            scen_df.loc["Po bat. centrální", "Nabití baterií z komunitních přetoků (MWh)"] = _to_mwh(_central_charge)
            # proxy mezimístního přesunu u centrálu = vybití (komunitně nabitá baterie -> host OM)
            scen_df.loc["Po bat. centrální", "Sdílení přes baterie mezi OM (MWh)"] = _to_mwh(_central_discharge if _central_discharge > 0 else _central_charge)

        # Celkové sdílení mezi OM = přímé peer-to-peer + sdílení přes baterie
        _direct = pd.to_numeric(_as_series(scen_df, "Přímé sdílení mezi OM (MWh)"), errors="coerce").fillna(0.0) if "Přímé sdílení mezi OM (MWh)" in scen_df.columns else pd.Series(0.0, index=scen_df.index)
        _via_batt = pd.to_numeric(_as_series(scen_df, "Sdílení přes baterie mezi OM (MWh)"), errors="coerce").fillna(0.0) if "Sdílení přes baterie mezi OM (MWh)" in scen_df.columns else pd.Series(0.0, index=scen_df.index)
        _total_share = _direct + _via_batt
        scen_df["Celkové sdílení mezi OM (MWh)"] = _total_share
        scen_df["Sdílení (MWh)"] = _total_share
        scen_df["Přesun energie přes baterii mezi OM (MWh)"] = scen_df["Sdílení přes baterie mezi OM (MWh)"]

    except Exception:
        pass

    # Diagnostics: duplicate hourly columns often break pd.to_numeric with DataFrame input
    try:
        for _sc, _df in hourly.items():
            if isinstance(_df, pd.DataFrame):
                _dups = [str(c) for c in _df.columns[_df.columns.duplicated()]]
                if _dups:
                    print(f"[overview] hourly duplicates in {_sc}: {_dups}")
    except Exception:
        pass

    # Monthly series dictionary for plotting
    monthly = {
        "Import (kWh)": {k: _monthly(df, "import_kwh") for k, df in hourly.items()},
        "Export (kWh)": {k: _monthly(df, "export_kwh") for k, df in hourly.items()},
        "Curtailment (kWh)": {k: _monthly(df, "curtailed_kwh") for k, df in hourly.items()},
        "Sdílení (kWh)": {k: _monthly(df, "shared_kwh") for k, df in hourly.items()},
    }

    # Keep both tables in a dict to show in UI
    kpi_pack = {"KPI": kpi_df, "Scénáře": scen_df.reset_index().rename(columns={"index":"Scénář"})}

    return kpi_pack, hourly, monthly
# ----------------------------
# UI state
# ----------------------------
st.set_page_config(page_title="Energetická komunita – pipeline", layout="wide")
st.markdown("""
<style>
[data-testid="stHeader"]{display:none!important}
[data-testid="stAppViewContainer"]>section:first-child{padding-top:56px!important}
#dpu-nb{position:fixed;top:0;left:0;right:0;height:48px;background:#1b3280;color:#fff;
  display:flex;align-items:center;padding:0 20px;gap:10px;z-index:999999;
  font-family:system-ui,sans-serif;font-size:14px;box-shadow:0 2px 12px rgba(0,0,0,.28)}
#dpu-nb a{color:#fff;text-decoration:none;opacity:.70}
#dpu-nb a:hover{opacity:1}
#dpu-nb .dm{width:26px;height:26px;background:#2e8cff;border-radius:6px;
  display:flex;align-items:center;justify-content:center;font-weight:800;font-size:11px}
#dpu-nb .sep{opacity:.25;font-size:18px}
#dpu-nb .dn{font-weight:600}
</style>
<div id="dpu-nb">
  <a href="https://calm-cocada-79e019.netlify.app/">← Hub</a>
  <span class="sep">|</span>
  <div class="dm">DE</div>
  <span class="dn">Energetická komunita</span>
</div>
""", unsafe_allow_html=True)

st.title("Energetická komunita – pipeline (UI)")

if "rp" not in st.session_state:
    st.session_state["rp"] = None
if "cfg" not in st.session_state:
    st.session_state["cfg"] = {
        "wide_sep": "auto",
        "site_row_file_o": 2,
        "kwp_row_file_d": 3,
        "units": "mwh",
        "pair_freq": "60min",
        "price_commodity_mwh": 2500,
        "price_distribution_mwh": 2000,
        "price_feed_in_mwh": 800,
        "mode": "hybrid",
        "max_recipients": 5,
        # bat local defaults
        "eta_c": 0.95,
        "eta_d": 0.95,
    }

# ----------------------------
# Project / Run selection
# ----------------------------
with st.sidebar:
    st.header(_w("1. Vstupy a běh projektu"))
    _projects_active = list_projects()
    _default_project = st.session_state.get("_active_project", "projekt")
    if _default_project not in _projects_active:
        _projects_active = [_default_project] + _projects_active
    _project_options = ["➕ Nový projekt…"] + _projects_active
    _sel_default = st.session_state.get("_active_project_sel_value", _default_project)
    if _sel_default not in _project_options:
        _sel_default = _default_project
    _sel_idx = _project_options.index(_sel_default) if _sel_default in _project_options else 0
    _project_choice = st.selectbox(_w("Projekt"), options=_project_options, index=_sel_idx, key="_active_project_sel")
    st.session_state["_active_project_sel_value"] = _project_choice
    if _project_choice == "➕ Nový projekt…":
        if "_active_project_new" not in st.session_state:
            st.session_state["_active_project_new"] = _default_project
        with st.form("create_project_form"):
            project = st.text_input("Název nového projektu", key="_active_project_new")
            _create_proj = st.form_submit_button(_w("Použít projekt"))
        # When not submitted yet, still keep the typed value so user can continue typing.
        if not _create_proj:
            project = st.session_state.get("_active_project_new", _default_project)
    else:
        project = _project_choice
    st.session_state["_active_project"] = project

    # --- Run picker (behaves like Project picker) ---
    _runs = list_project_runs(project)
    _run_labels = [run_dir_label(p) for p in _runs]
    _run_options = ["➕ Nový běh…"] + _run_labels

    # default selection: current rp, otherwise last selected, otherwise newest run (index 0)
    _cur_rp = st.session_state.get("rp")
    _cur_label = None
    try:
        if _cur_rp and getattr(_cur_rp, "run_dir", None):
            _cur_label = Path(_cur_rp.run_dir).name
    except Exception:
        _cur_label = None

    _sel_run_default = st.session_state.get("_active_run_sel_value") or _cur_label or (_run_labels[0] if _run_labels else "➕ Nový běh…")
    if _sel_run_default not in _run_options:
        _sel_run_default = _run_labels[0] if _run_labels else "➕ Nový běh…"
    _run_idx = _run_options.index(_sel_run_default)

    _run_choice = st.selectbox(_w("Běh"), options=_run_options, index=_run_idx, key="_active_run_sel")
    st.session_state["_active_run_sel_value"] = _run_choice

    if _run_choice == "➕ Nový běh…":
        # Prefill default run name once when switching to "New run" option.
        if ("_new_run_name" not in st.session_state) or (st.session_state.get("_new_run_name_autofill_project") != project):
            st.session_state["_new_run_name"] = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.session_state["_new_run_name_autofill_project"] = project

        with st.form("create_run_form"):
            # Putting this input as the first field in the form makes the UX nicer (cursor goes here).
            run_name = st.text_input(
                _w("Název nového běhu"),
                key="_new_run_name",
                help="Výchozí je YYYYMMDD_HHMMSS – můžeš přepsat pro lepší orientaci (např. zimni_stadion_100kWh_test).",
            )
            _create = st.form_submit_button(_w("Nový běh"))
        if _create:
            st.session_state["rp"] = ensure_run_dirs(project, run_name=run_name)
            st.session_state["_active_run_sel_value"] = Path(st.session_state["rp"].run_dir).name
            st.rerun()
    else:
        # open existing run immediately when selected
        _picked_path = None
        for p in _runs:
            if run_dir_label(p) == _run_choice:
                _picked_path = p
                break
        if _picked_path is not None:
            _existing_rp = runpaths_from_existing_run(_picked_path)
            # update session only if changed
            if (not st.session_state.get("rp")) or (Path(st.session_state["rp"].run_dir) != _picked_path):
                st.session_state["rp"] = _existing_rp

    rp: Optional[RunPaths] = st.session_state["rp"]
    with st.expander(_w("Umístění výstupu"), expanded=False):
        _default_runs_root = str(st.session_state.get("_runs_root_override", str(RUNS_ROOT)))
        _new_runs_root = st.text_input(_w("Složka výstupů"), value=_default_runs_root, key="_runs_root_override")
        if rp:
            st.write(f"Run: `{rp.run_dir}`")
            st.write(f"CSV: `{rp.csv_dir}`")
            st.write(f"LOG: `{rp.logs_dir}`")
        else:
            st.info(_w("Vyber existující běh, nebo založ nový přes **Nový běh**."))


    # --- Variant picker (config sub-scenarios within a run) ---
    if rp:
        _run_dir = Path(rp.run_dir)
        _cfg = st.session_state.get("cfg", {})
        _variants = ["base"] + list_run_variants(_run_dir)
        _active_v = get_active_variant(_run_dir, _cfg)
        if _active_v not in _variants:
            _variants = ["base"] + [v for v in _variants if v != "base"]
            _active_v = "base"
        # Varianta se vybírá z listu stejně jako Projekt/Běh; nová se založí až explicitně
        _variant_new_token = "__new__"
        _variant_opts = [_variant_new_token] + _variants
        def _variant_fmt(v: str) -> str:
            if v == _variant_new_token:
                return _w("+ Založit novou variantu…")
            if v == "base":
                return _w("výchozí")
            return v
        _picked_v = st.selectbox(_w("Varianta"), options=_variant_opts, index=_variant_opts.index(_active_v) if _active_v in _variant_opts else 1, format_func=_variant_fmt, key="_active_variant_sel")
        if _picked_v == _variant_new_token:
            with st.form("new_variant_form", clear_on_submit=False):
                default_name = f"v{len([v for v in _variants if v != 'base'])+1:02d}"
                _new_v_name = st.text_input(_w("Název nové varianty"), value=st.session_state.get("_new_variant_name", default_name), key="_new_variant_name")
                submitted = st.form_submit_button(_w("Založit novou variantu"))
            if submitted:
                new_name = _unique_variant_name(_run_dir, _new_v_name.strip() or default_name)
                save_variant_patch(_run_dir, new_name, {"base": "base"})
                set_active_variant(_run_dir, _cfg, new_name)
                save_cfg_into_run(rp, _cfg)
                st.rerun()
        else:
            if _picked_v != _active_v:
                set_active_variant(_run_dir, _cfg, _picked_v)
            save_cfg_into_run(rp, _cfg)
    st.divider()
# stop if no run
if st.session_state["rp"] is None:
    st.stop()

rp = st.session_state["rp"]
cfg = st.session_state["cfg"]

with st.expander("🧭 Doporučený pracovní postup v aplikaci", expanded=False):
    st.markdown("""
    **Doporučené pořadí práce (UI):**

    1. **Vstupy a běh projektu** – zadej cesty, název běhu a pracovní složku.
    2. **Nastavení výpočtu** – ceny, sdílení, omezení, parametry baterií.
    3. **Fáze 1 – Příprava dat** – transformace WIDE→LONG, mapování OM do objektů, nastavení objektů.
    4. **Fáze 2 – Bilance bez baterií** – pairing v OM a komunitní sdílení bez baterií (baseline).
    5. **Fáze 3A / 3B – Bateriové scénáře** – lokální baterie v OM a jedna komunitní baterie v OM.
    6. **Fáze 4 – Ekonomika scénářů** – vyhodnocení variant lokálních/komunitních baterií.
    7. **Výsledky** – KPI, měsíční a hodinové grafy.

    *Pozn.: Interní názvy souborů a skriptů (step1/step2/step4a/step4b...) mohou zůstat historické; UI názvy jsou sjednocené kvůli přehlednosti.*
    """)


# ----------------------------
# Celkové výsledky (průběžně od začátku)
# ----------------------------
with st.expander(_w("Celkové výsledky (průběžně)"), expanded=True):
    try:
        kpi_pack, hourly_pack, monthly_pack = compute_overview(rp)

        st.subheader(_w("Souhrn KPI"))
        st.dataframe(_ui_prepare_kpi_df(kpi_pack["KPI"]), use_container_width=True, hide_index=True)

        st.subheader(_w("Porovnání scénářů (rychlé)"))
        st.dataframe(_ui_prepare_scen_df(kpi_pack["Scénáře"]), use_container_width=True, hide_index=True)

        tab_m, tab_h = st.tabs(["Měsíční grafy", "Hodinové grafy"])

        with tab_m:
            metric_opts = list(monthly_pack.keys())
            metric = st.selectbox("Metrika (měsíční)", metric_opts, index=0, key="ov_month_metric_top")
            scen_series = monthly_pack.get(metric, {})
            mdf = pd.DataFrame({k: v for k, v in scen_series.items() if v is not None and not v.empty})
            if mdf.empty:
                st.info("Zatím nemám dost podkladů pro měsíční grafy (spusť další kroky).")
            else:
                mdf.index = mdf.index.astype(str)
                st.dataframe(mdf.reset_index().rename(columns={"index": "měsíc"}), use_container_width=True)
                st.bar_chart(mdf)

        with tab_h:
            scen_opts = list(hourly_pack.keys())
            scen = st.selectbox("Scénář (hodinově)", scen_opts, index=0, key="ov_hour_scen_top")
            dfh = hourly_pack.get(scen)
            if dfh is None or dfh.empty:
                st.info("Hodinové grafy: zatím nemám data pro vybraný scénář.")
            else:
                cols = [c for c in dfh.columns if c != "datetime"]
                if not cols:
                    st.info("Hodinové grafy: žádné numerické sloupce.")
                else:
                    col = st.selectbox("Hodinová metrika", cols, index=0, key="ov_hour_col_top")
                    tmp = dfh[["datetime", col]].copy()
                    tmp[col] = pd.to_numeric(_as_series(tmp, col), errors="coerce").fillna(0.0)
                    st.line_chart(tmp.set_index("datetime")[col])

                    tmp["hour"] = pd.to_datetime(tmp["datetime"]).dt.hour
                    avg = tmp.groupby("hour")[col].mean()
                    st.caption("Průměrný den (průměr přes rok)")
                    st.line_chart(avg)
    except Exception as e:
        st.warning(f"Nepodařilo se sestavit souhrn výsledků: {e}")
        st.code(traceback.format_exc())

# ----------------------------
# Step 1 – upload WIDE + run
# ----------------------------
with st.expander(_ui_section_title("Krok 1 – Nahrání dat pro výpočet", rp, required=["eand_wide.csv","ean_o_long.csv","ean_d_long.csv"], optional=["step1.log"]), expanded=True):
    st.header(_w("Fáze 1 – Příprava dat (WIDE → LONG, mapování OM)"))

    colA, colB = st.columns(2)
    with colA:
        eano_file = st.file_uploader(_w("Nahraj EANo WIDE CSV (spotřeba)"), type=["csv"], key="u_eano_wide")
    with colB:
        eand_file = st.file_uploader(_w("Nahraj EANd WIDE CSV (výroba)"), type=["csv"], key="u_eand_wide")

    if eano_file:
        save_upload_into_run(eano_file, rp, "eano_wide.csv", also_copy_to=rp.csv_dir / "eano_wide.csv")
    if eand_file:
        save_upload_into_run(eand_file, rp, "eand_wide.csv", also_copy_to=rp.csv_dir / "eand_wide.csv")

    st.subheader(_w("Nastavení výpočtu"))
    cfg = st.session_state["cfg"]

    load_prev = st.checkbox("Načíst data z předchozích výpočtů", value=False, key="cb_load_prev")
    if load_prev:
        with st.expander(_w("📦 Import dat z předchozích výpočtů"), expanded=True):
            _projects = list_projects()
            if project not in _projects:
                _projects = [project] + _projects
            src_project = st.selectbox(
                "Zdrojový projekt pro import",
                options=_projects,
                index=_projects.index(st.session_state.get("_cfg_source_project", project)) if st.session_state.get("_cfg_source_project", project) in _projects else 0,
                key="_cfg_source_project_sel",
            )
            st.session_state["_cfg_source_project"] = src_project
            runs = list_project_runs(src_project)
            if not runs:
                st.info("Pro tento projekt zatím nejsou žádné uložené běhy s nastavením.")
                src_run_str = str(rp.run_dir) if rp else ""
            else:
                opts = [str(p) for p in runs]
                labels = {str(p): run_dir_label(p) for p in runs}
                default = st.session_state.get("_cfg_source_run", str(rp.run_dir) if rp else opts[0])
                if default not in opts:
                    default = opts[0]
                src_run_str = st.selectbox(
                    "Zdrojový výpočet pro import",
                    options=opts,
                    index=opts.index(default),
                    format_func=lambda x: labels.get(x, x),
                    key="_cfg_source_run_sel",
                )
                st.session_state["_cfg_source_run"] = src_run_str

            # Data k načtení z vybraného běhu
            load_mode = st.radio(
                "Data k načtení",
                options=["Jen vstupy", "Jen výsledky", "Vstupy i výsledky"],
                horizontal=True,
                key="_load_prev_mode",
            )
            load_cfg_too = st.checkbox("Importovat data včetně nastavení výpočtu", value=True, key="_load_prev_cfg_too")

            # Variants (config sub-scenarios)
            var_mode = "Bez variant"
            var_pick = None
            if load_cfg_too:
                src_run_tmp = Path(src_run_str) if src_run_str else None
                src_vars = list_run_variants(src_run_tmp) if src_run_tmp else []
                if src_vars:
                    st.markdown("**Varianty importu nastavení**")
                    var_mode = st.radio(
                        "Načíst varianty",
                        options=[
                            "Bez variant",
                            "Aktivní varianta",
                            "Vybrat variantu",
                            "Všechny varianty",
                        ],
                        horizontal=True,
                        index=1,
                        key="_load_prev_variant_mode",
                    )
                    if var_mode == "Vybrat variantu":
                        default_v = detect_active_variant(src_run_tmp) or src_vars[0]
                        if default_v not in src_vars:
                            default_v = src_vars[0]
                        var_pick = st.selectbox(
                            "Vyber variantu",
                            options=src_vars,
                            index=src_vars.index(default_v),
                            key="_load_prev_variant_pick",
                        )
                else:
                    st.caption("Ve zdrojovém běhu nejsou žádné uložené varianty (RUN/_variants).")

            src_run = Path(src_run_str)
            src_inputs_dir = src_run / "_inputs"
            src_csv_dir = src_run / "csv"
            dst_inputs_dir = rp.run_dir / "_inputs"
            dst_csv_dir = rp.csv_dir
            
            src_input_files = sorted([p.name for p in src_inputs_dir.glob("*") if p.is_file()]) if src_inputs_dir.exists() else []
            src_result_files = sorted([p.name for p in src_csv_dir.glob("*") if p.is_file()]) if src_csv_dir.exists() else []
            
            if load_mode in ("Jen vstupy", "Vstupy i výsledky"):
                st.markdown("**Data ve vybraném zdrojovém výpočtu**")
                pick_inputs = st.multiselect(
                    "Vybrat data k načtení",
                    options=src_input_files,
                    default=src_input_files,
                    key="_pick_prev_inputs",
                )
            else:
                pick_inputs = []
            
            if load_mode in ("Jen výsledky", "Vstupy i výsledky"):
                st.markdown("**Výsledky v zdrojovém běhu**")
                # Rozumné defaulty: typické výstupy, pokud existují
                _preferred = [
                    "by_hour_after.csv",
                    "allocations.csv",
                    "by_hour_after_bat_local.csv",
                    "by_hour_after_bat_central.csv",
                    "central_batt_config.csv",
                    "central_econ_best.csv",
                ]
                default_results = [f for f in _preferred if f in src_result_files] or src_result_files
                pick_results = st.multiselect(
                    "Vyber výsledkové soubory k načtení",
                    options=src_result_files,
                    default=default_results,
                    key="_pick_prev_results",
                )
            else:
                pick_results = []
            
            if rp is None:
                st.info("Nejdřív založ aktivní běh (Nový běh).")
            else:
                if st.button("↩️ Importovat zvolená data z vybraného výpočtu", use_container_width=True):
                    src_run = Path(src_run_str)
                    did_any = False
                    msgs = []
                    if load_cfg_too:
                        maybe_load_cfg_from_run(src_run, cfg)
                        st.session_state.pop("_cfg_sig", None)
                        did_any = True
                        msgs.append("nastavení")
                        # Variants: copy config sub-scenarios if requested
                        try:
                            if load_cfg_too and 'var_mode' in locals():
                                src_run_dir = Path(src_run_str)
                                dst_run_dir = _run_dir_from_rp(rp)
                                copied_variants: list[str] = []
                                if var_mode == "Všechny varianty":
                                    for v in list_run_variants(src_run_dir):
                                        nv = copy_variant_from_run(src_run_dir, dst_run_dir, v)
                                        if nv:
                                            copied_variants.append(nv)
                                elif var_mode == "Aktivní varianta":
                                    av = detect_active_variant(src_run_dir)
                                    if av:
                                        nv = copy_variant_from_run(src_run_dir, dst_run_dir, av)
                                        if nv:
                                            copied_variants.append(nv)
                                            save_run_state(dst_run_dir, {"active_variant": nv})
                                            cfg["active_variant"] = nv
                                elif var_mode == "Vybrat variantu" and var_pick:
                                    nv = copy_variant_from_run(src_run_dir, dst_run_dir, var_pick)
                                    if nv:
                                        copied_variants.append(nv)
                                        save_run_state(dst_run_dir, {"active_variant": nv})
                                        cfg["active_variant"] = nv
                                if copied_variants:
                                    msgs.append(f"varianty ({', '.join(copied_variants)})")
                                    did_any = True
                        except Exception:
                            pass

                    if pick_inputs:
                        n, miss = _copy_selected_files(src_run / "_inputs", rp.run_dir / "_inputs", pick_inputs)
                        # legacy copy, aby kroky fungovaly beze změny
                        _copy_selected_files(src_run / "_inputs", rp.csv_dir, pick_inputs)
                        did_any = did_any or (n > 0)
                        msgs.append(f"vstupy ({n} souborů)")
                    if pick_results:
                        n, miss = _copy_selected_files(src_run / "csv", rp.csv_dir, pick_results)
                        did_any = did_any or (n > 0)
                        msgs.append(f"výsledky ({n} souborů)")
                    if did_any:
                        st.success("Načteno: " + ", ".join(msgs) + f"  ←  {src_run.name}")
                    else:
                        st.warning("Nebylo co načíst (zkontroluj zdrojový běh a výběr souborů).")
                    st.rerun()
    st.markdown("### Formát vstupů")
    cfg["wide_sep"] = st.selectbox("Oddělovač v csv souborech spotřeby a výroby", options=["auto", ";", ",", "\t"], index=0)
    cfg["site_row_file_o"] = st.number_input("Pořadové číslo řádku s názvem objektu v csv souboru", min_value=0, value=int(cfg["site_row_file_o"]))
    cfg["kwp_row_file_d"] = st.number_input("Pořadové číslo řádku s instalovaným výkonem výrobny v csv souboru", min_value=0, value=int(cfg["kwp_row_file_d"]))
    cfg["units"] = st.selectbox("Jednotky množství elektrické energie", options=["kwh", "mwh"], index=1 if cfg["units"] == "mwh" else 0)
    cfg["pair_freq"] = st.text_input("Krok spotřeby a výroby", value=str(cfg["pair_freq"]))

    step1_ready = (rp.csv_dir / "eano_wide.csv").exists() and (rp.csv_dir / "eand_wide.csv").exists()

    if st.button("Spustit Krok 1"):
        if not step1_ready:
            st.error("Chybí eano_wide.csv nebo eand_wide.csv (nahraj oba).")
        else:
            sep = cfg["wide_sep"]
            sep_arg = "auto" if sep == "auto" else sep
            cmd = [
                sys.executable, "-m", "ec_balance.pipeline.step1_wide_to_long",
                "--eano_wide", str(rp.csv_dir / "eano_wide.csv"),
                "--eand_wide", str(rp.csv_dir / "eand_wide.csv"),
                "--outdir", str(rp.csv_dir),
                "--wide_sep", sep_arg,
                "--site_row_file", str(int(cfg["site_row_file_o"])),
                "--kwp_row_file", str(int(cfg["kwp_row_file_d"])),
                "--units", cfg["units"],
            ]
            run_cmd(cmd, cwd=APP_ROOT, log_path=rp.logs_dir / "step1.log")

    # previews from long CSV (cache in session_state so clicks won't blank them)
    def _load_preview_cached(key: str, path: Path) -> pd.DataFrame:
        prev_key = f"_{key}_mtime"
        if not path.exists():
            return st.session_state.get(key, pd.DataFrame())
        mtime = path.stat().st_mtime
        if st.session_state.get(prev_key) != mtime:
            st.session_state[prev_key] = mtime
            st.session_state[key] = load_ean_list(path)
        return st.session_state.get(key, pd.DataFrame())

    eano_long_path = rp.csv_dir / "ean_o_long.csv"
    eand_long_path = rp.csv_dir / "ean_d_long.csv"

    st.subheader(_w("Přehled EANo (spotřeba) – z LONG CSV"))
    try:
        eano_list = _load_preview_cached("eano_list", eano_long_path)
        st.dataframe(_ui_prepare_df_columns(eano_list), width="stretch", height=240)
    except Exception as e:
        st.warning(f"Nepodařilo se načíst přehled EANo z long CSV: {e}")

    st.subheader(_w("Přehled EANd (výroba) – z LONG CSV"))
    try:
        eand_list = _load_preview_cached("eand_list", eand_long_path)
        st.dataframe(_ui_prepare_df_columns(eand_list), width="stretch", height=240)
    except Exception as e:
        st.warning(f"Nepodařilo se načíst přehled EANd z long CSV: {e}")

    # ----------------------------
    # site_map editor (Krok 2 input)
    # ----------------------------
    st.subheader(_w("Párování míst spotřeby a výroby"))

    site_map_path = rp.csv_dir / "site_map.csv"
    pairs_df = None

    # Try load existing site_map, otherwise infer defaults
    try:
        if site_map_path.exists():
            pairs_df = load_site_map(site_map_path)
            # If it's the old site_map from step1 (no pairing columns), try to enrich it
            if not set(["eano_ean", "eand_ean"]).issubset(set(pairs_df.columns)):
                # build default pairs from previews
                if "eano_list" in st.session_state and "eand_list" in st.session_state:
                    pairs_df = infer_default_pairs(st.session_state["eano_list"], st.session_state["eand_list"])
        else:
            if "eano_list" in st.session_state and "eand_list" in st.session_state:
                pairs_df = infer_default_pairs(st.session_state["eano_list"], st.session_state["eand_list"])
    except Exception as e:
        st.warning(f"site_map.csv se nepovedlo načíst: {e}")

    if pairs_df is None:
        pairs_df = pd.DataFrame(columns=["site_group", "site", "eano_ean", "eand_ean"])

    pairs_df = ensure_site_map_has_group(pairs_df)

    # Zobrazení pouze pro čtení – párování EANo ↔ EANd a exportní omezení se řeší přes vstupy/constraints.
    pairs_view = (
        pairs_df.rename(columns={
            "site": "Název odběrného místa",
            "eano_ean": "Označení místa spotřeby",
            "eand_ean": "Označení místa výroby",
        })
        .drop(columns=["site_group"], errors="ignore")
    )
    st.dataframe(pairs_view, use_container_width=True, hide_index=True)


    c1, c2 = st.columns([1, 2])
    with c1:
        if st.button(_w("Uložit site_map.csv pro Krok 2")):
            try:
                out_df = to_site_map_csv(pairs_df)
                out_df.to_csv(site_map_path, index=False, encoding="utf-8")
                st.success(f"Uloženo: {site_map_path}")
            except Exception as e:
                st.error(f"Nepodařilo se uložit site_map.csv: {e}")

    with c2:
        st.write("")

    # ----------------------------
    # Constraints + local batt caps (between 1 and 3)
    # ----------------------------
    
    st.subheader(_w("Constraints"))

    constraints_path = rp.csv_dir / "site_constraints.csv"
    bat_local_cap_path = rp.csv_dir / "bat_local_cap_by_site.csv"

    # Build list of sites from eand_list (preferred) or from site_map
    sites_for_constraints = []
    if "eand_list" in st.session_state and not st.session_state["eand_list"].empty:
        sites_for_constraints = sorted([s for s in st.session_state["eand_list"]["site"].dropna().astype(str).unique() if s.strip() != ""])
    elif site_map_path.exists():
        try:
            sm = load_site_map(site_map_path)
            if "site" in sm.columns:
                sites_for_constraints = sorted([s for s in sm["site"].dropna().astype(str).unique() if s.strip() != ""])
        except Exception:
            pass

    if not sites_for_constraints:
        st.info("Nejdřív proběhni Krok 1 (abych věděl seznam objektů z EANd/EANo).")

    # Load existing constraints if exist
    if constraints_path.exists():
        try:
            constraints_df = read_csv_auto(constraints_path)
            constraints_df = constraints_df.dropna(axis=1, how="all")
            constraints_df = constraints_df.loc[:, ~constraints_df.columns.astype(str).str.startswith("Unnamed")].copy()
        except Exception:
            constraints_df = pd.DataFrame()
    else:
        constraints_df = pd.DataFrame()

    # Standard constraints schema
    # columns: site | allow_export_grid (0/1) | allow_import_grid (0/1)
    # Pozn.: starší názvy (allow_export, allow_charge_from_grid) převedeme na *_grid.
    base = pd.DataFrame({"site": sites_for_constraints})
    _constraints_norm = constraints_df.copy()
    if not _constraints_norm.empty:
        if "allow_export_grid" not in _constraints_norm.columns and "allow_export" in _constraints_norm.columns:
            _constraints_norm["allow_export_grid"] = _constraints_norm["allow_export"]
        if "allow_import_grid" not in _constraints_norm.columns and "allow_charge_from_grid" in _constraints_norm.columns:
            _constraints_norm["allow_import_grid"] = _constraints_norm["allow_charge_from_grid"]

    if not _constraints_norm.empty and "site" in _constraints_norm.columns:
        merged = base.merge(_constraints_norm, on="site", how="left")
    else:
        merged = base.copy()

    if "allow_export_grid" not in merged.columns:
        merged["allow_export_grid"] = 1
    if "allow_import_grid" not in merged.columns:
        merged["allow_import_grid"] = 1

        st.subheader(_w("Nastavení přetoků a odběru ze sítě"))
    _constraints_view = merged[["site", "allow_export_grid", "allow_import_grid"]].copy()
    _constraints_view = _constraints_view.rename(columns={
        "site": _w("Název odběrného místa"),
        "allow_export_grid": _w("Povolit přetoky"),
        "allow_import_grid": _w("Povolit dobíjení baterie ze sítě"),
    })
    def _to_yesno(v):
        try:
            return "ANO" if int(float(v)) == 1 else "NE"
        except Exception:
            return "NE"
    _constraints_view[_w("Povolit přetoky")] = _constraints_view[_w("Povolit přetoky")].apply(_to_yesno)
    _constraints_view[_w("Povolit dobíjení baterie ze sítě")] = _constraints_view[_w("Povolit dobíjení baterie ze sítě")].apply(_to_yesno)
    constraints_edit = st.data_editor(
        _constraints_view,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key="constraints_editor",
        column_config={
            _w("Povolit přetoky"): st.column_config.SelectboxColumn(_w("Povolit přetoky"), options=["ANO","NE"]),
            _w("Povolit dobíjení baterie ze sítě"): st.column_config.SelectboxColumn(_w("Povolit dobíjení baterie ze sítě"), options=["ANO","NE"]),
        },
    )

    if st.button(_w("Potvrdit nastavení přetoků a odběru ze sítě")):
        try:
            out = pd.DataFrame(constraints_edit).copy()
            out = out.rename(columns={
                _w("Název odběrného místa"): "site",
                _w("Povolit přetoky"): "allow_export_grid",
                _w("Povolit dobíjení baterie ze sítě"): "allow_import_grid",
            })
            out["allow_export_grid"] = out["allow_export_grid"].astype(str).str.upper().map({"ANO":1,"NE":0}).fillna(0).astype(int)
            out["allow_import_grid"] = out["allow_import_grid"].astype(str).str.upper().map({"ANO":1,"NE":0}).fillna(0).astype(int)
            out = out[["site", "allow_export_grid", "allow_import_grid"]]
            out.to_csv(constraints_path, index=False, encoding="utf-8")
            st.success(f"Uloženo: {constraints_path}")
        except Exception as e:
            st.error(f"Nešlo uložit constraints: {e}")

    

    st.markdown("### Referenční jednotkové ceny")
    cfg["price_commodity_mwh"] = st.number_input("Cena silové elektřiny (Kč/MWh)", value=float(cfg["price_commodity_mwh"]))
    cfg["price_distribution_mwh"] = st.number_input("Cena distribuční složky elektřiny (Kč/MWh)", value=float(cfg["price_distribution_mwh"]))
    cfg["price_feed_in_mwh"] = st.number_input("Cena za prodej přetoků (Kč/MWh)", value=float(cfg["price_feed_in_mwh"]))


    # --- Individuální ceny po OM (volitelné) ---
    st.markdown("#### Nastavení individuálních jednotkových cen")
    sites_for_prices = [str(s) for s in load_sites_from_any(rp)] if rp is not None else []
    sites_for_prices = [s for s in sites_for_prices if str(s).strip() and str(s).strip().lower() != 'odstranit']
    if not sites_for_prices:
        st.info("Nejdřív nahraj vstupy nebo načti OM z běhu – pak lze nastavit individuální ceny po OM.")
    else:
        # struktura: cfg["site_prices"] = {site: {"commodity": val|None, "distribution": val|None, "feed_in": val|None}}
        #           cfg["site_price_mode"] = {site: {"commodity": "shared|individual", ...}}
        cfg.setdefault("site_prices", {})
        cfg.setdefault("site_price_mode", {})
        rows = []
        for s in sites_for_prices:
            cfg["site_prices"].setdefault(s, {})
            cfg["site_price_mode"].setdefault(s, {})
            for k in ("commodity","distribution","feed_in"):
                cfg["site_prices"][s].setdefault(k, None)
                cfg["site_price_mode"][s].setdefault(k, "shared")
            rows.append({
                "OM": s,
                "Komodita": ("společná" if cfg["site_price_mode"][s]["commodity"]=="shared" else "individuální"),
                "Cena komodity (Kč/MWh)": cfg["site_prices"][s]["commodity"],
                "Distribuce": ("společná" if cfg["site_price_mode"][s]["distribution"]=="shared" else "individuální"),
                "Cena distribuce (Kč/MWh)": cfg["site_prices"][s]["distribution"],
                "Výkup": ("společná" if cfg["site_price_mode"][s]["feed_in"]=="shared" else "individuální"),
                "Výkup (Kč/MWh)": cfg["site_prices"][s]["feed_in"],
            })
        import pandas as _pd
        _df = _pd.DataFrame(rows)
        edited = st.data_editor(
            _df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "OM": st.column_config.TextColumn("Název odběrného místa", disabled=True),
                "Komodita": st.column_config.SelectboxColumn("Silová elektřina", options=["společná","individuální"], help="Zvol „společná“ pro použití referenční ceny, nebo „individuální“ pro vlastní cenu."),
                "Distribuce": st.column_config.SelectboxColumn("Distribuční složka", options=["společná","individuální"], help="Zvol „společná“ pro použití referenční ceny, nebo „individuální“ pro vlastní cenu."),
                "Výkup": st.column_config.SelectboxColumn("Prodej přetoků", options=["společná","individuální"], help="Zvol „společná“ pro použití referenční ceny, nebo „individuální“ pro vlastní cenu."),
                "Cena komodity (Kč/MWh)": st.column_config.NumberColumn("Cena silové elektřiny (Kč/MWh)", step=1.0),
                "Cena distribuce (Kč/MWh)": st.column_config.NumberColumn("Cena distribuční složky elektřiny (Kč/MWh)", step=1.0),
                "Výkup (Kč/MWh)": st.column_config.NumberColumn("Cena za prodej přetoků (Kč/MWh)", step=1.0),
            },
            key="_site_price_editor",
        )
        # zapsat zpět do cfg
        for _, r in edited.iterrows():
            s = str(r["OM"])
            cfg["site_price_mode"][s]["commodity"] = ("shared" if str(r["Komodita"]).strip().lower().startswith("společ") else "individual")
            cfg["site_price_mode"][s]["distribution"] = ("shared" if str(r["Distribuce"]).strip().lower().startswith("společ") else "individual")
            cfg["site_price_mode"][s]["feed_in"] = ("shared" if str(r["Výkup"]).strip().lower().startswith("společ") else "individual")
            cfg["site_prices"][s]["commodity"] = None if _pd.isna(r["Cena komodity (Kč/MWh)"]) else float(r["Cena komodity (Kč/MWh)"])
            cfg["site_prices"][s]["distribution"] = None if _pd.isna(r["Cena distribuce (Kč/MWh)"]) else float(r["Cena distribuce (Kč/MWh)"])
            cfg["site_prices"][s]["feed_in"] = None if _pd.isna(r["Výkup (Kč/MWh)"]) else float(r["Výkup (Kč/MWh)"])

        cfg["mode"] = st.selectbox(
            _w("Režim sdílení"),
            options=["hybrid", "proportional"],
            index=0 if cfg["mode"] == "hybrid" else 1,
            format_func=lambda v: ("hybridní" if v=="hybrid" else "proporční"),
        )
        cfg["max_recipients"] = st.number_input(_w("Max. příjemců"), min_value=1, value=int(cfg["max_recipients"]))

        # Persist numeric/settings inputs into the run folder (RUN/_config/run_config.json).
        if rp is not None:
            cfg_sig = json.dumps(cfg, ensure_ascii=False, sort_keys=True, default=str)
            if st.session_state.get("_cfg_sig") != cfg_sig:
                st.session_state["_cfg_sig"] = cfg_sig
                save_cfg_into_run(rp, _cfg)



# --- Inputs stored in runs (load / inspect) ---
# ----------------------------
    # Step 2
    # ----------------------------
with st.expander(_ui_section_title("Krok 2 - Bilance výroby a spotřeby bez baterií a sdílení", rp, required=["by_hour_after.csv","allocations.csv"], optional=["step2.log","step3.log"]), expanded=False):
    st.header(_w("Fáze 2 – Pairing v OM (lokální bilance bez baterií)"))
    st.subheader(_w("Varianty výroben (PV) – správa v rámci běhu"))
    st.caption("Pro úpravy výroben si založ variantu (v levé části aplikace). Ve výchozí variantě nelze upravovat varianty výkonů výroben.")

    _run_dir = Path(rp.run_dir)
    _active_variant = get_active_variant(_run_dir, _cfg)
    if _active_variant == "base":
        st.info("Ve výchozí variantě není možné měnit výkon výroben. Založ novou variantu v levé části aplikace.")
    else:
        sites_for_pv = [str(s) for s in load_sites_from_any(rp)]
        if not sites_for_pv:
            st.warning("Nejdřív nahraj vstupy / načti OM z běhu – pak lze nastavovat varianty výroben.")
        else:
            patch = load_variant_patch(_run_dir, _active_variant)
            pv_patch = patch.get("pv", {}) if isinstance(patch, dict) else {}
            active_map = pv_patch.get("active", {}) if isinstance(pv_patch, dict) else {}
            kwp_map = pv_patch.get("kwp_override", {}) if isinstance(pv_patch, dict) else {}

            # Baseline kWp (from Krok 1 output, if available)
            base_kwp_map = {}
            for _cand in ["kwp_by_site.csv", "kwp_by_site_long.csv", "kwp_by_site_wide.csv", "site_kwp.csv"]:
                _p = rp.csv_dir / _cand
                if _p.exists():
                    try:
                        _dfk = pd.read_csv(_p)
                        # accept either (site, kwp) or (site, kwp_kwp / kwp)
                        if "site" in _dfk.columns:
                            if "kwp" in _dfk.columns:
                                base_kwp_map = dict(zip(_dfk["site"].astype(str), _dfk["kwp"]))
                            elif "kWp" in _dfk.columns:
                                base_kwp_map = dict(zip(_dfk["site"].astype(str), _dfk["kWp"]))
                        if base_kwp_map:
                            break
                    except Exception:
                        pass

            df_pv = pd.DataFrame({
                "site": sites_for_pv,
                "navržený výkon výrobny - výchozí": [base_kwp_map.get(s, np.nan) for s in sites_for_pv],
                "aktivní": [bool(active_map.get(s, True)) for s in sites_for_pv],
                "kWp override": [kwp_map.get(s, None) for s in sites_for_pv],
            })
            edited = st.data_editor(
                df_pv,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                disabled=["site", "navržený výkon výrobny - výchozí"],
                column_config={
                    "site": st.column_config.TextColumn("Název odběrného místa"),
                    "navržený výkon výrobny - výchozí": st.column_config.NumberColumn("Navržený výkon výrobny - výchozí", format="%.3f"),
                    "aktivní": st.column_config.CheckboxColumn("Aktivní výrobna"),
                    "kWp override": st.column_config.NumberColumn("Nově navržený výkon výrobny", format="%.3f"),
                },
                key="pv_variant_editor",
            )

            col_pv1, col_pv2 = st.columns([1,1])
            with col_pv1:
                if st.button("💾 Uložit variantu výroben", key="save_pv_variant"):
                    active_map2 = {str(r["site"]): bool(r["aktivní"]) for _, r in edited.iterrows()}
                    kwp_map2 = {}
                    for _, r in edited.iterrows():
                        s = str(r["site"])
                        v = r.get("kWp override", None)
                        if v is None or (isinstance(v, float) and np.isnan(v)):
                            continue
                        try:
                            kwp_map2[s] = float(v)
                        except Exception:
                            continue
                    pv_patch2 = {"active": active_map2, "kwp_override": kwp_map2}
                    patch = dict(patch or {})
                    patch["pv"] = pv_patch2
                    save_variant_patch(_run_dir, _active_variant, patch)
                    # also persist into cfg for downstream scripts (if/when they start reading it)
                    cfg.setdefault("variants", {}).setdefault(_active_variant, {})["pv"] = pv_patch2
                    save_cfg_into_run(rp, _cfg)

                    # Write helper CSV (safe even if pipeline ignores it)
                    out = pd.DataFrame({
                        "site": sites_for_pv,
                        "active": [active_map2.get(s, True) for s in sites_for_pv],
                        "kwp_override": [kwp_map2.get(s, np.nan) for s in sites_for_pv],
                    })
                    out.to_csv(rp.csv_dir / "pv_variant_by_site.csv", index=False, encoding="utf-8")
                    st.success("Uloženo do aktivní varianty a do csv/pv_variant_by_site.csv")
            with col_pv2:


                if st.button("Spustit Krok 2"):
                    need = ["ean_o_long.csv", "ean_d_long.csv"]
                    missing = [f for f in need if not (rp.csv_dir / f).exists()]
                    if missing:
                        st.error(f"Chybí soubory z Kroku 1: {', '.join(missing)}")
                    else:
                        cmd = [
                            sys.executable, "-m", "ec_balance.pipeline.step2_local_pv",
                            "--eano_long_csv", str(rp.csv_dir / "ean_o_long.csv"),
                            "--eand_long_csv", str(rp.csv_dir / "ean_d_long.csv"),
                            "--outdir", str(rp.csv_dir),
                            "--pair_freq", str(cfg["pair_freq"]),
                        ]
                        # site_map is optional but usually wanted
                        if site_map_path.exists():
                            cmd += ["--site_map_csv", str(site_map_path)]
                        if constraints_path.exists():
                            cmd += ["--constraints_csv", str(constraints_path)]
                        run_cmd(cmd, cwd=APP_ROOT, log_path=rp.logs_dir / "step2.log")

    # --- Souhrn po Kroku 2 (po odběrných místech) ---
    st.markdown('---')
    st.subheader(_w('Souhrn bilance po instalaci výroben (po OM)'))
    def _fmt_num_cs(x, decimals=3):
        try:
            if x is None or (isinstance(x, float) and np.isnan(x)):
                return ''
            xf=float(x)
        except Exception:
            return str(x)
        # integers without decimals
        if abs(xf - round(xf)) < 1e-9:
            s=f'{int(round(xf)):,d}'
            return s.replace(',', ' ')
        s=f'{xf:,.{decimals}f}'
        s=s.replace(',', ' ').replace('.', ',')
        # trim trailing zeros
        if ',' in s:
            s=s.rstrip('0').rstrip(',')
        return s
    def _pick_col(df, candidates):
        cols=set(df.columns)
        for c in candidates:
            if c in cols:
                return c
        return None
    site_sum_path = rp.csv_dir / 'eano_after_pv__site.csv'
    if site_sum_path.exists():
        df_site = pd.read_csv(site_sum_path)
        # Try to find key columns (kWh)
        col_site = 'site' if 'site' in df_site.columns else df_site.columns[0]
        col_imp_before = _pick_col(df_site, ['import_before_kwh','import_before_pv_kwh','import_no_pv_kwh','import_without_pv_kwh','import_baseline_kwh','import_grid_before_kwh','import_grid_kwh_before'])
        col_imp_after  = _pick_col(df_site, ['import_after_kwh','import_after_pv_kwh','import_with_pv_kwh','import_grid_after_kwh','import_grid_kwh_after','import_kwh'])
        col_exp_after  = _pick_col(df_site, ['export_after_kwh','export_after_pv_kwh','export_kwh','grid_export_kwh'])
        col_curt       = _pick_col(df_site, ['curtailed_kwh','curtailment_kwh','spill_kwh'])
        want = {
            'Název odběrného místa': df_site[col_site],
        }
        if col_imp_before: want[_w('Nákup EE ze sítě před instalací výroben (MWh)')] = df_site[col_imp_before]/1000.0
        if col_imp_after:  want[_w('Nákup EE ze sítě po instalaci výroben (MWh)')]  = df_site[col_imp_after]/1000.0
        if col_exp_after:  want[_w('Přetoky po instalaci výroben (MWh)')]            = df_site[col_exp_after]/1000.0
        if col_curt:       want[_w('Zmařená energie (MWh)')]                          = df_site[col_curt]/1000.0
        out_df = pd.DataFrame(want)
        # Format numbers Czech style
        for c in out_df.columns:
            if c=='Název odběrného místa':
                continue
            out_df[c]=out_df[c].apply(_fmt_num_cs)
        st.dataframe(out_df, use_container_width=True, hide_index=True)
        missing_cols=[name for name,val in [('import_before',col_imp_before),('import_after',col_imp_after),('export_after',col_exp_after),('curtailed',col_curt)] if val is None]
        if missing_cols:
            st.caption('Pozn.: Některé sloupce nebyly ve výstupu Kroku 2 nalezeny: ' + ', '.join(missing_cols))
    else:
        st.info(_w('Souhrnná tabulka se zobrazí po úspěšném spuštění Kroku 2 (chybí eano_after_pv__site.csv).'))
    # ----------------------------
    # Krok 3 – komunitní sdílení bez baterií
    # ----------------------------
with st.expander(_ui_section_title("Krok 3 - Komunitní sdílení bez baterií", rp, required=["by_hour_after.csv","allocations.csv"], optional=["step3.log"]), expanded=False):
    st.subheader(_w("Krok 3 - Komunitní sdílení bez baterií"))
    if st.button(_w("Spustit Krok 3 (sdílení)"), key="run_step3"):
        need = ["eano_after_pv.csv", "eand_after_pv.csv", "local_selfcons.csv"]
        missing = [f for f in need if not (rp.csv_dir / f).exists()]
        if missing:
            st.error(f"Chybí soubory z Kroku 2: {', '.join(missing)}")
        else:
            cmd = [
                sys.executable, "-m", "ec_balance.pipeline.step3_sharing",
                "--eano_after_pv_csv", str(rp.csv_dir / "eano_after_pv.csv"),
                "--eand_after_pv_csv", str(rp.csv_dir / "eand_after_pv.csv"),
                "--local_selfcons_csv", str(rp.csv_dir / "local_selfcons.csv"),
                "--outdir", str(rp.csv_dir),
                "--price_commodity_mwh", str(cfg["price_commodity_mwh"]),
                "--price_distribution_mwh", str(cfg["price_distribution_mwh"]),
                "--price_feed_in_mwh", str(cfg["price_feed_in_mwh"]),
                "--mode", str(cfg["mode"]),
                "--max_recipients", str(int(cfg["max_recipients"])),
            ]
            if site_map_path.exists():
                cmd += ["--site_map_csv", str(site_map_path)]
            if constraints_path.exists():
                cmd += ["--constraints_csv", str(constraints_path)]
            run_cmd(cmd, cwd=APP_ROOT, log_path=rp.logs_dir / "step3.log")

    # --- Souhrn po Kroku 3 (po doběhnutí) ---
    try:
        import pandas as _pd
        by_site_path = None
        for nm in ["by_site_after.csv","by_site_after_sharing.csv","by_site_after_share.csv","by_site_after_step3.csv"]:
            pth = rp.csv_dir / nm
            if pth.exists():
                by_site_path = pth
                break
        if by_site_path is None:
            st.info("Po spuštění Kroku 3 se zde zobrazí souhrnná tabulka po odběrných místech (pokud je k dispozici by_site_after.csv).")
        else:
            df_bs = _pd.read_csv(by_site_path)
            # Mapování sloupců (Krok 3 – sdílení) → zobrazované metriky
            # Pozn.: blocked_import_kwh je nadbytečné → nezobrazovat.
            _display_cols = [
                ("site", "Název odběrného místa", None),
                ("import_local_kwh", "Nákup EE ze sítě před sdílením (MWh)", 1000.0),
                ("export_local_kwh", "Přetoky před sdílením (MWh)", 1000.0),
                ("import_residual_kwh", "Nákup EE ze sítě po sdílení (MWh)", 1000.0),
                ("export_residual_kwh", "Přetoky po sdílení (MWh)", 1000.0),
                ("shared_in_kwh", "Sdílení dodané (MWh)", 1000.0),
                ("shared_out_kwh", "Sdílení přijaté (MWh)", 1000.0),
                ("curtailed_kwh", "Zmařená energie (MWh)", 1000.0),
            ]
            out = _pd.DataFrame()
            for _col, _label, _div in _display_cols:
                if _col not in df_bs.columns:
                    continue
                if _div is None:
                    out[_label] = df_bs[_col].astype(str)
                else:
                    out[_label] = _pd.to_numeric(df_bs[_col], errors="coerce") / _div
            if out.shape[1] == 0:
                st.info("Souhrnná tabulka po Kroku 3 je k dispozici, ale očekávané sloupce nebyly nalezeny.")
            else:
                # formát čísel: tisíce mezerou, desetiny čárkou
                def _fmt_num_cs3(x, decimals=3):
                    try:
                        if x is None or (isinstance(x, float) and np.isnan(x)):
                            return ''
                        xf=float(x)
                    except Exception:
                        return str(x)
                    if abs(xf - round(xf)) < 1e-9:
                        s=f'{int(round(xf)):,d}'
                        return s.replace(',', ' ')
                    s=f'{xf:,.{decimals}f}'.replace(',', ' ').replace('.', ',')
                    if ',' in s:
                        s=s.rstrip('0').rstrip(',')
                    return s
                for _c in out.columns:
                    if _c == 'Název odběrného místa':
                        continue
                    out[_c] = out[_c].apply(_fmt_num_cs3)
                st.dataframe(out, use_container_width=True, hide_index=True)
    except Exception as _e:
        st.warning(f"Souhrn po Kroku 3 se nepodařilo načíst: {_e}")

# Step 4 – local battery sensitivity + choose batteries
# ----------------------------

# ----------------------------
# Krok 5 – centrální baterie (návrh + umístění)
# ----------------------------

with st.expander("🔧 Parametry baterií a ekonomiky", expanded=False):
    cfg = st.session_state["cfg"]
    st.caption(_w("Nastavení, které ovlivňuje bateriové scénáře a ekonomiku. Logika výpočtů se tím nemění, jen parametry."))

    c1, c2, c3 = st.columns(3)
    with c1:
        cfg.setdefault("eta_c", 0.95)
        cfg.setdefault("eta_d", 0.95)
        cfg["eta_c"] = st.number_input("Účinnost nabíjení ηc", min_value=0.0, max_value=1.0, value=float(cfg["eta_c"]))
        cfg["eta_d"] = st.number_input("Účinnost vybíjení ηd", min_value=0.0, max_value=1.0, value=float(cfg["eta_d"]))
    with c2:
        cfg.setdefault("cap_kwh_list", "0,5,10,15")
        cfg["cap_kwh_list"] = st.text_input("Výchozí seznam kapacit (kWh)", value=str(cfg["cap_kwh_list"]))
        cfg.setdefault("use_max_kwh_per_kwp", False)
        cfg["use_max_kwh_per_kwp"] = st.checkbox("Uplatnit limit max kWh/kWp", value=bool(cfg["use_max_kwh_per_kwp"]))
    with c3:
        cfg.setdefault("max_kwh_per_kwp", 1.0)
        cfg.setdefault("limit_only_when_kwp_positive", True)
        cfg["max_kwh_per_kwp"] = st.number_input("Max kWh na 1 kWp", min_value=0.0, value=float(cfg["max_kwh_per_kwp"]))
        cfg["limit_only_when_kwp_positive"] = st.checkbox("Aplikovat jen když kWp > 0", value=bool(cfg["limit_only_when_kwp_positive"]))

    st.markdown("---")
    st.markdown("**Ekonomika baterek**")
    cfg.setdefault("project_years", 15)
    cfg.setdefault("discount_rate", 0.05)
    cfg.setdefault("local_price_per_kwh", 0.0)
    cfg.setdefault("local_fixed_cost", 0.0)
    cfg.setdefault("central_price_per_kwh", 0.0)
    cfg.setdefault("central_fixed_cost", 0.0)

    e1, e2, e3 = st.columns(3)
    with e1:
        cfg["project_years"] = st.number_input("Doba projektu (roky)", min_value=1, value=int(cfg["project_years"]))
        cfg["discount_rate"] = st.number_input("Diskontní sazba", min_value=0.0, value=float(cfg["discount_rate"]))
    with e2:
        cfg["local_price_per_kwh"] = st.number_input("Lokální: cena Kč/kWh", min_value=0.0, value=float(cfg["local_price_per_kwh"]))
        cfg["local_fixed_cost"] = st.number_input("Lokální: fixní náklad Kč", min_value=0.0, value=float(cfg["local_fixed_cost"]))
    with e3:
        cfg["central_price_per_kwh"] = st.number_input("Komunitní: cena Kč/kWh", min_value=0.0, value=float(cfg["central_price_per_kwh"]))
        cfg["central_fixed_cost"] = st.number_input("Komunitní: fixní náklad Kč", min_value=0.0, value=float(cfg["central_fixed_cost"]))

    # Persist numeric/settings inputs into the run folder
    if st.session_state.get("rp") is not None:
        rp_ = st.session_state["rp"]
        cfg_sig = json.dumps(cfg, ensure_ascii=False, sort_keys=True, default=str)
        if st.session_state.get("_cfg_sig") != cfg_sig:
            st.session_state["_cfg_sig"] = cfg_sig
            save_cfg_into_run(rp_, cfg)

with st.expander(_ui_section_title("🔋 Fáze 3B – Jedna komunitní baterie v OM", rp, required=["by_hour_after_bat_central.csv","central_batt_config.csv"], optional=["step4.log","step4b.log"]), expanded=False):
    st.header("Fáze 3B – Jedna komunitní baterie v OM (návrh + umístění)")
    st.subheader(_w("Varianta komunitní baterie (3B)"))
    st.caption(_w("Komunitní baterie je fyzicky v jednom OM. Tady ji nastavíš v aktivní variantě (patch) a pak znovu spustíš scénář 3B."))

    _run_dir = Path(rp.run_dir)
    _active_variant = get_active_variant(_run_dir, _cfg)

    p_step3_bh = rp.csv_dir / "by_hour_after.csv"
    if not p_step3_bh.exists():
        st.info("Nevidím by-hour výstup po sdílení (by_hour_after.csv). Spusť nejdřív Krok 3.")
    else:
        df3 = read_csv_auto(p_step3_bh)
        df3 = df3.dropna(axis=1, how="all")
        if "datetime" in df3.columns:
            df3["datetime"] = pd.to_datetime(df3["datetime"], errors="coerce")
            df3 = df3.dropna(subset=["datetime"]).copy()
            df3["_t"] = df3["datetime"]
        else:
            # fallback: try to infer time col
            tcol = next((c for c in df3.columns if "time" in c.lower() or "date" in c.lower()), None)
            if tcol is None:
                st.error("by_hour_after.csv nemá sloupec datetime (ani nic podobného).")
                df3 = None
            else:
                df3[tcol] = pd.to_datetime(df3[tcol], errors="coerce")
                df3 = df3.dropna(subset=[tcol]).copy()
                df3["_t"] = df3[tcol]

        if df3 is not None:
            # Find surplus/export col + curtailed
            def _find_best_energy_col(df: pd.DataFrame, must_have: list[str], prefer: list[str] | None = None) -> tuple[str | None, str | None]:
                """Return (col_name, unit) where unit is 'kwh' or 'mwh'."""
                cols = list(df.columns)
                scored: list[tuple[int, str, str | None]] = []
                for c in cols:
                    cl = str(c).lower()
                    if not all(k in cl for k in must_have):
                        continue
                    unit = None
                    if "kwh" in cl:
                        unit = "kwh"
                    elif "mwh" in cl:
                        unit = "mwh"
                    score = 0
                    if prefer:
                        for i, p in enumerate(prefer):
                            if p in cl:
                                score += 50 - i
                    if "after" in cl:
                        score += 10
                    if "share" in cl or "sharing" in cl:
                        score += 8
                    if unit == "kwh":
                        score += 5
                    if unit == "mwh":
                        score += 3
                    if unit is None:
                        continue
                    scored.append((score, str(c), unit))
                if not scored:
                    return None, None
                scored.sort(key=lambda x: x[0], reverse=True)
                _, col, unit = scored[0]
                return col, unit

            # Find surplus/export column (robust) + unit conversion
            # Pro centrální baterii (po sdílení) preferujeme *residual* sloupce natvrdo, pokud existují.
            exp_col, exp_unit = None, None
            if "export_residual_kwh" in df3.columns:
                exp_col, exp_unit = "export_residual_kwh", "kwh"
            elif "export_residual_mwh" in df3.columns:
                exp_col, exp_unit = "export_residual_mwh", "mwh"
            else:
                exp_col, exp_unit = _find_best_energy_col(
                    df3,
                    must_have=["export"],
                    prefer=["export_residual", "residual", "export_after_share", "after_share", "export_after", "after", "share"],
                )
            if exp_col is None:
                exp_col, exp_unit = _find_best_energy_col(
                    df3,
                    must_have=["surplus"],
                    prefer=["after_share", "after", "share"],
                )
            # další fallbacky (některé datasety nepoužívají slovo "export")
            if exp_col is None:
                exp_col, exp_unit = _find_best_energy_col(
                    df3,
                    must_have=["feed"],
                    prefer=["after_share", "after", "share", "grid"],
                )
            if exp_col is None:
                exp_col, exp_unit = _find_best_energy_col(
                    df3,
                    must_have=["to_grid"],
                    prefer=["after_share", "after", "share"],
                )
            if "curtailed_kwh" in df3.columns:
                curt_col, curt_unit = "curtailed_kwh", "kwh"
            elif "curtailed_mwh" in df3.columns:
                curt_col, curt_unit = "curtailed_mwh", "mwh"
            else:
                curt_col, curt_unit = _find_best_energy_col(
                    df3,
                    must_have=["curtail"],
                    prefer=["residual", "after_share", "after"],
                )
            if curt_col is None:
                curt_col, curt_unit = _find_best_energy_col(df3, must_have=["curtailed"], prefer=["after_share", "after"])

            if exp_col is None:
                st.error(
                    f"{p_step3_bh.name}: nenašel jsem sloupec s komunitním přebytkem/exportem. "
                    "Hledám něco jako export_after_share_kwh / export_after_kwh / export_after_share_mwh / export_after_mwh / surplus_*. "
                    f"Dostupné sloupce: {', '.join([str(c) for c in df3.columns])}"
                )
            else:
                st.caption(f"Komunitní přebytek/export: {exp_col} ({exp_unit}); curtailment: {curt_col or 'nenalezeno'}")
                export_only = pd.to_numeric(df3[exp_col], errors="coerce").fillna(0.0).astype(float)
                if exp_unit == "mwh":
                    export_only = export_only * 1000.0
                export_ts = export_only.groupby(df3["_t"]).sum().sort_index()

                curt_ts = None
                if curt_col is not None:
                    curt_only = pd.to_numeric(df3[curt_col], errors="coerce").fillna(0.0).astype(float)
                    if curt_unit == "mwh":
                        curt_only = curt_only * 1000.0
                    curt_ts = curt_only.groupby(df3["_t"]).sum().sort_index()
                else:
                    curt_ts = pd.Series(0.0, index=export_ts.index)

                idx_u = export_ts.index.union(curt_ts.index)
                export_ts = export_ts.reindex(idx_u, fill_value=0.0)
                curt_ts = curt_ts.reindex(idx_u, fill_value=0.0)

                # "surplus" = přetok do sítě + zmařená (curtailed) energie
                surplus_ts = (export_ts + curt_ts).sort_index()
                annual_surplus_kwh = float(surplus_ts.sum())
                # nulový check / diagnostika mapování
                nz_export = float((export_ts > 0).sum()) if len(export_ts) else 0.0
                nz_curt = float((curt_ts > 0).sum()) if len(curt_ts) else 0.0
                if annual_surplus_kwh <= 0:
                    st.error(
                        "Komunitní přebytek pro centrální baterii vychází 0 kWh. "
                        f"Použité sloupce: export={exp_col} ({exp_unit}), curtailed={curt_col or 'None'} ({curt_unit or '-'})"
                    )
                    st.caption(
                        f"Debug: nenulové hodiny export={int(nz_export)}, curtailed={int(nz_curt)}; "
                        f"sloupce v by_hour_after.csv: {', '.join([str(c) for c in df3.columns])}"
                    )
                else:
                    st.caption(
                        f"Debug surplus: suma={annual_surplus_kwh:,.1f} kWh; nenulové hodiny export={int(nz_export)}, curtailed={int(nz_curt)}"
                    )
                cols = st.columns([1, 1, 2])
                with cols[0]:
                    target_cycles = st.number_input(
                        "Cílové cykly/rok (při využití 100% přetoků)",
                        min_value=50,
                        max_value=2000,
                        value=int(cfg.get("central_target_cycles", 300)),
                        step=50,
                    )
                suggested_cap_kwh = (annual_surplus_kwh / float(target_cycles)) if target_cycles > 0 else 0.0
                with cols[1]:
                    cap_kwh = st.number_input(
                        "Kapacita centrální baterie (kWh)",
                        min_value=0.0,
                        value=float(cfg.get("central_cap_kwh", suggested_cap_kwh)),
                        step=10.0,
                        help="Návrh = roční přetoky / cílové cykly. Můžeš ručně upravit.",
                    )
                cfg["central_target_cycles"] = int(target_cycles)
                cfg["central_cap_kwh"] = float(cap_kwh)

                with cols[2]:
                    st.write("**Podklad pro návrh**")
                    st.write(f"Roční komunitní přebytek (vč. curtailmentu): **{annual_surplus_kwh/1000:,.2f} MWh**")
                    st.write(f"Návrh kapacity pro {int(target_cycles)} cyklů/rok: **{suggested_cap_kwh:,.1f} kWh**")

                # Demand by site (after sharing preferred)
                demand_df = None
                demand_source = ""

                # Pozn.: Pro siting potřebujeme HODINOVÝ import po OM po sdílení.
                # by_site_after.csv bývá agregovaný bez datetime, takže sám o sobě nestačí.
                # Preferovaný zdroj proto je:
                #   eano_after_pv.csv (import po OM před sdílením, hodinově)
                #   + allocations.csv (kolik bylo nasdíleno do konkrétního OM po hodinách)
                # => import po sdílení na OM = max(import_po_FVE - přijaté_sdílení, 0)

                # 1) Zkusit sestavit hodinový import po OM po sdílení z eano_after_pv + allocations
                p_eano_after_pv = rp.csv_dir / "eano_after_pv.csv"
                p_alloc = None
                for name in ["allocations.csv", "share_allocations.csv", "sharing_allocations.csv", "allocations_long.csv"]:
                    p = rp.csv_dir / name
                    if p.exists() and p.is_file():
                        p_alloc = p
                        break

                if p_eano_after_pv.exists() and p_alloc is not None:
                    try:
                        eano_tmp = read_csv_auto(p_eano_after_pv)
                        alloc_tmp = read_csv_auto(p_alloc)

                        # --- eano_after_pv: datetime + site + import po FVE ---
                        if "datetime" in eano_tmp.columns:
                            eano_tmp["datetime"] = pd.to_datetime(eano_tmp["datetime"], errors="coerce")
                        else:
                            tcol_e = next((c for c in eano_tmp.columns if "time" in str(c).lower() or "date" in str(c).lower()), None)
                            if tcol_e is not None:
                                eano_tmp["datetime"] = pd.to_datetime(eano_tmp[tcol_e], errors="coerce")

                        if "site" not in eano_tmp.columns:
                            sc_e = next((c for c in eano_tmp.columns if str(c).lower() in ["om", "meter", "mp", "place", "location"]), None)
                            if sc_e is not None:
                                eano_tmp["site"] = eano_tmp[sc_e]

                        imp_e_col = None
                        for c in eano_tmp.columns:
                            cl = str(c).lower()
                            if ("import" in cl) and (("after" in cl) or ("pv" in cl)) and (("kwh" in cl) or ("mwh" in cl)):
                                imp_e_col = c
                                break
                        if imp_e_col is None:
                            for c in eano_tmp.columns:
                                cl = str(c).lower()
                                if ("import" in cl) and (("kwh" in cl) or ("mwh" in cl)):
                                    imp_e_col = c
                                    break

                        # --- allocations: datetime + to_site + shared energy ---
                        if "datetime" in alloc_tmp.columns:
                            alloc_tmp["datetime"] = pd.to_datetime(alloc_tmp["datetime"], errors="coerce")
                        elif "t" in alloc_tmp.columns:
                            alloc_tmp["datetime"] = pd.to_datetime(alloc_tmp["t"], errors="coerce")
                        elif "timestamp" in alloc_tmp.columns:
                            alloc_tmp["datetime"] = pd.to_datetime(alloc_tmp["timestamp"], errors="coerce")
                        else:
                            tcol_a = next((c for c in alloc_tmp.columns if "time" in str(c).lower() or "date" in str(c).lower()), None)
                            alloc_tmp["datetime"] = pd.to_datetime(alloc_tmp[tcol_a], errors="coerce") if tcol_a is not None else pd.NaT

                        to_col = None
                        for cand in ["to_site", "site_to", "recipient_site", "dst_site", "target_site"]:
                            if cand in alloc_tmp.columns:
                                to_col = cand
                                break
                        if to_col is None:
                            # fallback: když existuje site a není from_site/to_site, použij site
                            if "site" in alloc_tmp.columns:
                                to_col = "site"

                        alloc_val_col = None
                        for c in alloc_tmp.columns:
                            cl = str(c).lower()
                            if (("share" in cl) or ("alloc" in cl) or ("energy" in cl)) and (("kwh" in cl) or ("mwh" in cl)):
                                alloc_val_col = c
                                break
                        if alloc_val_col is None:
                            for c in alloc_tmp.columns:
                                cl = str(c).lower()
                                if ("kwh" in cl) or ("mwh" in cl):
                                    if c not in ["soc_kwh", "batt_charge_kwh", "batt_discharge_kwh"]:
                                        alloc_val_col = c
                                        break

                        if imp_e_col is not None and to_col is not None:
                            eano_tmp = eano_tmp.dropna(subset=["datetime"]).copy()
                            alloc_tmp = alloc_tmp.dropna(subset=["datetime"]).copy()

                            eano_tmp["site"] = eano_tmp["site"].astype(str).str.strip()
                            alloc_tmp[to_col] = alloc_tmp[to_col].astype(str).str.strip()

                            eano_tmp[imp_e_col] = pd.to_numeric(eano_tmp[imp_e_col], errors="coerce").fillna(0.0)
                            if "mwh" in str(imp_e_col).lower():
                                eano_tmp["_import_pv_kwh"] = eano_tmp[imp_e_col] * 1000.0
                            else:
                                eano_tmp["_import_pv_kwh"] = eano_tmp[imp_e_col]

                            if alloc_val_col is not None:
                                alloc_tmp[alloc_val_col] = pd.to_numeric(alloc_tmp[alloc_val_col], errors="coerce").fillna(0.0)
                                if "mwh" in str(alloc_val_col).lower():
                                    alloc_tmp["_shared_to_site_kwh"] = alloc_tmp[alloc_val_col] * 1000.0
                                else:
                                    alloc_tmp["_shared_to_site_kwh"] = alloc_tmp[alloc_val_col]
                            else:
                                alloc_tmp["_shared_to_site_kwh"] = 0.0

                            recv = (
                                alloc_tmp.groupby(["datetime", to_col], as_index=False)["_shared_to_site_kwh"]
                                .sum()
                                .rename(columns={to_col: "site"})
                            )

                            base = eano_tmp[["datetime", "site", "_import_pv_kwh"]].copy()
                            tmpm = base.merge(recv, on=["datetime", "site"], how="left")
                            tmpm["_shared_to_site_kwh"] = pd.to_numeric(tmpm["_shared_to_site_kwh"], errors="coerce").fillna(0.0)
                            tmpm["demand_kwh"] = (tmpm["_import_pv_kwh"] - tmpm["_shared_to_site_kwh"]).clip(lower=0.0)

                            demand_df = tmpm[["datetime", "site", "demand_kwh"]].copy()
                            demand_source = f"derived from eano_after_pv.csv:{imp_e_col} - {p_alloc.name}:{alloc_val_col or 'n/a'} grouped by {to_col}"
                    except Exception as _e_dem:
                        st.warning(f"Nepodařilo se složit hodinový import po OM ze sdílení ({type(_e_dem).__name__}): {_e_dem}")

                # 2) Fallback: by_site_after*, ale jen pokud obsahuje datetime (některé verze ho nemají)
                if demand_df is None:
                    p_by_site = None
                    for name in ["by_site_after.csv", "by_site_after_share.csv", "by_site_after_sharing.csv", "by_site_after_after.csv"]:
                        p = rp.csv_dir / name
                        if p.exists() and p.is_file():
                            p_by_site = p
                            break

                    if p_by_site is not None:
                        tmp = read_csv_auto(p_by_site)
                        # normalize time column
                        if "datetime" in tmp.columns:
                            tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
                        else:
                            tcol = next((c for c in tmp.columns if "time" in str(c).lower() or "date" in str(c).lower()), None)
                            if tcol is not None:
                                tmp["datetime"] = pd.to_datetime(tmp[tcol], errors="coerce")

                        if "datetime" in tmp.columns:
                            tmp = tmp.dropna(subset=["datetime"]).copy()

                        # ensure site column exists
                        if "site" not in tmp.columns:
                            sc = next((c for c in tmp.columns if str(c).lower() in ["om", "meter", "mp", "place", "location"]), None)
                            if sc is not None:
                                tmp["site"] = tmp[sc]

                        # find import column
                        imp_col = None
                        for c in tmp.columns:
                            cl = str(c).lower()
                            if ("import" in cl) and (("kwh" in cl) or ("mwh" in cl)) and (("after" in cl) or ("share" in cl) or ("sharing" in cl)):
                                imp_col = c
                                break
                        if imp_col is None:
                            for c in tmp.columns:
                                cl = str(c).lower()
                                if ("import" in cl) and (("kwh" in cl) or ("mwh" in cl)):
                                    imp_col = c
                                    break

                        if ("datetime" in tmp.columns) and (imp_col is not None) and ("site" in tmp.columns):
                            tmp[imp_col] = pd.to_numeric(tmp[imp_col], errors="coerce").fillna(0.0)
                            demand_df = tmp[["datetime", "site", imp_col]].rename(columns={imp_col: "demand_kwh"})
                            if "mwh" in str(imp_col).lower():
                                demand_df["demand_kwh"] = demand_df["demand_kwh"] * 1000.0
                            demand_source = f"{p_by_site.name}:{imp_col}"

                # 3) Poslední fallback: eano_after_pv (bez sdílení po OM) – horší, ale aspoň něco
                if demand_df is None:
                    p_eano = rp.csv_dir / "eano_after_pv.csv"
                    if not p_eano.exists():
                        st.error("Nemám podklady pro hodinový import po site (eano_after_pv.csv / allocations.csv / by_site_after*.csv).")
                    else:
                        tmp = read_csv_auto(p_eano)
                        if "datetime" in tmp.columns:
                            tmp["datetime"] = pd.to_datetime(tmp["datetime"], errors="coerce")
                        else:
                            tcol = next((c for c in tmp.columns if "time" in str(c).lower() or "date" in str(c).lower()), None)
                            if tcol is not None:
                                tmp["datetime"] = pd.to_datetime(tmp[tcol], errors="coerce")
                        if "datetime" in tmp.columns:
                            tmp = tmp.dropna(subset=["datetime"]).copy()
                        if "import_after_kwh" in tmp.columns:
                            imp_col = "import_after_kwh"
                        else:
                            imp_col = next((c for c in tmp.columns if "import" in str(c).lower() and ("kwh" in str(c).lower() or "mwh" in str(c).lower())), None)
                        if imp_col is not None and "site" in tmp.columns:
                            tmp[imp_col] = pd.to_numeric(tmp[imp_col], errors="coerce").fillna(0.0)
                            demand_df = tmp.rename(columns={imp_col: "demand_kwh"})[["datetime", "site", "demand_kwh"]]
                            if "mwh" in str(imp_col).lower():
                                demand_df["demand_kwh"] = demand_df["demand_kwh"] * 1000.0
                            demand_source = f"fallback {p_eano.name}:{imp_col}"
                if demand_df is not None:
                    if demand_source:
                        st.caption(f"Import po OM (pro vybíjení baterie) beru z: {demand_source}")
                    all_sites = sorted(demand_df["site"].astype(str).unique().tolist())
                    default_sel = all_sites[: min(20, len(all_sites))]
                    sel_sites = st.multiselect("OM pro analýzu umístění (siting)", all_sites, default=default_sel)

                    # Community import po sdílení (agregace přes všechna OM) – použijeme demand_df
                    comm_import_ts = demand_df.groupby("datetime")["demand_kwh"].sum().sort_index()

                    # Debug / nulový check demand po sdílení
                    _d_sum = float(pd.to_numeric(demand_df["demand_kwh"], errors="coerce").fillna(0.0).sum())
                    _d_nz = int((pd.to_numeric(demand_df["demand_kwh"], errors="coerce").fillna(0.0) > 0).sum())
                    st.caption(f"Debug demand po sdílení (OM): suma={_d_sum:,.1f} kWh; nenulové řádky={_d_nz}; unikátní OM={demand_df['site'].astype(str).nunique()}")

                    # Zarovnáme časový index s přebytky/exporty (export_ts / curt_ts / surplus_ts)
                    try:
                        if "export_ts" in locals():
                            idx_u2 = comm_import_ts.index.union(export_ts.index)
                            comm_import_ts = comm_import_ts.reindex(idx_u2, fill_value=0.0)
                            export_ts = export_ts.reindex(idx_u2, fill_value=0.0)
                            curt_ts = curt_ts.reindex(idx_u2, fill_value=0.0)
                            surplus_ts = (export_ts + curt_ts).sort_index()
                    except Exception:
                        pass


                    def simulate_for_site(site_name: str) -> tuple[pd.DataFrame, dict]:
                        """Simulace centrální baterie umístěné na jednom OM, ale nad komunitním profilem.

                        - Nabíjení: z komunitního přebytku (export do sítě + curtailed)
                        - Vybíjení: pouze proti importu vybraného OM (nemůžeme kompenzovat import jiných OM)
                        - Výstup je komunitní by-hour (import/export/curtail po baterii) + SOC/charge/discharge
                        """

                        # časová řada importu vybraného OM (po sdílení)
                        site_ts = (
                            demand_df.loc[demand_df["site"].astype(str).str.strip() == str(site_name).strip(), ["datetime", "demand_kwh"]]
                            .set_index("datetime")["demand_kwh"]
                            .sort_index()
                        )

                        # společný index
                        idx = comm_import_ts.index.union(site_ts.index)
                        if "export_ts" in locals():
                            idx = idx.union(export_ts.index)
                        if "curt_ts" in locals():
                            idx = idx.union(curt_ts.index)

                        comm_imp = comm_import_ts.reindex(idx, fill_value=0.0)
                        site_imp = site_ts.reindex(idx, fill_value=0.0)

                        exp = export_ts.reindex(idx, fill_value=0.0)
                        cur = curt_ts.reindex(idx, fill_value=0.0)

                        soc = 0.0
                        socs = []
                        charges = []
                        discharges = []
                        import_after = []
                        export_after = []
                        curt_after = []

                        for t in idx:
                            export_kwh = float(exp.loc[t]) if t in exp.index else 0.0
                            curt_kwh = float(cur.loc[t]) if t in cur.index else 0.0
                            avail = max(export_kwh, 0.0)

                            # charge (pouze z komunitního exportu po sdílení; curtailed neredukujeme)
                            charge = min(avail, max(cap_kwh - soc, 0.0))
                            exp_used = charge

                            soc += charge
                            export_kwh_after = max(export_kwh - exp_used, 0.0)
                            curt_kwh_after = max(curt_kwh, 0.0)

                            # discharge (jen proti importu vybraného OM)
                            imp_site = float(site_imp.loc[t]) if t in site_imp.index else 0.0
                            discharge = min(soc, max(imp_site, 0.0))
                            soc -= discharge

                            # komunitní import po baterii = komunitní import po sdílení - discharge
                            imp_comm = float(comm_imp.loc[t]) if t in comm_imp.index else 0.0
                            imp_comm_after = max(imp_comm - discharge, 0.0)

                            socs.append(soc)
                            charges.append(charge)
                            discharges.append(discharge)
                            import_after.append(imp_comm_after)
                            export_after.append(export_kwh_after)
                            curt_after.append(curt_kwh_after)

                        bh = pd.DataFrame(
                            {
                                "datetime": idx,
                                "batt_site": str(site_name),
                                "soc_kwh": socs,
                                "batt_charge_kwh": charges,
                                "batt_discharge_kwh": discharges,
                                "import_after_batt_kwh": import_after,
                                "export_after_batt_kwh": export_after,
                                "curtailed_after_batt_kwh": curt_after,
                                # pro kontrolu:
                                "import_before_kwh": comm_imp.reindex(idx, fill_value=0.0).to_numpy(),
                                "export_before_kwh": exp.reindex(idx, fill_value=0.0).to_numpy(),
                                "curtailed_before_kwh": cur.reindex(idx, fill_value=0.0).to_numpy(),
                                "site_import_before_kwh": site_imp.reindex(idx, fill_value=0.0).to_numpy(),
                            }
                        )

                        charge_sum = float(np.nansum(charges))
                        discharge_sum = float(np.nansum(discharges))

                        summary = {
                            "site": str(site_name),
                            "cap_kwh": float(cap_kwh),
                            "charge_kwh": charge_sum,
                            "discharge_kwh": discharge_sum,
                            "end_soc_kwh": float(soc),
                            "eq_cycles": (discharge_sum / float(cap_kwh)) if float(cap_kwh) > 0 else 0.0,
                            "import_after_mwh": float(np.nansum(import_after)) / 1000.0,
                            "export_after_mwh": float(np.nansum(export_after)) / 1000.0,
                            "curtailed_after_mwh": float(np.nansum(curt_after)) / 1000.0,
                        }
                        return bh, summary

                    # Debug průniku hodin mezi komunitním surplus a importy po OM
                    _overlap_hours = len(comm_import_ts.index.intersection(export_ts.index)) if "export_ts" in locals() else 0
                    st.caption(f"Debug průnik hodin (demand vs export): {_overlap_hours}")

                    colA, colB = st.columns([1, 2])
                    with colA:
                        if st.button("Analyzovat umístění (siting)"):
                            results = []
                            for s in sel_sites:
                                _, summ = simulate_for_site(s)
                                results.append(summ)
                            out = pd.DataFrame(results)
                            if not out.empty and {"charge_kwh", "discharge_kwh"}.issubset(out.columns):
                                if float(pd.to_numeric(out["charge_kwh"], errors="coerce").fillna(0).sum()) == 0 and float(pd.to_numeric(out["discharge_kwh"], errors="coerce").fillna(0).sum()) == 0:
                                    st.warning(
                                        "Siting analýza vyšla s nulovým charge/discharge pro všechny OM. "
                                        "Zkontroluj mapování komunitního přebytku (by_hour_after.csv) a importů po sdílení (allocations/eano)."
                                    )
                            # robustní řazení: starší verze může mít discharge_kwh místo discharge_mwh
                            if "discharge_mwh" not in out.columns and "discharge_kwh" in out.columns:
                                out["discharge_mwh"] = out["discharge_kwh"].astype(float) / 1000.0
                            sort_col = "discharge_mwh" if "discharge_mwh" in out.columns else ("discharge_kwh" if "discharge_kwh" in out.columns else None)
                            if sort_col:
                                out = out.sort_values([sort_col], ascending=False)
                            out_path = rp.csv_dir / "central_siting_analysis.csv"
                            out.to_csv(out_path, index=False, encoding="utf-8")
                            st.success(f"Hotovo: {out_path.name}")

                    with colB:
                        p_siting = rp.csv_dir / "central_siting_analysis.csv"
                        if p_siting.exists():
                            st.subheader(_w("central_siting_analysis.csv"))
                            sit = read_csv_auto(p_siting)
                            st.dataframe(sit, use_container_width=True)
                            best_site = st.selectbox("Vyber OM pro detailní průběh", sit["site"].astype(str).tolist())
                            snap_save = st.checkbox("Uložit tento detail jako variantu baterie", value=False, key="snap_central_save")
                            snap_name = ""
                            if snap_save:
                                _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                snap_name = st.text_input("Název varianty (centrální baterie – detail)", value=f"central_{_ts}", key="snap_central_name")

                            if st.button("Vytvořit detailní by-hour pro vybrané OM"):
                                bh, _ = simulate_for_site(best_site)
                                p_bh = rp.csv_dir / "by_hour_after_bat_central.csv"
                                bh.to_csv(p_bh, index=False, encoding="utf-8")
                                # config file
                                pd.DataFrame([{"site": best_site, "cap_kwh": float(cap_kwh)}]).to_csv(
                                    rp.csv_dir / "central_batt_config.csv", index=False, encoding="utf-8"
                                )
                                st.success(f"Uloženo: {p_bh.name} + central_batt_config.csv")
                                # optional snapshot
                                if snap_save and snap_name.strip():
                                    try:
                                        snap_dir = save_battery_snapshot(
                                            Path(rp.run_dir),
                                            snap_name.strip(),
                                            {
                                                "kind": "central_detail_by_hour",
                                                "site": str(best_site),
                                                "cap_kwh": float(cap_kwh),
                                            },
                                            files=[p_bh, rp.csv_dir / "central_batt_config.csv"],
                                        )
                                        st.success(f"Snapshot uložen: {snap_dir.relative_to(Path(rp.run_dir))}")
                                    except Exception as _e:
                                        st.warning(f"Snapshot se nepodařilo uložit: {_e}")

                                st.dataframe(bh.head(48), use_container_width=True)
with st.expander(_ui_section_title("🔋 Fáze 3A – Lokální baterie v OM", rp, required=["by_hour_after_bat_local.csv"], optional=["bat_local_cap_by_site.csv","step4a.log"]), expanded=False):
    st.header(_w("Fáze 3A – Lokální baterie v OM (citlivost, bez sdílení)"))
    st.subheader(_w("Varianty lokálních baterií (3A)"))
    st.caption("Upravuješ parametry baterií v aktivní variantě (patch). Potom spusť scénář 3A znovu. Změny se ukládají jen do varianty – `base` zůstává čistý.")

    _run_dir = Path(rp.run_dir)
    _active_variant = get_active_variant(_run_dir, _cfg)

    st.caption(
        "Pozn.: Citlivost počítáme **nezávisle po místě** (ostatní mají cap=0). "
        "Bruteforce kombinací přes všechna místa roste exponenciálně (např. 4^10 = 1 048 576 běhů) a nedává smysl."
    )

    # Výběr míst pro citlivost (když chceš jen pár sitů)
    # Seznam bereme primárně z odběrů (ean_o_long), aby to odpovídalo skutečným odběrným místům
    sites_for_pick: list[str] = []
    p_eano = rp.csv_dir / "ean_o_long.csv"
    if p_eano.exists():
        try:
            df_eano0 = read_csv_auto(p_eano)
            if "site" in df_eano0.columns:
                sites_for_pick = sorted(df_eano0["site"].astype(str).str.strip().unique().tolist())
        except Exception:
            sites_for_pick = []
    if not sites_for_pick:
        sites_for_pick = [str(s) for s in load_sites_from_any(rp)]

    default_pick = st.session_state.get("sens_sites", sites_for_pick)
    st.session_state["sens_sites"] = st.multiselect(
        "Místa, pro která spočítat citlivost",
        options=sites_for_pick,
        default=default_pick,
    )

    st.session_state["sens_keep_tmp"] = st.checkbox(
        "Nechat dočasné výstupy citlivosti (debug)",
        value=bool(st.session_state.get("sens_keep_tmp", False)),
    )



    def _sum_df_numeric(df: pd.DataFrame) -> pd.DataFrame:
        num = df.select_dtypes(include=["number"])
        if num.empty:
            return pd.DataFrame({"metric": [], "sum": []})
        s = num.sum(numeric_only=True).sort_index()
        return pd.DataFrame({"metric": s.index, "sum": s.values})

    def _parse_caps(s: str) -> list[float]:
        out=[]
        for part in str(s).replace(";", ",").split(","):
            part=str(part).strip()
            if not part:
                continue
            try:
                out.append(float(part))
            except Exception:
                pass
        return out

    def _load_kwp_map(csv_path: Path) -> dict[str, float]:
        if not csv_path.exists():
            return {}
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            return {}
        if "site" not in df.columns:
            return {}
        kwp_col = "kwp" if "kwp" in df.columns else None
        if kwp_col is None:
            # první numerický sloupec mimo site
            for c in df.columns:
                if c == "site":
                    continue
                if pd.api.types.is_numeric_dtype(df[c]):
                    kwp_col = c
                    break
        if kwp_col is None:
            return {}
        m={}
        for _,r in df.iterrows():
            site=str(r["site"]).strip()
            try:
                m[site]=float(r[kwp_col])
            except Exception:
                m[site]=0.0
        return m

    # seznam odběrných míst (ad 5) – primárně ean_o_long.csv
    sites = []
    p_eano_long = rp.csv_dir / "ean_o_long.csv"
    if p_eano_long.exists():
        try:
            df_eano_long = read_csv_auto(p_eano_long)
            if "site" in df_eano_long.columns:
                sites = sorted(df_eano_long["site"].astype(str).str.strip().unique().tolist())
        except Exception:
            sites = []
    if not sites:
        sites = load_sites_from_any(rp)

    cap_mode = st.radio(
        "Jak zadat varianty kapacit? (ad 5)",
        ["Jednotný seznam pro všechna místa", "Zvlášť pro každé místo"],
        index=1,
        key="cap_mode_step4",
    )

    if cap_mode == "Zvlášť pro každé místo":
        st.caption(_w("Zadej varianty kapacit pro každé místo zvlášť (odděl čárkou)."))
        default_caps = str(cfg.get("cap_kwh_list", "0,5,10,15"))

        if "cap_by_site_table" not in st.session_state:
            st.session_state["cap_by_site_table"] = pd.DataFrame({
                "site": pd.Series([str(s) for s in sites], dtype="string"),
                "cap_kwh_list": pd.Series([default_caps] * len(sites), dtype="string"),
            })
        else:
            df = st.session_state["cap_by_site_table"].copy()
            if "site" not in df.columns:
                df["site"] = pd.Series([], dtype="string")
            if "cap_kwh_list" not in df.columns:
                df["cap_kwh_list"] = pd.Series([], dtype="string")
            df["site"] = df["site"].astype("string")
            df["cap_kwh_list"] = df["cap_kwh_list"].astype("string")

            existing = set(df["site"].astype(str).tolist())
            missing = [str(s) for s in sites if str(s) not in existing]
            if missing:
                df = pd.concat([df, pd.DataFrame({
                    "site": pd.Series(missing, dtype="string"),
                    "cap_kwh_list": pd.Series([default_caps] * len(missing), dtype="string"),
                })], ignore_index=True)
            st.session_state["cap_by_site_table"] = df

        cap_df = st.data_editor(
            st.session_state["cap_by_site_table"],
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            column_config={
                "site": st.column_config.TextColumn("site"),
                "cap_kwh_list": st.column_config.TextColumn("varianty kapacit (kWh)"),
            },
            key="cap_by_site_editor",
        )
        st.session_state["cap_by_site_table"] = cap_df.copy()

        if st.button("Uložit cap_list_by_site.csv"):
            out = cap_df.copy()
            out["site"] = out["site"].astype(str)
            out["cap_kwh_list"] = out["cap_kwh_list"].astype(str)
            out.to_csv(rp.csv_dir / "cap_list_by_site.csv", index=False)
            st.success("Uloženo: cap_list_by_site.csv")
    else:
        st.info("Použije se jednotný seznam z levého panelu (Výchozí seznam kapacit).")


    def run_local_sensitivity() -> Path | None:
        """
        Rychlá citlivost bez bruteforce kombinací (lokální baterky bez sdílení):

        Pro každý vybraný site spočítá varianty kapacity *nezávisle* tak, že do Step4a pošle
        vstupní CSV odfiltrované jen na daný site. Tím dostaneme reálné by-hour chování pro konkrétní místo
        (a hlavně to není "agregát za celou komunitu", což by dávalo identické výsledky pro všechny).

        Výsledek ukládáme do local_sensitivity_long.csv (append + deduplikace podle site+cap_kwh).
        """
        eano_path = rp.csv_dir / "eano_after_pv.csv"
        eand_path = rp.csv_dir / "eand_after_pv.csv"
        if not eano_path.exists() or not eand_path.exists():
            st.error("Chybí eano_after_pv.csv / eand_after_pv.csv (spusť nejdřív Krok 2).")
            return None

        # --- načti vstupy (kvůli rychlosti filtrujeme v paměti a pro jednotlivé běhy zapisujeme malé CSV) ---
        try:
            eano_all = read_csv_auto(eano_path)
            eand_all = read_csv_auto(eand_path)
        except Exception as e:
            st.error(f"Nejde načíst vstupy pro citlivost: {e}")
            return None

        if "site" not in eano_all.columns or "site" not in eand_all.columns:
            st.error("Ve vstupních souborech chybí sloupec 'site'.")
            return None

        # --- constraints/kwp jsou volitelné (posíláme jen pokud jsou to skutečné soubory) ---
        constraints_path = rp.csv_dir / "site_constraints.csv"
        have_constraints = constraints_path.exists() and constraints_path.is_file()
        kwp_path = rp.csv_dir / "kwp_by_site.csv"
        have_kwp = kwp_path.exists() and kwp_path.is_file()

        # --- které sity počítat ---
        picked_sites = [str(s) for s in st.session_state.get("sens_sites", [])]
        if not picked_sites:
            st.warning("Nevybral(a) jsi žádná místa pro citlivost.")
            return None

        # --- helper: najdi sloupec podle kandidátů / substringů ---
        def _find_col(df: pd.DataFrame, candidates: list[str], substrings: list[str] | None = None) -> str | None:
            cols = list(df.columns)
            for c in candidates:
                if c in cols:
                    return c
            if substrings:
                low = {c: str(c).lower() for c in cols}
                for c, lc in low.items():
                    ok = True
                    for s in substrings:
                        if s not in lc:
                            ok = False
                            break
                    if ok:
                        return c
            return None

        # baseline (cap=0) dopočítáme bez spouštění Step4a: vezmeme import/export z after_pv
        # (0 varianta je vždy "no-battery", zbytečný běh)
        def _baseline_totals(site: str) -> dict:
            eano_s = eano_all.loc[eano_all["site"].astype(str).str.strip() == str(site).strip()].copy()
            eand_s = eand_all.loc[eand_all["site"].astype(str).str.strip() == str(site).strip()].copy()

            # Pokud existuje 'curtailed_kwh', přičteme ho k exportu po PV, aby baterka mohla nabíjet
            # i v režimu, kdy je export do sítě zakázán (přebytky se pak evidují jako curtailed).
            if "curtailed_kwh" in eand_s.columns:
                if "export_after_kwh" in eand_s.columns:
                    eand_s["export_after_kwh"] = eand_s["export_after_kwh"].fillna(0) + eand_s["curtailed_kwh"].fillna(0)
                elif "export_after_pv_kwh" in eand_s.columns:
                    eand_s["export_after_pv_kwh"] = eand_s["export_after_pv_kwh"].fillna(0) + eand_s["curtailed_kwh"].fillna(0)
                else:
                    # fallback: vytvoříme export_after_kwh
                    eand_s["export_after_kwh"] = eand_s["curtailed_kwh"].fillna(0)

            # import typicky v eand, export typicky v eano (ale hledáme v obou)
            imp_col = _find_col(eand_s, ["import_after_pv_kwh", "import_after_kwh", "import_kwh"], substrings=["import", "kwh"])
            exp_col = _find_col(eano_s, ["export_after_pv_kwh", "export_after_kwh", "export_kwh"], substrings=["export", "kwh"])

            if imp_col is None:
                imp_col = _find_col(eano_s, ["import_after_pv_kwh", "import_after_kwh", "import_kwh"], substrings=["import", "kwh"])
            if exp_col is None:
                exp_col = _find_col(eand_s, ["export_after_pv_kwh", "export_after_kwh", "export_kwh"], substrings=["export", "kwh"])

            import0_kwh = float(eand_s[imp_col].sum()) if imp_col and imp_col in eand_s.columns else (float(eano_s[imp_col].sum()) if imp_col and imp_col in eano_s.columns else float("nan"))
            export0_kwh = float(eano_s[exp_col].sum()) if exp_col and exp_col in eano_s.columns else (float(eand_s[exp_col].sum()) if exp_col and exp_col in eand_s.columns else float("nan"))

            return {
                "import0_mwh": import0_kwh / 1000.0 if not np.isnan(import0_kwh) else np.nan,
                "export0_mwh": export0_kwh / 1000.0 if not np.isnan(export0_kwh) else np.nan,
            }

        # --- kde uložit výsledky ---
        out_path = rp.csv_dir / "local_sensitivity_long.csv"
        now_iso = datetime.now().isoformat(timespec="seconds")

        # když už existuje, načti a budeme appendovat (a pak deduplikovat)
        prev = None
        if out_path.exists():
            try:
                prev = pd.read_csv(out_path)
            except Exception:
                prev = None

        # user volby
        keep_tmp = bool(st.session_state.get("sens_keep_tmp", False))
        skip_done = st.checkbox("Přeskočit už spočítané (site+cap)", value=True, key="sens_skip_done")
        show_progress = st.checkbox("Zobrazovat průběh (pomalejší UI)", value=False, key="sens_show_progress")

        done_set = set()
        if skip_done and prev is not None and {"site", "cap_kwh"}.issubset(set(prev.columns)):
            done_set = set(zip(prev["site"].astype(str), prev["cap_kwh"].astype(float)))

        rows = []

        tmp_root = rp.csv_dir / "_tmp_step4_sensitivity"
        if tmp_root.exists() and not keep_tmp:
            try:
                import shutil
                shutil.rmtree(tmp_root)
            except Exception:
                pass
        tmp_root.mkdir(parents=True, exist_ok=True)

        # progress bar
        total_jobs = 0
        caps_by_site = {}
        for site in picked_sites:
            # varianty kapacit bereme z tabulky cap_by_site_table, pokud existuje, jinak z cfg["cap_kwh_list"]
            caps = []
            if "cap_by_site_table" in st.session_state:
                df_caps = st.session_state["cap_by_site_table"].copy()
                if "site" in df_caps.columns and "cap_kwh_list" in df_caps.columns:
                    m = df_caps.loc[df_caps["site"].astype(str).str.strip() == str(site).strip()]
                    if not m.empty:
                        caps = _parse_caps(m["cap_kwh_list"].iloc[0])
            if not caps:
                caps = _parse_caps(str(cfg.get("cap_kwh_list", "")))

            # 1) ponech i 0 (uživatel ji chce vidět/vybírat); reálný běh pro 0 nespouštíme, vytvoříme baseline řádek
            caps = [float(c) for c in caps if float(c) >= 0.0]

            # 2) deduplikuj + seřaď
            caps = sorted(list(dict.fromkeys(caps)))
            caps_by_site[site] = caps
            total_jobs += len(caps)

        if total_jobs == 0:
            st.warning("Nemám co počítat: pro vybraná místa nejsou žádné kapacity.")
            return None

        prog = st.progress(0)
        done_jobs = 0

        for site in picked_sites:
            caps = caps_by_site.get(site, [])
            if not caps:
                continue

            base = _baseline_totals(site)

            # připrav site-only vstupy (jednou na site)
            eano_s = eano_all.loc[eano_all["site"].astype(str).str.strip() == str(site).strip()].copy()
            eand_s = eand_all.loc[eand_all["site"].astype(str).str.strip() == str(site).strip()].copy()

            # Pokud existuje 'curtailed_kwh', přičteme ho k exportu po PV, aby baterka mohla nabíjet
            # i v režimu, kdy je export do sítě zakázán (přebytky se pak evidují jako curtailed).
            if "curtailed_kwh" in eand_s.columns:
                if "export_after_kwh" in eand_s.columns:
                    eand_s["export_after_kwh"] = eand_s["export_after_kwh"].fillna(0) + eand_s["curtailed_kwh"].fillna(0)
                elif "export_after_pv_kwh" in eand_s.columns:
                    eand_s["export_after_pv_kwh"] = eand_s["export_after_pv_kwh"].fillna(0) + eand_s["curtailed_kwh"].fillna(0)
                else:
                    # fallback: vytvoříme export_after_kwh
                    eand_s["export_after_kwh"] = eand_s["curtailed_kwh"].fillna(0)

            site_dir = tmp_root / _slug(site)
            site_dir.mkdir(parents=True, exist_ok=True)

            eano_site_path = site_dir / "eano_after_pv__site.csv"
            eand_site_path = site_dir / "eand_after_pv__site.csv"
            eano_s.to_csv(eano_site_path, index=False)
            eand_s.to_csv(eand_site_path, index=False)

            for cap in caps:
                key = (site, float(cap))
                if skip_done and key in done_set:
                    done_jobs += 1
                    prog.progress(min(done_jobs / total_jobs, 1.0))
                    continue

                run_dir = site_dir / f"{float(cap):g}kwh"
                run_dir.mkdir(parents=True, exist_ok=True)

                cap_path = run_dir / "bat_local_cap_by_site.csv"
                pd.DataFrame({"site": [site], "cap_kwh": [float(cap)]}).to_csv(cap_path, index=False)

                cmd = [
                    sys.executable, "-m", "ec_balance.pipeline.step4a_batt_local_byhour",
                    "--eano_after_pv_csv", str(eano_site_path),
                    "--eand_after_pv_csv", str(eand_site_path),
                    "--outdir", str(run_dir),
                    "--cap_by_site_csv", str(cap_path),
                ]
                if have_constraints:
                    cmd += ["--constraints_csv", str(constraints_path)]
                if have_kwp:
                    cmd += ["--kwp_csv", str(kwp_path)]

                log_path = rp.logs_dir / f"step4_sens_{_slug(site)}_{float(cap):g}.log"
                run_cmd(cmd, cwd=APP_ROOT, log_path=log_path)

                bh_path = run_dir / "by_hour_after_bat_local.csv"
                if not bh_path.exists():
                    st.error(f"Chybí by_hour_after_bat_local.csv pro {site}, cap={cap:g}.")
                    done_jobs += 1
                    prog.progress(min(done_jobs / total_jobs, 1.0))
                    continue

                try:
                    bh = read_csv_auto(bh_path)
                except Exception as e:
                    st.error(f"Nejde načíst by_hour_after_bat_local.csv pro {site}, cap={cap:g}: {e}")
                    done_jobs += 1
                    prog.progress(min(done_jobs / total_jobs, 1.0))
                    continue

                            # KPI z by-hour (robustně)
                imp_after_mwh = (bh["import_after_batt_kwh"].sum() / 1000.0) if "import_after_batt_kwh" in bh.columns else np.nan
                exp_after_mwh = (bh["export_after_batt_kwh"].sum() / 1000.0) if "export_after_batt_kwh" in bh.columns else np.nan

                # 1) Charge: preferujeme explicitní sloupce, ale když v by-hour nejsou,
                #    dopočteme z rozdílu exportu vůči cap=0 (tj. co jsme "sežrali" do baterky).
                charge_kwh = 0.0
                has_charge_cols = False
                for col in ["own_stored_kwh", "shared_stored_kwh", "charge_kwh", "batt_charge_kwh"]:
                    if col in bh.columns:
                        has_charge_cols = True
                        charge_kwh += float(bh[col].sum())

                end_soc_kwh = float(bh["soc_kwh"].iloc[-1]) if "soc_kwh" in bh.columns and len(bh) else np.nan

                base_imp0 = float(base.get("import0_mwh", np.nan)) if base.get("import0_mwh", np.nan) is not None else np.nan
                base_exp0 = float(base.get("export0_mwh", np.nan)) if base.get("export0_mwh", np.nan) is not None else np.nan

                if (not has_charge_cols) and (not np.isnan(base_exp0)) and (not np.isnan(exp_after_mwh)):
                    # export0 - export_after ≈ energie uložená do baterky (+/- malé ztráty)
                    charge_kwh = max((base_exp0 - exp_after_mwh) * 1000.0, 0.0)

            

                # 1b) Pokud je export do sítě zakázaný (export0=0) nebo export delta nic neřekne,
                #     dopočítáme nabíjení z průběhu SOC: součet kladných změn SOC v čase.
                #     Tohle zachytí i nabíjení ze "zmařené" (curtailed) energie.
                if (charge_kwh <= 0.0) and ("soc_kwh" in bh.columns) and (len(bh) > 1):
                    try:
                        soc = bh["soc_kwh"].astype(float).to_numpy()
                        dsoc = np.diff(soc)
                        charge_kwh = float(np.clip(dsoc, 0, None).sum())
                    except Exception:
                        pass
    # 2) Discharge: robustní proxy = úspora importu vůči cap=0
                discharge_kwh = 0.0
                if (not np.isnan(base_imp0)) and (not np.isnan(imp_after_mwh)):
                    discharge_kwh = max((base_imp0 - imp_after_mwh) * 1000.0, 0.0)
                else:
                    # fallback: když nemáme baseline import0, zkusíme to odvodit z charge a SOC (start bereme 0)
                    if not np.isnan(end_soc_kwh):
                        discharge_kwh = max(charge_kwh - end_soc_kwh, 0.0)

                discharge_mwh = discharge_kwh / 1000.0

                # Ekvivalentní cykly: používáme discharge (užitečná energie do spotřeby)
                eq_cycles = (discharge_kwh / float(cap)) if float(cap) > 0 else 0.0

                row = {
                    "computed_at": now_iso,
                    "site": site,
                    "cap_kwh": float(cap),
                    "import0_mwh": base.get("import0_mwh", np.nan),
                    "export0_mwh": base.get("export0_mwh", np.nan),
                    "import_mwh": imp_after_mwh,
                    "export_mwh": exp_after_mwh,
                    "charge_total_mwh": charge_kwh / 1000.0,
                    "discharge_mwh": discharge_mwh,
                    "end_soc_kwh": end_soc_kwh,
                    "eq_cycles": eq_cycles,
                    "delta_import_mwh_vs0": (base.get("import0_mwh", np.nan) - imp_after_mwh) if not np.isnan(base.get("import0_mwh", np.nan)) and not np.isnan(imp_after_mwh) else np.nan,
                    "delta_export_mwh_vs0": (base.get("export0_mwh", np.nan) - exp_after_mwh) if not np.isnan(base.get("export0_mwh", np.nan)) and not np.isnan(exp_after_mwh) else np.nan,
                    "run_dir": str(run_dir),
                    "is_interpolated": False,
                }
                rows.append(row)

                if show_progress:
                    st.write(f"{site} | cap={cap:g} kWh → Δimport={row['delta_import_mwh_vs0']:.3f} MWh, cycles={row['eq_cycles']:.1f}")

                done_jobs += 1
                prog.progress(min(done_jobs / total_jobs, 1.0))

            # úklid per-site, pokud nechceme tmp
            if (not keep_tmp) and site_dir.exists():
                # smažeme jen výstupy jednotlivých capů, necháme site-only vstupy (nevadí)
                pass

        sens = pd.DataFrame(rows)
        if sens.empty:
            st.warning("Nevznikl žádný výstup citlivosti (zkontroluj logy v runs/.../logs).")
            return None

        # append + dedup
        if prev is not None and not prev.empty:
            merged = pd.concat([prev, sens], ignore_index=True)
        else:
            merged = sens

        # deduplikace: nech poslední computed_at pro stejný (site, cap_kwh)
        if {"site", "cap_kwh", "computed_at"}.issubset(set(merged.columns)):
            merged["_cap"] = merged["cap_kwh"].astype(float)
            merged["_t"] = pd.to_datetime(merged["computed_at"], errors="coerce")
            merged = merged.sort_values(["site", "_cap", "_t"]).drop_duplicates(["site", "_cap"], keep="last")
            merged = merged.drop(columns=["_cap", "_t"])

        merged.to_csv(out_path, index=False)
        return out_path

    if st.button("Spustit Krok 4 (citlivost)"):
        p = run_local_sensitivity()
        if p:
            st.success(f"Hotovo: {p.name}")

    p_sens = rp.csv_dir / "local_sensitivity_long.csv"
    if p_sens.exists():
        sens = pd.read_csv(p_sens)
        st.subheader("Výstup: local_sensitivity_long.csv")
        st.dataframe(sens, use_container_width=True)
        with st.expander("Souhrny (ad 8)"):
            st.dataframe(_sum_df_numeric(sens), use_container_width=True)

            st.subheader("Interpolace chybějících variant (volitelně)")
        st.caption(
            "Když si necháš spočítat jen 2–3 kapacity na místo, můžeš si zbytek **hrubě aproximovat** lineární interpolací. "
            "Užitečné pro rychlý výběr kandidátů; finální vítězné varianty stejně doporučuju dopočítat 'naostro'."
        )

        if st.button("Vytvořit interpolovaný odhad (bez dalších běhů)"):
            desired_caps_by_site = {}
            if "cap_by_site_table" in st.session_state:
                df_caps = st.session_state["cap_by_site_table"].copy()
                if {"site", "cap_kwh_list"}.issubset(set(df_caps.columns)):
                    for _, r in df_caps.iterrows():
                        site = str(r["site"])
                        caps = [c for c in _parse_caps(r["cap_kwh_list"]) if float(c) >= 0.0]
                        desired_caps_by_site[site] = sorted(list(dict.fromkeys([float(c) for c in caps])))
            # fallback: použij aktuálně spočítané
            if not desired_caps_by_site:
                for site in sens["site"].astype(str).unique():
                    desired_caps_by_site[str(site)] = sorted(sens.loc[sens["site"].astype(str)==str(site), "cap_kwh"].astype(float).unique().tolist())

            metrics = ["delta_import_mwh_vs0", "delta_export_mwh_vs0", "discharge_mwh", "charge_total_mwh"]
            rows_i = []
            now_i = datetime.now().isoformat(timespec="seconds")

            for site, desired_caps in desired_caps_by_site.items():
                s_site = sens.loc[sens["site"].astype(str) == str(site)].copy()
                if s_site.empty:
                    continue
                s_site["cap_kwh"] = s_site["cap_kwh"].astype(float)
                s_site = s_site.sort_values("cap_kwh")

                known_caps = s_site["cap_kwh"].unique().tolist()
                min_cap = float(min(known_caps))
                max_cap = float(max(known_caps))

                # jen pro caps "mezi" známými body
                for cap in desired_caps:
                    cap = float(cap)
                    if cap in known_caps:
                        continue
                    if cap < min_cap or cap > max_cap:
                        continue

                    lo = s_site[s_site["cap_kwh"] < cap].tail(1)
                    hi = s_site[s_site["cap_kwh"] > cap].head(1)
                    if lo.empty or hi.empty:
                        continue
                    lo_cap = float(lo["cap_kwh"].iloc[0])
                    hi_cap = float(hi["cap_kwh"].iloc[0])
                    if hi_cap <= lo_cap:
                        continue
                    w = (cap - lo_cap) / (hi_cap - lo_cap)

                    row = {
                        "computed_at": now_i,
                        "site": str(site),
                        "cap_kwh": cap,
                        "is_interpolated": True,
                    }
                    # přenes baseline sloupce, pokud existují
                    for base_col in ["import0_mwh", "export0_mwh"]:
                        if base_col in s_site.columns:
                            row[base_col] = float(lo[base_col].iloc[0])  # stejné pro všechny capy

                    # lineární interpolace metrik
                    for met in metrics:
                        if met in s_site.columns:
                            a = float(lo[met].iloc[0])
                            b = float(hi[met].iloc[0])
                            row[met] = a + w * (b - a)

                    # dopočti eq_cycles z discharge
                    if "discharge_mwh" in row and cap > 0:
                        row["eq_cycles"] = (row["discharge_mwh"] * 1000.0) / cap

                    rows_i.append(row)

            if not rows_i:
                st.info("Nemám co interpolovat (buď už máš všechny body, nebo chybějí krajní body pro odhad).")
            else:
                interp_df = pd.DataFrame(rows_i)
                out_i = rp.csv_dir / "local_sensitivity_interp.csv"
                interp_df.to_csv(out_i, index=False)
                st.success(f"Uloženo: {out_i.name}")
                st.dataframe(interp_df.sort_values(["site","cap_kwh"]), use_container_width=True)

        st.subheader("Výběr varianty baterie pro navazující výpočty")
        kwp_map = _load_kwp_map(rp.csv_dir / "kwp_by_site.csv")
        use_limit = bool(cfg.get("use_max_kwh_per_kwp", False))
        max_ratio = float(cfg.get("max_kwh_per_kwp", 0.0))
        only_pos = bool(cfg.get("limit_only_when_kwp_positive", True))

        chosen=[]
        for site in sorted(sens["site"].astype(str).unique().tolist()):
            s_site = sens[sens["site"].astype(str).str.strip()==str(site).strip()].copy()
            options = sorted(s_site["cap_kwh"].astype(float).unique().tolist())
            if 0.0 not in options:
                options = [0.0] + options

            if use_limit and max_ratio > 0:
                kwp = float(kwp_map.get(site, 0.0))
                if (not only_pos) or (kwp > 0):
                    max_cap = kwp * max_ratio
                    options = [c for c in options if c <= max_cap + 1e-9] or [0.0]

            # default = nejlepší discharge
            best_cap = 0.0
            best = s_site.loc[s_site["cap_kwh"].astype(float).isin(options)].sort_values(["discharge_mwh","eq_cycles"], ascending=[False, True]).head(1)
            if not best.empty:
                best_cap = float(best["cap_kwh"].iloc[0])

            col1, col2 = st.columns([2,3])
            with col1:
                st.write(f"**{site}** (kWp={kwp_map.get(site,0.0):g})")
            with col2:
                sel = st.selectbox(
                    f"Kapacita (kWh) – {site}",
                    options=options,
                    index=options.index(best_cap) if best_cap in options else 0,
                    key=f"sel_cap_{site}",
                )
            chosen.append({"site": site, "cap_kwh": float(sel)})

        chosen_df = pd.DataFrame(chosen)
        st.dataframe(chosen_df, use_container_width=True)

        if st.button("Uložit vybrané kapacity → bat_local_cap_by_site.csv"):
            chosen_df.to_csv(rp.csv_dir / "bat_local_cap_by_site.csv", index=False)
            st.success("Uloženo: bat_local_cap_by_site.csv")

    # ----------------------------
    # Step 4a – local battery by-hour
    # ----------------------------
    st.header("Fáze 3A – Lokální baterie v OM (hodinový výpočet)")

    if st.button("Spustit Krok 4a"):
        need = ["eano_after_pv.csv","eand_after_pv.csv"]
        missing = [f for f in need if not (rp.csv_dir / f).exists()]
        if missing:
            st.error(f"Chybí soubory z Kroku 2: {', '.join(missing)}")
        else:
            # Připravíme vstup pro baterku: pokud eand_after_pv.csv obsahuje 'curtailed_kwh',
            # přičteme ho k exportu po PV, aby baterka mohla nabíjet i ze zmařené energie.
            eand_in = rp.csv_dir / "eand_after_pv.csv"
            eand_for_batt = eand_in
            try:
                _eand_tmp = pd.read_csv(eand_in)
                if "curtailed_kwh" in _eand_tmp.columns:
                    if "export_after_kwh" in _eand_tmp.columns:
                        _eand_tmp["export_after_kwh"] = _eand_tmp["export_after_kwh"].fillna(0) + _eand_tmp["curtailed_kwh"].fillna(0)
                    elif "export_after_pv_kwh" in _eand_tmp.columns:
                        _eand_tmp["export_after_pv_kwh"] = _eand_tmp["export_after_pv_kwh"].fillna(0) + _eand_tmp["curtailed_kwh"].fillna(0)
                    else:
                        _eand_tmp["export_after_kwh"] = _eand_tmp["curtailed_kwh"].fillna(0)
                    eand_for_batt = rp.csv_dir / "eand_after_pv__for_batt.csv"
                    _eand_tmp.to_csv(eand_for_batt, index=False)
            except Exception:
                eand_for_batt = eand_in

            cmd = [
                sys.executable, "-m", "ec_balance.pipeline.step4a_batt_local_byhour",
                "--eano_after_pv_csv", str(rp.csv_dir / "eano_after_pv.csv"),
                "--eand_after_pv_csv", str(eand_for_batt),
                "--outdir", str(rp.csv_dir),
                "--eta_c", str(cfg.get("eta_c",0.95)),
                "--eta_d", str(cfg.get("eta_d",0.95)),
            ]
            cap_by_site = rp.csv_dir / "bat_local_cap_by_site.csv"
            if cap_by_site.exists():
                cmd += ["--cap_by_site_csv", str(cap_by_site)]
            constraints_path = rp.csv_dir / "site_constraints.csv"
            if constraints_path.exists():
                cmd += ["--constraints_csv", str(constraints_path)]
            kwp_path = rp.csv_dir / "kwp_by_site.csv"
            if kwp_path.exists():
                cmd += ["--kwp_csv", str(kwp_path)]
            run_cmd(cmd, cwd=APP_ROOT, log_path=rp.logs_dir / "step4a.log")

    p_bh = rp.csv_dir / "by_hour_after_bat_local.csv"
    if p_bh.exists():
        bh = read_csv_auto(p_bh)
        st.subheader("Výstup: by_hour_after_bat_local.csv")
        # Battery snapshot tagging (lokální baterie) – užitečné pro porovnání variant kapacit
        with st.expander("📌 Uložit / označit tento výsledek jako variantu baterií", expanded=False):
            _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            snap_name_l = st.text_input("Název varianty (lokální baterie)", value=f"local_{_ts}", key="snap_local_name")
            if st.button("Uložit snapshot (config + by-hour)", key="snap_local_save"):
                files = [rp.csv_dir / "by_hour_after_bat_local.csv"]
                for extra in ["bat_local_cap_by_site.csv", "local_sensitivity_long.csv", "step4a.log"]:
                    fp = (rp.csv_dir / extra) if extra.endswith(".csv") else (rp.logs_dir / extra)
                    if fp.exists():
                        files.append(fp)
                try:
                    snap_dir = save_battery_snapshot(
                        Path(rp.run_dir),
                        snap_name_l.strip() if snap_name_l.strip() else f"local_{_ts}",
                        {"kind": "local_batt_by_hour", "note": "snapshot po Krok 4a"},
                        files=files,
                    )
                    st.success(f"Snapshot uložen: {snap_dir.relative_to(Path(rp.run_dir))}")
                except Exception as _e:
                    st.warning(f"Snapshot se nepodařilo uložit: {_e}")

        st.dataframe(bh.head(200), use_container_width=True)
        with st.expander("Souhrny (ad 8)"):
            st.dataframe(_sum_df_numeric(bh), use_container_width=True)

    # ----------------------------
    # Step 4b – battery economics (local vs central separately)
    # ----------------------------
with st.expander(_ui_section_title("💰 Fáze 4 – Ekonomické vyhodnocení scénářů", rp, required=["central_econ_best.csv"], optional=["central_siting_analysis.csv","central_econ_sensitivity.csv"]), expanded=False):
    st.header("Fáze 4 – Ekonomické vyhodnocení scénářů baterií")

    def compute_econ(kind: str):
        try:
            from ec_balance.pipeline import step4b_batt_econ as econ
        except Exception as e:
            st.error(f"Nešlo importovat step4b_batt_econ: {e}")
            return

        if kind == "local":
            bh_path = rp.csv_dir / "by_hour_after_bat_local.csv"
            out_path = rp.csv_dir / "local_econ_best.csv"
            price_per_kwh = float(cfg.get("local_price_per_kwh", 0.0))
            fixed_cost = float(cfg.get("local_fixed_cost", 0.0))
        else:
            bh_path = rp.csv_dir / "by_hour_after_bat_central.csv"
            out_path = rp.csv_dir / "central_econ_best.csv"
            price_per_kwh = float(cfg.get("central_price_per_kwh", 0.0))
            fixed_cost = float(cfg.get("central_fixed_cost", 0.0))

        if not bh_path.exists():
            st.error(f"Chybí {bh_path.name}.")
            return

        bh = pd.read_csv(bh_path)
        shift_kwh = econ._sum_discharge_kwh(bh)
        cap_kwh = econ._estimate_cap_kwh(bh, meta_df=None)
        d = econ._econ_summary(
            energy_shift_kwh=shift_kwh,
            cap_kwh=cap_kwh,
            price_commodity_mwh=float(cfg["price_commodity_mwh"]),
            price_distribution_mwh=float(cfg["price_distribution_mwh"]),
            price_feed_in_mwh=float(cfg["price_feed_in_mwh"]),
            price_per_kwh=price_per_kwh,
            fixed_cost=fixed_cost,
            years=int(cfg.get("project_years", 15)),
            discount=float(cfg.get("discount_rate", 0.05)),
        )
        pd.DataFrame([d]).to_csv(out_path, index=False)
        st.success(f"Hotovo: {out_path.name}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Spočítat ekonomiku – lokální"):
            compute_econ("local")
    with col2:
        if st.button("Spočítat ekonomiku – centrální"):
            compute_econ("central")

    p_le = rp.csv_dir / "local_econ_best.csv"
    p_ce = rp.csv_dir / "central_econ_best.csv"
    cols = st.columns(2)
    with cols[0]:
        if p_le.exists():
            st.subheader("local_econ_best.csv")
            st.dataframe(pd.read_csv(p_le), use_container_width=True)
    with cols[1]:
        if p_ce.exists():
            st.subheader("central_econ_best.csv")
            st.dataframe(pd.read_csv(p_ce), use_container_width=True)



    # ----------------------------
    # Celkové výsledky (tabulky + grafy)
    # ----------------------------
with st.expander(_ui_section_title("📊 Výsledky", rp, required=["by_hour_after.csv"], optional=["by_hour_after_bat_local.csv","by_hour_after_bat_central.csv"]), expanded=True):
    st.header("Výsledky")

    # --- Import výsledků z jiného projektu/běhu (bez přepočtu) ---


# Import výsledků z jiného běhu je přesunut do Fáze 1 (Vstupy) – aby se dalo obnovit run bez přepočtu.
    try:
        kpi_pack, hourly_pack, monthly_pack = compute_overview(rp)
    except Exception as e:
        st.error(f"Nepodařilo se sestavit souhrn výsledků: {e}")
        st.exception(e)
        st.code(traceback.format_exc())
        kpi_pack, hourly_pack, monthly_pack = None, {}, {}

    if kpi_pack is not None:
        tabs = st.tabs(["📋 Souhrn a KPI", "📅 Měsíční srovnání", "📈 Hodinová diagnostika", "🔖 Varianty baterií"])

        with tabs[0]:
            st.caption("Přehled hlavních KPI a srovnání scénářů na jednom místě.")
            st.subheader("Souhrnné metriky a porovnání scénářů")
            st.caption("Finální souhrn za celý výpočet. Níže je detail KPI a porovnání scénářů.")

            kpi_df = kpi_pack.get("KPI", pd.DataFrame()).copy()
            scen_df_view = kpi_pack.get("Scénáře", pd.DataFrame()).copy()

            c1, c2 = st.columns([1.2, 1])
            with c1:
                st.markdown("**KPI přehled**")
                st.dataframe(_ui_prepare_kpi_df(kpi_df), use_container_width=True, hide_index=True)
            with c2:
                st.markdown("**Rychlé porovnání scénářů**")
                st.dataframe(_ui_prepare_scen_df(scen_df_view), use_container_width=True, hide_index=True)

        with tabs[1]:
            st.caption("Vybranou metriku porovnáváš mezi scénáři vedle sebe; pod tím je jedno master skládané zobrazení pro zvolený scénář.")
            st.subheader("Měsíční součty a srovnání scénářů")
            st.caption("Vybraná metrika = sloupce vedle sebe podle scénáře. Master graf = skládané složky pro jeden scénář.")

            metric_opts = list(monthly_pack.keys())
            metric = st.selectbox("Vyber metriku", metric_opts, index=0, key="monthly_metric_select")
            scen_series = monthly_pack.get(metric, {})

            all_idx = None
            for s in scen_series.values():
                if isinstance(s, pd.Series) and not s.empty:
                    idx = s.index.astype(str)
                    all_idx = idx if all_idx is None else all_idx.union(idx)
            if all_idx is None or len(all_idx) == 0:
                st.info("Nemám z čeho vykreslit měsíční grafy (chybí by-hour soubory).")
            else:
                out = pd.DataFrame(index=sorted(all_idx))
                for scen, ser in scen_series.items():
                    if isinstance(ser, pd.Series) and not ser.empty:
                        out[scen] = ser.reindex(pd.PeriodIndex(out.index, freq="M")).values
                    else:
                        out[scen] = 0.0
                out = out.fillna(0.0).astype(float)
                out_mwh = out / 1000.0
                out_mwh.index = out_mwh.index.astype(str)

                left, right = st.columns([1.35, 1])
                with left:
                    st.markdown("**Vybraná metrika – porovnání scénářů (grouped)**")
                    chart_df = out_mwh.reset_index().rename(columns={"index": "Měsíc"})
                    chart_long = chart_df.melt(id_vars=["Měsíc"], var_name="Scénář", value_name="MWh")
                    ch = (
                        alt.Chart(chart_long)
                        .mark_bar()
                        .encode(
                            x=alt.X("Měsíc:N", sort=list(chart_df["Měsíc"]), title=None),
                            xOffset=alt.XOffset("Scénář:N"),
                            y=alt.Y("MWh:Q", title=f"{metric} [MWh]"),
                            tooltip=["Měsíc:N", "Scénář:N", alt.Tooltip("MWh:Q", format=",.3f")],
                        )
                        .properties(height=340)
                    )
                    st.altair_chart(ch, use_container_width=True)

                with right:
                    st.markdown("**Master zobrazení – skládané složky (1 scénář)**")
                    master_candidates = [
                        ("Import (kWh)", "Import"),
                        ("Export (kWh)", "Export"),
                        ("Curtailment (kWh)", "Curtailment"),
                        ("Sdílení (kWh)", "Sdílení"),
                    ]
                    available_master = [(k, lbl) for k, lbl in master_candidates if k in monthly_pack]
                    scen_master_opts = [c for c in out_mwh.columns]
                    scen_master = (
                        st.selectbox(
                            "Scénář pro master graf",
                            scen_master_opts,
                            index=min(2, len(scen_master_opts)-1) if scen_master_opts else 0,
                            key="monthly_master_scen",
                        )
                        if scen_master_opts else None
                    )

                    if not available_master or scen_master is None:
                        st.info("Master graf nelze sestavit (chybí měsíční řady import/export/curtailment/sdílení).")
                    else:
                        master = pd.DataFrame(index=out_mwh.index)
                        for k_src, lbl in available_master:
                            ser = monthly_pack.get(k_src, {}).get(scen_master)
                            if isinstance(ser, pd.Series) and not ser.empty:
                                master[lbl] = (ser.reindex(pd.PeriodIndex(master.index, freq="M")).astype(float).values) / 1000.0
                            else:
                                master[lbl] = 0.0
                        master = master.fillna(0.0).reset_index().rename(columns={"index": "Měsíc"})
                        master_long = master.melt(id_vars=["Měsíc"], var_name="Složka", value_name="MWh")
                        mch = (
                            alt.Chart(master_long)
                            .mark_bar()
                            .encode(
                                x=alt.X("Měsíc:N", sort=list(master["Měsíc"]), title=None),
                                y=alt.Y("MWh:Q", title=f"{scen_master} [MWh]", stack=True),
                                color=alt.Color("Složka:N", legend=alt.Legend(orient="bottom")),
                                tooltip=["Měsíc:N", "Složka:N", alt.Tooltip("MWh:Q", format=",.3f")],
                            )
                            .properties(height=340)
                        )
                        st.altair_chart(mch, use_container_width=True)

                with st.expander("Tabulka měsíčních hodnot (vybraná metrika)", expanded=False):
                    st.dataframe(out_mwh, use_container_width=True)

        with tabs[2]:
            st.caption("Detailní průběhy po hodinách pro diagnostiku konkrétního scénáře a metriky.")
            st.subheader("Hodinové průběhy (detailní diagnostika)")
            st.caption("Interaktivní rychlý pohled do by-hour dat bez zásahu do výpočetní logiky.")

            scen_opts = [k for k in ["Původní", "Po FVE", "Po sdílení", "Po bat. centrální", "Po bat. lokální"] if k in hourly_pack]
            if not scen_opts:
                st.info("Nemám k dispozici žádné by-hour průběhy.")
            else:
                top_l, top_r = st.columns([1.2, 1])
                with top_l:
                    scen = st.selectbox("Scénář", scen_opts, index=min(2, len(scen_opts)-1), key="hourly_scen")

                dfh = hourly_pack[scen].copy()
                dfh = _ensure_datetime_col(dfh, "datetime")
                numeric_cols = [c for c in list(dict.fromkeys(dfh.columns)) if c != "datetime" and pd.api.types.is_numeric_dtype(pd.to_numeric(_as_series(dfh, c), errors="coerce"))]
                if not numeric_cols:
                    st.info("Ve vybraném scénáři nejsou žádné číselné sloupce pro graf.")
                else:
                    with top_r:
                        col = st.selectbox("Sloupec", numeric_cols, index=0, key="hourly_col")
                    view = st.radio("Zobrazení", ["Průměrný den (hodina 0–23)", "Časová řada (rok)"], horizontal=True, key="hourly_view")

                    s = pd.to_numeric(_as_series(dfh, col), errors="coerce").fillna(0.0)

                    kc1, kc2, kc3 = st.columns(3)
                    kc1.metric("Průměr", f"{float(s.mean()):,.3f}")
                    kc2.metric("Maximum", f"{float(s.max()):,.3f}")
                    kc3.metric("Součet", f"{float(s.sum()):,.3f}")

                    if view.startswith("Průměrný"):
                        prof = s.groupby(dfh["datetime"].dt.hour).mean()
                        st.line_chart(prof)
                    else:
                        tmp = dfh[["datetime"]].copy()
                        tmp[col] = s.values
                        tmp = tmp.set_index("datetime")
                        st.line_chart(tmp[[col]])

                    with st.expander("Náhled zdrojových hodinových dat (prvních 200 řádků)", expanded=False):
                        preview = dfh[["datetime"]].copy()
                        preview[col] = s.values
                        st.dataframe(preview.head(200), use_container_width=True, hide_index=True)
        with tabs[3]:
            st.caption("Uložené snapshoty/varianty baterií v rámci tohoto běhu. Užitečné pro rychlé porovnání bez dalšího přepočtu.")
            snap_root = rp.run_dir / "_variants" / "_battery_snapshots"
            if not snap_root.exists():
                st.info("V tomto běhu zatím nejsou uložené žádné varianty baterií (snapshoty).")
            else:
                snap_dirs = sorted([d for d in snap_root.iterdir() if d.is_dir()], key=lambda x: x.name)
                snap_names = [d.name for d in snap_dirs]
                cols = st.columns([2, 2, 1])
                with cols[0]:
                    a_name = st.selectbox("Varianta A", snap_names, index=0, key="snapA")
                with cols[1]:
                    compare = st.checkbox("Porovnat s variantou B", value=False)
                    b_name = st.selectbox("Varianta B", snap_names, index=min(1, len(snap_names)-1), key="snapB", disabled=not compare)
                with cols[2]:
                    show_files = st.checkbox("Zobrazit soubory", value=False)

                def _read_meta(folder: Path) -> dict:
                    mp = folder / "meta.json"
                    if mp.exists():
                        try:
                            return json.loads(mp.read_text(encoding="utf-8"))
                        except Exception:
                            return {}
                    return {}

                def _snapshot_tables(folder: Path):
                    # Prefer local, then central; fallback to any by_hour*.csv
                    candidates = [
                        folder / "by_hour_after_bat_local.csv",
                        folder / "by_hour_after_bat_central.csv",
                        folder / "by_hour_after_bat.csv",
                    ]
                    for c in candidates:
                        if c.exists():
                            return c
                    # fallback: first csv with by_hour
                    for c in folder.glob("by_hour*.csv"):
                        return c
                    return None

                def _sum_col(df: pd.DataFrame, col: str) -> float:
                    if col not in df.columns:
                        return float("nan")
                    s = pd.to_numeric(_as_series(df, col), errors="coerce").fillna(0.0)
                    return float(s.sum())

                def _snapshot_kpis(folder: Path) -> dict:
                    p = _snapshot_tables(folder)
                    if p is None:
                        return {"_source": "—", "Import (MWh)": float("nan")}
                    df = pd.read_csv(p)
                    out = {"_source": p.name}
                    # common
                    out["Import (MWh)"] = _sum_col(df, "import_kwh") / 1000.0
                    out["Export (MWh)"] = _sum_col(df, "export_kwh") / 1000.0
                    out["Curtailment (MWh)"] = _sum_col(df, "curtailed_kwh") / 1000.0
                    out["Sdílení (MWh)"] = _sum_col(df, "shared_kwh") / 1000.0
                    # battery (best effort)
                    out["Nabití bat. (MWh)"] = _sum_col(df, "batt_charge_kwh") / 1000.0
                    out["Vybití bat. (MWh)"] = _sum_col(df, "batt_discharge_kwh") / 1000.0
                    out["Sdílení přes bat. (MWh)"] = _sum_col(df, "batt_discharge_shared_kwh") / 1000.0
                    out["Lokální posun (MWh)"] = _sum_col(df, "batt_discharge_own_kwh") / 1000.0
                    return out

                def _folder_for(name: str) -> Path:
                    return snap_root / name

                a_folder = _folder_for(a_name)
                a_meta = _read_meta(a_folder)
                a_k = _snapshot_kpis(a_folder)

                st.subheader("Varianta A – přehled")
                if a_meta:
                    with st.expander("Meta (A)", expanded=False):
                        st.json(a_meta)
                st.dataframe(pd.DataFrame([a_k]).set_index("_source"), use_container_width=True)

                if compare and b_name:
                    b_folder = _folder_for(b_name)
                    b_meta = _read_meta(b_folder)
                    b_k = _snapshot_kpis(b_folder)

                    st.subheader("Varianta B – přehled")
                    if b_meta:
                        with st.expander("Meta (B)", expanded=False):
                            st.json(b_meta)
                    st.dataframe(pd.DataFrame([b_k]).set_index("_source"), use_container_width=True)

                    # diff
                    st.subheader("Rozdíl (B − A)")
                    diff = {}
                    for k in ["Import (MWh)","Export (MWh)","Curtailment (MWh)","Sdílení (MWh)","Nabití bat. (MWh)","Vybití bat. (MWh)","Sdílení přes bat. (MWh)","Lokální posun (MWh)"]:
                        av = a_k.get(k, float("nan"))
                        bv = b_k.get(k, float("nan"))
                        if pd.isna(av) or pd.isna(bv):
                            diff[k] = float("nan")
                        else:
                            diff[k] = bv - av
                    st.dataframe(pd.DataFrame([diff]), use_container_width=True)

                if show_files:
                    st.subheader("Soubory ve variantě A")
                    files_a = sorted([f.name for f in a_folder.iterdir() if f.is_file()])
                    st.write(files_a)
                    if compare and b_name:
                        st.subheader("Soubory ve variantě B")
                        files_b = sorted([f.name for f in (_folder_for(b_name)).iterdir() if f.is_file()])
                        st.write(files_b)

                st.divider()
                st.caption("Tip: pokud chceš snapshot použít jako aktuální data, můžeš ho zkopírovat do tohoto běhu.")
                act_cols = st.columns([1,1,2])
                with act_cols[0]:
                    if st.button("📥 Aktivovat variantu A", use_container_width=True):
                        # Copy all files from snapshot folder into run csv_dir (and keep in _battery_snapshots)
                        src = a_folder
                        dst = rp.csv_dir
                        pick = [f.name for f in src.iterdir() if f.is_file() and f.suffix.lower() in {".csv",".json"}]
                        n, miss = _copy_selected_files(src, dst, pick)
                        st.success(f"Zkopírováno {n} souborů do csv/ (chybělo {miss}).")
                        st.rerun()
                with act_cols[1]:
                    if compare and b_name and st.button("📥 Aktivovat variantu B", use_container_width=True):
                        src = _folder_for(b_name)
                        dst = rp.csv_dir
                        pick = [f.name for f in src.iterdir() if f.is_file() and f.suffix.lower() in {".csv",".json"}]
                        n, miss = _copy_selected_files(src, dst, pick)
                        st.success(f"Zkopírováno {n} souborů do csv/ (chybělo {miss}).")
                        st.rerun()