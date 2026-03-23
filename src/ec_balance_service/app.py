from __future__ import annotations

from fastapi import FastAPI, APIRouter, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
import contextlib, io, sys, importlib, shutil
import pandas as pd

app = FastAPI(title="ec-balance service", version="0.1")

# --- CORS (lokĂˇlnĂ­ vĂ˝voj + GitHub Pages hostname/y) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "https://dpuenergy.github.io",
        "https://dpuenergy.github.io/energeticka-komunita-bilance",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- cesty pro vstupy/vĂ˝stupy ---
BASE = Path(__file__).resolve().parent.parent.parent  # projekt root
OUT_DIR = (BASE / "out")
if not OUT_DIR.exists():
    OUT_DIR = BASE / "_ci_out"
CSV_DIR = OUT_DIR / "csv"
CSV_DIR.mkdir(parents=True, exist_ok=True)

UPLOAD_DIR = BASE / "data" / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# statickĂ© soubory (aĹĄ jde stahovat pĹ™es /files/â€¦)
app.mount("/files", StaticFiles(directory=str(OUT_DIR)), name="files")

# --- jednoduchĂ© info endpointy ---
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    try:
        import ec_balance as pkg  # type: ignore
        return {"service": app.version, "ec_balance": getattr(pkg, "__version__", "unknown")}
    except Exception:
        return {"service": app.version, "ec_balance": "unknown"}

api = APIRouter(prefix="/api")

# --- util: uloĹľenĂ­ uploadu (CSV nebo XLSX->CSV) ---
def _save_upload_to_csv(dest_dir: Path, desired_name: str, up: UploadFile) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    ext = (up.filename or "").split(".")[-1].lower()
    out_path = dest_dir / desired_name
    if ext in ("xlsx", "xls"):
        df = pd.read_excel(up.file, sheet_name=0)
        df.to_csv(out_path, index=False)
    else:
        with open(out_path, "wb") as f:
            shutil.copyfileobj(up.file, f)
    return out_path

# --- vĂ˝pis/stahovĂˇnĂ­ vĂ˝stupĹŻ ---
@api.get("/outputs")
def list_outputs():
    return {"root": OUT_DIR.name, "csv": sorted(p.name for p in CSV_DIR.glob("*.csv"))}

@api.get("/outputs/{name}")
def get_output_file(name: str):
    p = CSV_DIR / name
    if not p.exists():
        return {"error": f"{name} not found"}
    return FileResponse(str(p), media_type="text/csv", filename=name)

# --- upload vstupĹŻ (klĂ­ÄŤ = nĂˇzev parametru, napĹ™. eano_after_pv_csv) ---
@api.post("/upload")
async def upload_input(key: str = Form(...), file: UploadFile = File(...)):
    safe_key = key.strip().replace("/", "_").replace("\\", "_")
    desired_name = f"{safe_key}.csv"
    saved = _save_upload_to_csv(UPLOAD_DIR, desired_name, file)
    return {"ok": True, "key": safe_key, "path": str(saved)}

# --- mapovĂˇnĂ­ krok -> modul ---
_STEP_TO_MODULE = {
    "step3":  "ec_balance.pipeline.step3_sharing",
    "step4a": "ec_balance.pipeline.step4a_batt_local_byhour",
    "step4b-econ": "ec_balance.pipeline.step4b_batt_econ",
    "step5a": "ec_balance.pipeline.step5a_batt_central_byhour",
    "step5":  "ec_balance.pipeline.step5_batt_central",
    "step6":  "ec_balance.pipeline.step6_excel_scenarios",
}

def _kv_to_argv(d: dict | None) -> list[str]:
    argv: list[str] = []
    for k, v in (d or {}).items():
        if v is None:
            continue
        k = str(k).replace("_", "-")
        if isinstance(v, bool):
            v = "true" if v else "false"
        argv += [f"--{k}", str(v)]
    return argv

def _run_step(step: str, args: dict) -> dict:
    if step not in _STEP_TO_MODULE:
        return {"ok": False, "error": f"Unknown step: {step}"}
    module_path = _STEP_TO_MODULE[step]
    mod = importlib.import_module(module_path)

    before = set(p.name for p in CSV_DIR.glob("*.csv"))
    buf = io.StringIO()
    rc = 0
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        old_argv = sys.argv[:]
        try:
            sys.argv = ["ecb"] + _kv_to_argv(args)
            mod.main()
        except SystemExit as e:
            rc = int(getattr(e, "code", 1) or 0)
        except Exception as e:
            rc = 1
            print(f"[ERROR] {type(e).__name__}: {e}")
        finally:
            sys.argv = old_argv

    after = set(p.name for p in CSV_DIR.glob("*.csv"))
    new_files = sorted(list(after - before))
    return {"ok": rc == 0, "return_code": rc, "log": buf.getvalue(), "new_csv": new_files}

@api.post("/run/{step}")
async def run_step(step: str, args: dict):
    # Body JSON = map CLI parametrĹŻ (viz README/CLI)
    return _run_step(step, args or {})

# --- jednoduchĂ© summary pro step3 (ukĂˇzka) ---
@api.get("/summary/step3")
def summary_step3():
    by_hour = CSV_DIR / "by_hour_after.csv"
    if not by_hour.exists():
        return {"ok": False, "error": "by_hour_after.csv not found"}
    df = pd.read_csv(by_hour)
    cols = df.columns.str.lower()
    def _sum(col_like: str) -> float:
        idx = [i for i, c in enumerate(cols) if col_like in c]
        return float(df.iloc[:, idx[0]].sum()) if idx else 0.0
    return {
        "ok": True,
        "rows": int(len(df)),
        "sum_import_kwh": _sum("import"),
        "sum_export_kwh": _sum("export"),
        "note": "orientaÄŤnĂ­ metrika; nĂˇzvy sloupcĹŻ pĹ™Ă­padnÄ› doladĂ­me",
    }

app.include_router(api)

app.mount('/ui', StaticFiles(directory='webui', html=True), name='ui')
