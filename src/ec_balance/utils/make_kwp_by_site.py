# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2025 Kuba

import argparse
from pathlib import Path
import re
import pandas as pd

def _detect_sep(path: Path) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            s = f.readline() + f.readline()
    except Exception:
        return ","
    return ";" if s.count(";") > s.count(",") else ","

def _flatten_multiindex_cols(cols) -> list[str]:
    import pandas as pd
    if not isinstance(cols, pd.MultiIndex):
        return [str(c) for c in cols]
    return [" / ".join([str(x) for x in tup if str(x) != "nan"]) for tup in cols]

def _extract_kwp_from_headers(headers: list[str]) -> pd.DataFrame:
    cols = [c for c in headers if c.lower() not in ("datetime", "time", "ts", "timestamp")]
    rows = []
    for c in cols:
        m = re.search(r"kwp\s*[=:]\s*([0-9]+(?:\.[0-9]+)?)", str(c), flags=re.IGNORECASE)
        kwp = float(m.group(1)) if m else 0.0
        rows.append({"site": str(c), "kwp": kwp})
    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser(description="Vytvoř mapu kWp podle HLAVIČEK wide CSV (výroba D).")
    ap.add_argument("--eand_wide", required=True, help="Cesta k wide CSV (výroba).")
    ap.add_argument("--out", required=True, help="Kam zapsat CSV s mapou kWp.")
    ap.add_argument("--sep", default="", help="Oddělovač (',' nebo ';'). Když necháš prázdné, detekuju.")
    ap.add_argument("--header_rows", type=int, default=1, help="Počet řádků hlavičky (1 = běžná, >1 = multiheader).")
    ap.add_argument("--encoding", default="utf-8", help="Kódování vstupu.")
    args = ap.parse_args()

    src = Path(args.eand_wide)
    if not src.exists():
        raise FileNotFoundError(f"Nenalezen soubor: {src}")

    sep = args.sep or _detect_sep(src)
    header = list(range(args.header_rows)) if args.header_rows and args.header_rows > 1 else 0

    # Stačí přečíst jen hlavičky (nrows=0)
    df = pd.read_csv(src, sep=sep, header=header, nrows=0, encoding=args.encoding)
    headers = _flatten_multiindex_cols(df.columns)

    # Pokud je první sloupec čas, vynecháme ho
    if headers:
        maybe_time = headers[0].lower()
        if any(k in maybe_time for k in ("time", "date", "datetime", "timestamp", "ts")):
            headers = headers[1:]

    out = _extract_kwp_from_headers(headers)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"[OK] kwp_by_site → {args.out} (N={len(out)})  sep='{sep}'  header_rows={args.header_rows}")

if __name__ == "__main__":
    main()
