# SPDX-License-Identifier: AGPL-3.0-or-later
from __future__ import annotations
from pathlib import Path
from typing import List

def load_yaml(path: str | None) -> dict:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        print(f"[i] config: soubor nenalezen: {p}")
        return {}
    try:
        import yaml  # type: ignore
    except Exception:
        print("[i] config: PyYAML není nainstalováno (pip install PyYAML)")
        return {}
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        print("[i] config: YAML není slovník – ignoruji.")
        return {}
    return data

def _flat_kv(d: dict | None) -> List[tuple[str, str]]:
    out: List[tuple[str, str]] = []
    if not isinstance(d, dict):
        return out
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, bool):
            v = "true" if v else "false"
        elif isinstance(v, (list, tuple)):
            v = ",".join(map(str, v))
        else:
            v = str(v)
        out.append((str(k), v))
    return out

def kv_to_argv(d_global: dict | None, d_step: dict | None) -> List[str]:
    argv: List[str] = []
    for k, v in _flat_kv(d_global):
        argv += [f"--{k}", v]
    for k, v in _flat_kv(d_step):
        argv += [f"--{k}", v]
    return argv
