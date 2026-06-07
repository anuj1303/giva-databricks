#!/usr/bin/env python3
"""
build_wheel.py — build a py3-none-any wheel for `giva` WITHOUT setuptools.

The package is pure-Python + data files, so a wheel is just a zip of the
`giva/` tree plus a PEP-427 .dist-info directory. Use when the normal
`python -m build` path is unavailable (offline / no setuptools).

    python scripts/build_wheel.py   ->   dist/giva-<ver>-py3-none-any.whl
"""
import base64, csv, hashlib, io, re, zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "giva"
DIST = ROOT / "dist"; DIST.mkdir(exist_ok=True)

# read version + metadata from pyproject (simple regex; no toml dep needed)
pp = (ROOT / "pyproject.toml").read_text()
def g(key, default=""):
    m = re.search(rf'^{key}\s*=\s*"([^"]*)"', pp, re.M)
    return m.group(1) if m else default
NAME, VER, DESC = "giva-databricks", g("version"), g("description")
DIST_FN = NAME.replace("-", "_")  # PEP 427 escaped name for filename + dist-info
REQUIRES = re.findall(r'"([^"]+)"', re.search(r'dependencies\s*=\s*\[(.*?)\]', pp, re.S).group(1))

distinfo = f"{DIST_FN}-{VER}.dist-info"
records = []


def _b64(data):
    return base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")


def add(zf, arcname, data: bytes):
    zf.writestr(arcname, data)
    records.append((arcname, f"sha256={_b64(data)}", str(len(data))))


whl = DIST / f"{DIST_FN}-{VER}-py3-none-any.whl"
with zipfile.ZipFile(whl, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
    # 1) package tree
    for p in sorted(PKG.rglob("*")):
        if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc":
            add(zf, str(p.relative_to(ROOT)).replace("\\", "/"), p.read_bytes())

    # 2) METADATA
    md = [f"Metadata-Version: 2.1", f"Name: {NAME}", f"Version: {VER}",
          f"Summary: {DESC}", "Requires-Python: >=3.9",
          "License-Expression: MIT"]
    for r in REQUIRES:
        md.append(f"Requires-Dist: {r}")
    md += ["", (ROOT / "README.md").read_text()]
    add(zf, f"{distinfo}/METADATA", "\n".join(md).encode())

    # 3) WHEEL
    add(zf, f"{distinfo}/WHEEL",
        b"Wheel-Version: 1.0\nGenerator: giva-build_wheel\nRoot-Is-Purelib: true\nTag: py3-none-any\n")

    # 4) top_level.txt
    add(zf, f"{distinfo}/top_level.txt", b"giva\n")

    # 5) RECORD (lists itself with no hash)
    rec = io.StringIO()
    w = csv.writer(rec, lineterminator="\n")
    for row in records:
        w.writerow(row)
    w.writerow([f"{distinfo}/RECORD", "", ""])
    zf.writestr(f"{distinfo}/RECORD", rec.getvalue())

print(f"built {whl.relative_to(ROOT)}  ({whl.stat().st_size/1024/1024:.1f} MB, {len(records)+1} files)")
