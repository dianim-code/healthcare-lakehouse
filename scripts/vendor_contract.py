# /// script
# requires-python = ">=3.12"
# dependencies = ["pyyaml>=6.0"]
# ///
"""Copy the gold contract and the gold sample into a consumer repository.

This repository owns the contract. Consumers (the semantic model and the Deneb gallery) keep a
vendored, read-only copy so they build without this repo or a Databricks workspace:

    <consumer>/contract/gold_schema.yml
    <consumer>/contract/reference/regions.csv
    <consumer>/contract/VENDORED.md          # version, date, checksums, attribution
    <consumer>/sample-data/gold/*.csv

Usage:
    uv run scripts/vendor_contract.py --to ../healthcare-semantic-model
    uv run scripts/vendor_contract.py --to ../deneb-gallery --check   # exit 1 if the copy is stale
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
from datetime import date
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT = REPO_ROOT / "docs" / "contract" / "gold_schema.yml"
REGIONS = REPO_ROOT / "docs" / "contract" / "reference" / "regions.csv"
SAMPLE = REPO_ROOT / "sample-data" / "gold"


def payload(consumer: Path) -> dict[Path, Path]:
    """Map source file -> destination file."""
    files = {
        CONTRACT: consumer / "contract" / "gold_schema.yml",
        REGIONS: consumer / "contract" / "reference" / "regions.csv",
    }
    files.update({csv: consumer / "sample-data" / "gold" / csv.name for csv in sorted(SAMPLE.glob("*.csv"))})
    return files


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def vendored_md(version: str, files: dict[Path, Path], consumer: Path) -> str:
    rows = "\n".join(
        f"| `{dst.relative_to(consumer).as_posix()}` | `{sha256(src)[:16]}` |" for src, dst in files.items()
    )
    return f"""# Vendored gold contract

**Do not edit these files here.** They are a read-only copy of the gold data contract owned by
the `healthcare-lakehouse` repository (`docs/contract/` and `sample-data/gold/`). To change the
contract, change it there, bump `contract_version`, then re-vendor:

```bash
# from healthcare-lakehouse/
uv run scripts/vendor_contract.py --to <path-to-this-repo>
```

- **Contract version:** {version}
- **Vendored on:** {date.today().isoformat()}
- **Human-readable contract:** `docs/data-contract.md` in `healthcare-lakehouse`

| File | SHA-256 (first 16) |
|---|---|
{rows}

## Data attribution

The CSVs in `sample-data/gold/` are derived from data published by the **Ministero della Salute**
(Italian Ministry of Health) under the [Italian Open Data License v2.0](https://www.dati.gov.it/iodl/2.0/)
and by **ISTAT** under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The data has been
cleaned, re-keyed and aggregated; neither publisher endorses this project. Full source list:
`docs/data-sources.md` in `healthcare-lakehouse`.
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--to", required=True, type=Path, help="root folder of the consumer repository")
    parser.add_argument("--check", action="store_true", help="only report whether the vendored copy is up to date")
    args = parser.parse_args(argv)

    consumer = args.to.resolve()
    if not consumer.is_dir():
        parser.error(f"{consumer} is not a directory")
    if consumer == REPO_ROOT:
        parser.error("--to must point to a consumer repository, not to this one")

    version = yaml.safe_load(CONTRACT.read_text("utf-8"))["contract_version"]
    files = payload(consumer)

    stale = [dst for src, dst in files.items() if not dst.exists() or sha256(src) != sha256(dst)]
    expected_csvs = {dst.name for dst in files.values() if dst.parent.name == "gold"}
    target_gold = consumer / "sample-data" / "gold"
    extra = sorted(p for p in target_gold.glob("*.csv") if p.name not in expected_csvs) if target_gold.exists() else []

    if args.check:
        for path in stale:
            print(f"stale   {path.relative_to(consumer).as_posix()}")
        for path in extra:
            print(f"extra   {path.relative_to(consumer).as_posix()}")
        print("up to date" if not (stale or extra) else f"contract {version}: vendored copy is out of date")
        return 1 if stale or extra else 0

    for src, dst in files.items():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
    for path in extra:
        path.unlink()
    (consumer / "contract" / "VENDORED.md").write_text(vendored_md(version, files, consumer), "utf-8", newline="\n")
    print(f"vendored contract {version} ({len(files)} files) -> {consumer}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
