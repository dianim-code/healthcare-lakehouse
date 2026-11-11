"""The packaged region seed must hold the same rows as the contract's copy in docs/contract/reference/."""

from __future__ import annotations

import csv
from importlib import resources

from support import REPO_ROOT

from healthcare_lakehouse.reference import region_rows


def _rows(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(text.splitlines()))


def test_packaged_region_seed_equals_the_contract_copy() -> None:
    """Same rows and columns. Line endings may differ: the contract copy is CRLF, the package copy LF."""
    contract_copy = (REPO_ROOT / "docs" / "contract" / "reference" / "regions.csv").read_text(encoding="utf-8")
    packaged = resources.files("healthcare_lakehouse").joinpath("reference/regions.csv").read_text(encoding="utf-8")
    assert _rows(packaged) == _rows(contract_copy), "regenerate src/healthcare_lakehouse/reference/regions.csv"


def test_region_seed_has_21_unique_regions() -> None:
    rows = region_rows()
    assert len(rows) == 21
    assert len({r[0] for r in rows}) == 21
    assert len({r[3] for r in rows}) == 21
    # ISTAT counts 20 regions: the two autonomous provinces share Trentino-Alto Adige.
    istat = {r[0]: r[2] for r in rows}
    assert len(set(istat.values())) == 20
    assert istat["041"] == istat["042"] == "Trentino-Alto Adige"
