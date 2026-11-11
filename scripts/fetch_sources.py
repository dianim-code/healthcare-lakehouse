# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.27"]
# ///
"""Download the raw public sources into data/raw/ (git-ignored).

Idempotent: a file that already exists is skipped unless --force is given.
Every run rewrites data/raw/manifest.json with URL, size, SHA-256 and download time,
so the exact inputs of a build can be traced.

Usage:
    uv run scripts/fetch_sources.py            # download what is missing
    uv run scripts/fetch_sources.py --force    # download everything again
    uv run scripts/fetch_sources.py --only beds_2023 population

Sources and licenses are documented in docs/data-sources.md.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"

MINISTRY = "https://www.dati.salute.gov.it/sites/default/files"

# ISTAT SDMX REST: dataflow 22_289 "Resident population on 1st January".
# Key = FREQ.REF_AREA.DATA_TYPE.SEX.AGE.MARITAL_STATUS -> annual, 21 areas, 1 Jan, both sexes,
# all ages, any marital status. Area codes come from the contract's region reference table.
REGIONS_SEED = REPO_ROOT / "docs" / "contract" / "reference" / "regions.csv"
with REGIONS_SEED.open(encoding="utf-8", newline="") as _fh:
    ISTAT_AREAS = "+".join(row["istat_area_code"] for row in csv.DictReader(_fh))
ISTAT_POPULATION_URL = (
    f"https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.0/A.{ISTAT_AREAS}.JAN.9.TOTAL.99/?startPeriod=2019"
)


@dataclass(frozen=True)
class Source:
    name: str
    url: str
    filename: str
    headers: dict[str, str] | None = None
    # "curl": the Ministry portal resets TLS handshakes coming from Python's OpenSSL stack,
    # while it serves the same files to the system curl. See docs/decisions.md (ADR-006).
    transport: str = "httpx"


SOURCES: list[Source] = [
    # Ministry of Health - "Posti letto per stabilimento ospedaliero e disciplina".
    # One file per publication; the schema is identical across all of them (23 columns).
    Source(
        "beds_2010_2019", f"{MINISTRY}/imported/C_17_dataset_96_0_upFile.csv", "beds_2010_2019.csv", transport="curl"
    ),
    Source("beds_2020", f"{MINISTRY}/imported/C_17_dataset_115_0_upFile.csv", "beds_2020.csv", transport="curl"),
    Source("beds_2021", f"{MINISTRY}/imported/C_17_dataset_153_0_upFile.csv", "beds_2021.csv", transport="curl"),
    Source(
        "beds_2022",
        f"{MINISTRY}/2024-12/Posti%20letto%20per%20stabilimento%20ospedaliero%20e%20disciplina_2022.csv",
        "beds_2022.csv",
        transport="curl",
    ),
    Source(
        "beds_2023",
        f"{MINISTRY}/2025-07/Posti%20letto%20per%20stabilimento%20ospedaliero%20e%20disciplina_2023_0.csv",
        "beds_2023.csv",
        transport="curl",
    ),
    Source(
        "population",
        ISTAT_POPULATION_URL,
        "population_istat.csv",
        headers={"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"},
    ),
]

USER_AGENT = "open-data-fetch/1.0"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_with_curl(source: Source, tmp: Path) -> None:
    curl = shutil.which("curl")
    if curl is None:
        raise RuntimeError("curl not found on PATH (needed for the Ministry of Health sources)")
    result = subprocess.run(
        [curl, "--fail", "--silent", "--show-error", "--location", "--retry", "3",
         "--user-agent", USER_AGENT, "--output", str(tmp), "--write-out", "%{content_type}", source.url],
        capture_output=True, text=True, check=False,
    )  # fmt: skip
    if result.returncode != 0:
        raise RuntimeError(f"{source.name}: curl failed ({result.stderr.strip()})")
    if "text/html" in result.stdout:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"{source.name}: got an HTML page instead of data ({source.url})")


def download(client: httpx.Client, source: Source, target: Path) -> None:
    tmp = target.with_suffix(target.suffix + ".part")
    if source.transport == "curl":
        download_with_curl(source, tmp)
        tmp.replace(target)
        return
    with client.stream("GET", source.url, headers=source.headers) as response:
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type:
            # The Ministry portal answers some missing files with an HTML page.
            raise RuntimeError(f"{source.name}: got an HTML page instead of data ({source.url})")
        with tmp.open("wb") as fh:
            for chunk in response.iter_bytes():
                fh.write(chunk)
    tmp.replace(target)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--force", action="store_true", help="download again even if the file exists")
    parser.add_argument("--only", nargs="*", metavar="NAME", help="download only these sources")
    args = parser.parse_args(argv)

    selected = [s for s in SOURCES if not args.only or s.name in args.only]
    unknown = set(args.only or []) - {s.name for s in SOURCES}
    if unknown:
        parser.error(f"unknown source(s): {', '.join(sorted(unknown))}")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = RAW_DIR / "manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8")) if manifest_path.exists() else {}

    # ISTAT answers slowly and rate-limits aggressively: keep a long timeout.
    timeout = httpx.Timeout(30.0, read=180.0)
    with httpx.Client(timeout=timeout, follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
        for source in selected:
            target = RAW_DIR / source.filename
            if target.exists() and not args.force:
                print(f"skip      {source.name:<16} {target.relative_to(REPO_ROOT)} (exists)")
            else:
                print(f"download  {source.name:<16} {source.url}")
                download(client, source, target)
                manifest[source.name] = {"downloaded_at": datetime.now(UTC).isoformat(timespec="seconds")}
            manifest.setdefault(source.name, {})
            manifest[source.name].update(
                {
                    "url": source.url,
                    "file": source.filename,
                    "bytes": target.stat().st_size,
                    "sha256": sha256(target),
                }
            )

    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", "utf-8")
    print(f"manifest  {manifest_path.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
