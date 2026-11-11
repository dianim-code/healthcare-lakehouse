# /// script
# requires-python = ">=3.12"
# dependencies = ["pandas>=2.2", "pyyaml>=6.0"]
# ///
"""Build the gold sample (sample-data/gold/*.csv) from data/raw/ and validate it against the contract.

This is a plain-pandas reference implementation of the gold rules in docs/data-contract.md.
The Databricks lakehouse implements the same rules in PySpark; this script exists so that the
semantic model and the Deneb gallery have contract-exact data without a Databricks workspace.

Usage:
    uv run scripts/fetch_sources.py
    uv run scripts/build_sample.py                       # default window 2021-2023
    uv run scripts/build_sample.py --from-year 2019 --to-year 2023
    uv run scripts/build_sample.py --check-only          # validate the committed CSVs
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
OUT_DIR = REPO_ROOT / "sample-data" / "gold"
CONTRACT = REPO_ROOT / "docs" / "contract" / "gold_schema.yml"
REGIONS_SEED = REPO_ROOT / "docs" / "contract" / "reference" / "regions.csv"

# Source column (Ministry CSV header, after trimming) -> staging name.
BEDS_COLUMNS = {
    "Anno": "year",
    "Codice Regione": "region_code",
    "Descrizione Regione": "region_name_src",
    "Codice Azienda": "authority_code",
    "Tipo Azienda": "authority_type_code",
    "Codice struttura": "structure_code",
    "Subcodice": "site_subcode",
    "Denominazione Struttura/Stabilimento": "facility_name",
    "Indirizzo": "address",
    "Codice Comune": "municipality_code",
    "Comune": "municipality_name",
    "Sigla Provincia": "province_code",
    "Codice tipo struttura": "facility_type_code",
    "Descrizione tipo struttura": "facility_type_name",
    "Codice disciplina": "discipline_code",
    "Descrizione disciplina": "discipline_name",
    "Tipo di Disciplina": "discipline_type",
    "N° Reparti": "ward_count",
    "Posti letto degenza ordinaria": "beds_ordinary",
    "Posti letto degenza a pagamento": "beds_paying",
    "Posti letto Day Hospital": "beds_day_hospital",
    "Posti letto Day Surgery": "beds_day_surgery",
    "Totale posti letto": "beds_total",
}
COUNT_COLUMNS = ["ward_count", "beds_ordinary", "beds_paying", "beds_day_hospital", "beds_day_surgery", "beds_total"]
# Labels verbatim from the Ministry data dictionary (v2.0, field TIPO AZIENDA).
AUTHORITY_TYPES = {"1": "Azienda sanitaria locale (ASL)", "2": "Struttura sanitaria aziendale di tipo ospedaliero"}
FACILITY_ATTRIBUTES = [
    "structure_code", "site_subcode", "facility_name", "address", "municipality_code",
    "municipality_name", "province_code", "region_code", "authority_code",
    "authority_type_code", "facility_type_code", "facility_type_name",
]  # fmt: skip


# --------------------------------------------------------------------------- cleaning rules


def clean_text(series: pd.Series) -> pd.Series:
    """Contract rule C1, step by step so that Spark can reproduce it exactly.

    1. U+00A0 (no-break space) -> space; backtick -> apostrophe.
    2. Runs of [space, tab, CR, LF] -> one space. An explicit ASCII set, not Python's Unicode-aware
       \\s, because Spark's Java regex \\s and trim() do not match U+00A0.
    3. Trim leading and trailing spaces. 4. Empty string -> null.
    """
    cleaned = (
        series.str.replace("\u00a0", " ", regex=False)
        .str.replace("`", "'", regex=False)
        .str.replace(r"[ \t\r\n]+", " ", regex=True)
        .str.replace(r"^ | $", "", regex=True)
    )
    return cleaned.mask(cleaned == "")


def beds_files_for(years: range) -> list[Path]:
    files = []
    if any(y <= 2019 for y in years):
        files.append(RAW_DIR / "beds_2010_2019.csv")
    files += [RAW_DIR / f"beds_{y}.csv" for y in years if y >= 2020]
    missing = [f for f in files if not f.exists()]
    if missing:
        sys.exit(f"Missing raw files: {[str(m.relative_to(REPO_ROOT)) for m in missing]}. Run fetch_sources.py first.")
    return files


def read_beds(years: range) -> pd.DataFrame:
    frames = []
    for path in beds_files_for(years):
        # The Ministry files are ISO-8859-1, ';'-separated, with fixed-width padded text fields.
        raw = pd.read_csv(path, sep=";", encoding="latin-1", dtype=str, keep_default_na=False)
        raw.columns = [c.strip() for c in raw.columns]
        if set(raw.columns) != set(BEDS_COLUMNS):
            sys.exit(f"{path.name}: unexpected columns {sorted(set(raw.columns) ^ set(BEDS_COLUMNS))}")
        frames.append(raw.rename(columns=BEDS_COLUMNS))
    df = pd.concat(frames, ignore_index=True)
    for col in df.columns:
        df[col] = clean_text(df[col])
    df["year"] = df["year"].astype(int)
    df = df[df["year"].isin(years)].copy()
    for col in COUNT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="raise").astype("int64")
    # Rule C2: facility business key.
    df["facility_id"] = df["structure_code"].where(
        df["site_subcode"].isna(), df["structure_code"] + "-" + df["site_subcode"]
    )
    return df


def read_population(years: range, regions: pd.DataFrame) -> pd.DataFrame:
    path = RAW_DIR / "population_istat.csv"
    if not path.exists():
        sys.exit("Missing data/raw/population_istat.csv. Run fetch_sources.py first.")
    raw = pd.read_csv(path, dtype=str)
    pop = raw[["REF_AREA", "TIME_PERIOD", "OBS_VALUE"]].rename(
        columns={"REF_AREA": "istat_area_code", "TIME_PERIOD": "year", "OBS_VALUE": "population"}
    )
    pop["year"] = pop["year"].astype(int)
    pop["population"] = pop["population"].astype("int64")
    pop = pop[pop["year"].isin(years)].merge(regions[["istat_area_code", "region_code"]], how="left")
    if pop["region_code"].isna().any():
        sys.exit(f"Unmapped ISTAT areas: {sorted(pop.loc[pop.region_code.isna(), 'istat_area_code'].unique())}")
    return pop[["year", "region_code", "population"]]


# --------------------------------------------------------------------------- gold tables


def build_gold(years: range) -> dict[str, pd.DataFrame]:
    regions = pd.read_csv(REGIONS_SEED, dtype=str)
    regions["is_autonomous_province"] = regions["is_autonomous_province"].map({"true": True, "false": False})
    regions["sort_order"] = regions["sort_order"].astype(int)

    beds = read_beds(years)
    unknown_regions = set(beds["region_code"]) - set(regions["region_code"])
    if unknown_regions:
        sys.exit(f"Region codes not in the reference seed: {sorted(unknown_regions)}")

    latest = max(years)
    dim_year = pd.DataFrame({"year": list(years)})
    dim_year["reference_date"] = pd.to_datetime(dim_year["year"].astype(str) + "-01-01").dt.date
    dim_year["is_latest_year"] = dim_year["year"] == latest

    # Rule C3 (SCD type 1): attributes come from the latest year the entity appears in, taken as a
    # whole row. (groupby().last() would pick the last non-null value per column and mix years.)
    disc = beds.sort_values(["year", "facility_id"]).drop_duplicates("discipline_code", keep="last")
    dim_discipline = pd.DataFrame(
        {
            "discipline_code": disc["discipline_code"],
            "discipline_name": disc["discipline_name"],
            "discipline_type": disc["discipline_type"].str.capitalize(),
        }
    ).sort_values("discipline_code")

    span = beds.groupby("facility_id")["year"].agg(first_year="min", last_year="max").reset_index()
    fac = (
        beds.sort_values(["year", "discipline_code"])
        .drop_duplicates("facility_id", keep="last")[["facility_id", *FACILITY_ATTRIBUTES]]
        .merge(span, on="facility_id")
        .sort_values("facility_id")
        .reset_index(drop=True)
    )
    # Rule C4: surrogate key = position of facility_id in ascending order.
    fac["facility_key"] = range(1, len(fac) + 1)
    fac["health_authority_id"] = fac["region_code"] + fac["authority_code"]
    fac["health_authority_type"] = fac["authority_type_code"].map(AUTHORITY_TYPES)
    dim_facility = fac

    fact = beds.merge(fac[["facility_id", "facility_key"]], on="facility_id")
    fact_hospital_beds = fact.sort_values(["year", "facility_key", "discipline_code"])

    fact_population = read_population(years, regions).sort_values(["year", "region_code"])

    return {
        "dim_year": dim_year,
        "dim_region": regions,
        "dim_discipline": dim_discipline,
        "dim_facility": dim_facility,
        "fact_hospital_beds": fact_hospital_beds,
        "fact_population": fact_population,
    }


# --------------------------------------------------------------------------- contract


def load_contract() -> dict:
    return yaml.safe_load(CONTRACT.read_text("utf-8"))


def conform(tables: dict[str, pd.DataFrame], contract: dict) -> dict[str, pd.DataFrame]:
    """Select and order columns exactly as the contract lists them."""
    out = {}
    for name, spec in contract["tables"].items():
        cols = [c["name"] for c in spec["columns"]]
        missing = set(cols) - set(tables[name].columns)
        if missing:
            sys.exit(f"{name}: builder does not produce {sorted(missing)}")
        out[name] = tables[name][cols].reset_index(drop=True)
    return out


TYPE_CHECKS = {
    "string": lambda s: s.dropna().map(lambda v: isinstance(v, str)).all(),
    "int": lambda s: pd.api.types.is_integer_dtype(s) and s.between(-(2**31), 2**31 - 1).all(),
    "bigint": lambda s: pd.api.types.is_integer_dtype(s),
    "boolean": lambda s: pd.api.types.is_bool_dtype(s),
    "date": lambda s: s.dropna().map(lambda v: hasattr(v, "isoformat") and len(v.isoformat()) == 10).all(),
}


def validate(tables: dict[str, pd.DataFrame], contract: dict) -> list[str]:
    errors: list[str] = []
    for name, spec in contract["tables"].items():
        df = tables[name]
        cols = [c["name"] for c in spec["columns"]]
        if list(df.columns) != cols:
            errors.append(f"{name}: columns {list(df.columns)} != contract {cols}")
            continue
        if df.empty:
            errors.append(f"{name}: no rows")
        for col in spec["columns"]:
            s = df[col["name"]]
            where = f"{name}.{col['name']}"
            if not TYPE_CHECKS[col["type"]](s):
                errors.append(f"{where}: values do not fit type {col['type']}")
            if not col["nullable"] and s.isna().any():
                errors.append(f"{where}: {int(s.isna().sum())} nulls in a non-nullable column")
            if "allowed_values" in col and not s.dropna().isin(col["allowed_values"]).all():
                errors.append(f"{where}: unexpected values {sorted(set(s.dropna()) - set(col['allowed_values']))}")
            if "min" in col and (s.dropna() < col["min"]).any():
                errors.append(f"{where}: values below {col['min']}")
            if "references" in col:
                ref_table, ref_col = col["references"].split(".")
                orphans = set(s.dropna()) - set(tables[ref_table][ref_col])
                if orphans:
                    errors.append(f"{where}: {len(orphans)} values missing in {col['references']}")
        if df.duplicated(spec["primary_key"]).any():
            errors.append(f"{name}: duplicate primary key {spec['primary_key']}")
        for unique in spec.get("unique", []):
            if df.duplicated(unique).any():
                errors.append(f"{name}: duplicate values in unique key {unique}")

    beds = tables["fact_hospital_beds"]
    regimes = beds[["beds_ordinary", "beds_paying", "beds_day_hospital", "beds_day_surgery"]].sum(axis=1)
    if (regimes != beds["beds_total"]).any():
        errors.append("fact_hospital_beds: beds_total differs from the sum of regimes")
    pop = tables["fact_population"]
    expected = len(tables["dim_year"]) * len(tables["dim_region"])
    if len(pop) != expected:
        errors.append(f"fact_population: {len(pop)} rows, expected {expected} (every year x region)")
    return errors


# --------------------------------------------------------------------------- csv io


def write_csv(df: pd.DataFrame, path: Path) -> None:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_bool_dtype(out[col]):
            out[col] = out[col].map({True: "true", False: "false"})
    out.to_csv(path, index=False, encoding="utf-8", lineterminator="\n")


def read_committed(contract: dict) -> dict[str, pd.DataFrame]:
    tables = {}
    for name, spec in contract["tables"].items():
        df = pd.read_csv(OUT_DIR / f"{name}.csv", dtype=str, keep_default_na=False)
        for col in spec["columns"]:
            s = df[col["name"]].mask(df[col["name"]] == "")
            match col["type"]:
                case "int" | "bigint":
                    s = s.astype("int64")
                case "boolean":
                    s = s.map({"true": True, "false": False}).astype(bool)
                case "date":
                    s = pd.to_datetime(s).dt.date
            df[col["name"]] = s
        tables[name] = df
    return tables


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--from-year", type=int, default=2021)
    parser.add_argument("--to-year", type=int, default=2023)
    parser.add_argument("--check-only", action="store_true", help="validate sample-data/gold without rebuilding")
    args = parser.parse_args(argv)

    contract = load_contract()
    if args.check_only:
        tables = read_committed(contract)
    else:
        if not 2019 <= args.from_year <= args.to_year <= 2023:
            parser.error("the window must lie within 2019-2023 (years covered by both sources)")
        tables = conform(build_gold(range(args.from_year, args.to_year + 1)), contract)

    errors = validate(tables, contract)
    if errors:
        print("CONTRACT VIOLATIONS:", *errors, sep="\n  ")
        return 1

    if not args.check_only:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        for name, df in tables.items():
            write_csv(df, OUT_DIR / f"{name}.csv")

    total = 0
    for name, df in tables.items():
        size = (OUT_DIR / f"{name}.csv").stat().st_size
        total += size
        print(f"{name:<20} {len(df):>7} rows  {size / 1024:>8.1f} KB")
    print(f"{'total':<20} {'':>7}       {total / 1024:>8.1f} KB  - contract {contract['contract_version']} OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
