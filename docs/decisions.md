# Decision log

Architecture decisions in a light ADR format: **context → decision → trade-offs**. Each entry is
dated. Version-dependent facts link to the official documentation they were checked against, and
the [verification log](#verification-log) at the end lists every such check.

---

## ADR-001 · One dataset, three repositories, contract owned by the lakehouse

*2026-11-10 · Accepted (revised the same day: originally a monorepo)*

**Context.** The portfolio has to show end-to-end data work, not three unrelated demos, and each
project has to stand on its own as a repository.

**Decision.** Three repositories built on one public dataset: `healthcare-lakehouse` (this one),
`healthcare-semantic-model` and `deneb-gallery`. The lakehouse **produces** the gold layer, so it
**owns** the data contract (`docs/contract/`), the source scripts (`scripts/`) and the gold sample
(`sample-data/gold/`). The two consumers keep a **vendored, read-only copy** of the contract and
the sample, written by `scripts/vendor_contract.py` together with a `contract/VENDORED.md` that
records the contract version and file checksums.

**Trade-offs.** The sample is duplicated in three repositories (about 1.2 MB each). In exchange each
repository builds and runs with no dependency on the others and no Databricks workspace. A
contract change means: bump `contract_version` here, then re-vendor to both consumers
(`vendor_contract.py --check` reports a stale copy).

## ADR-002 · Python 3.12 managed with uv

*2026-11-10 · Accepted*

**Context.** The PySpark code must behave the same locally and on Databricks serverless.

**Decision.** Use Python **3.12** (`.python-version`) and **uv** as the only package and
interpreter manager. Root scripts declare their dependencies inline (PEP 723), so
`uv run scripts/<x>.py` needs no setup.

**Evidence.** Every current Databricks serverless environment version from 3 to 6 ships
Python **3.12.3**. Version 6 is the latest, released 2026-09-03.
→ [Serverless environment versions](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/)

**Trade-offs.** No system Python is required, but contributors need uv.

## ADR-003 · Databricks Free Edition, bundle inside the lakehouse project

*2026-11-10 · Accepted*

**Context.** The workspace is Databricks **Free Edition** (serverless only). The VS Code extension
had created a `databricks.yml` at the repo root with the workspace host written in it.

**Decision.** The root `databricks.yml` was removed. The Asset Bundle lives in
`healthcare-lakehouse/databricks.yml` and takes the host from the Databricks CLI profile
(`~/.databrickscfg`), never from a file in the repo. Tooling: Databricks CLI **v1.24.0** (bundled
with the VS Code extension 2.23.1).

**Trade-offs.** The VS Code extension has to be pointed at `healthcare-lakehouse/` as its project
root. Free Edition limits still have to be checked against the docs (see the open items).

*ADR-004 (Power BI format) moved to `healthcare-semantic-model`, ADR-008 (Deneb chart set) to
`deneb-gallery`. The numbers are kept so that cross-references between the repositories stay valid.*

## ADR-005 · Language and licensing

*2026-11-10 · Accepted*

**Decision.** English for code, column names, docs and commit messages. The code is under the
**MIT** license. Data values stay in **Italian as published** (`Acuti`, `Casa di cura privata
accreditata`, `Sud e Isole`), because translating official categories would invent labels the
sources do not define. The data keeps its own licenses: IODL 2.0 (Ministry) and CC BY 4.0 (ISTAT),
with attribution in [`data-sources.md`](data-sources.md).

**Trade-offs.** Report labels mix English (UI) and Italian (categories), which is acceptable for an
Italian-domain dataset.

## ADR-006 · Sources: site-level beds plus ISTAT population

*2026-11-10 · Accepted*

**Context.** The first design used beds by site and discipline, a facility registry and ISTAT
population.

**Decision.**
- **Beds:** the Ministry dataset *Posti letto per stabilimento ospedaliero e disciplina*. Its
  23-column schema is identical in every edition from 2010 to 2023, so a single parser handles all
  of them.
- **Facility attributes come from the beds file itself.** It already carries region, health
  authority, municipality, province and facility type for every year. The separate registry
  dataset is a single snapshot and would add a second, inconsistent version of the same
  attributes.
- **Population:** ISTAT SDMX dataflow `22_289`, regions and autonomous provinces, from 2019. ISTAT
  uses the legacy area codes `ITD*` and `ITE*` for the North-East and Centre. The mapping was
  checked against codelist `CL_ITTER107` and stored in `contract/reference/regions.csv`.

**Download transport.** The Ministry portal resets TLS handshakes from Python's OpenSSL stack
(httpx and `ssl`, with or without legacy options), while it serves the same files to the system
`curl`. `fetch_sources.py` therefore downloads Ministry files with `curl` and an honest generic
user agent, and uses httpx for ISTAT. `curl` ships with Windows 10+ and every GitHub runner.

**Trade-offs.** The common window of both sources is **2019–2023** (beds stop at 2023, population
starts at 2019).

## ADR-007 · Contract design

*2026-11-10 · Accepted*

**Decision.**
- **Two facts at different grains** (beds: year × site × discipline; population: year × region)
  sharing `dim_year` and `dim_region`. Per-capita measures are computed in the model, not stored.
- **`region_code` is denormalised onto the beds fact**, so region filters and RLS reach both facts
  through one dimension without a snowflake.
- **SCD type 1** for facilities and disciplines. The history of a site's name or authority is not
  an analytical requirement, and type 2 would multiply facility rows for little gain.
- **Surrogate `facility_key`** = the ordinal of `facility_id` within the gold window. It is
  deterministic and reproducible in both pandas and Spark without a hashing library, but not
  stable across windows. The rejected alternative was a 64-bit hash: stable, but `xxhash64` in
  Spark is hard to reproduce bit-for-bit in pandas.
- **Sample window 2021–2023** (about 1.2 MB in total, below the 5 MB budget). The lakehouse
  defaults to 2019–2023.

---

## ADR-010 · Lakeflow pipeline in Python with `pyspark.pipelines`

*2026-11-10 · Accepted*

**Context.** The pipeline must be written against the current Lakeflow Declarative Pipelines Python API,
not a legacy one.

**Decision.** `from pyspark import pipelines as dp`, with `@dp.materialized_view(name=...)` for every dataset
(bronze, silver and gold are batch reads of files, so streaming tables are not needed). Quality checks use
`@dp.expect_all_or_fail({...})`. The legacy `import dlt` is not used. Settings are read with `spark.conf.get`
from the pipeline configuration.

**Evidence.**
- Python API page, last updated 2026-10-15: [docs.databricks.com/aws/en/ldp/developer/python-dev](https://docs.databricks.com/aws/en/ldp/developer/python-dev).
  It shows the `pyspark.pipelines` import, `materialized_view` and `table`, and reading an upstream dataset with
  `spark.read.table("name")`.
- Expectations reference, last updated 2026-09-11: [docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-expectations](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-expectations).
  It gives the signatures of `expect`, `expect_or_drop`, `expect_or_fail`, `expect_all`, `expect_all_or_drop` and
  `expect_all_or_fail`. The `expect` decorators are not part of Apache Spark.
- The `pyspark.pipelines` module is present in the pyspark 4.4.0.dev0 that `databricks-connect` 19.3 installs (checked
  in the project virtualenv).
- Pipeline configuration values read with `spark.conf.get`: [docs.databricks.com/aws/en/ldp/parameters](https://docs.databricks.com/aws/en/ldp/parameters)
  (found by search; the page was not fetched in full).

**Trade-offs.** The pipeline file runs only on Databricks and cannot be imported locally. It is covered by ruff and by
the tests of the functions it calls. It has not been executed, because deployment and runs on the workspace are out of
scope for this build.

## ADR-011 · Databricks Free Edition: what is verified and what is not

*2026-11-10 · Accepted*

**Context.** The workspace is Free Edition. Before the first deployment, the documentation was checked for the
services the project needs (Lakeflow pipelines, Volumes, bundle deployment), to record anything unsupported.

**Verified.**
- [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), last updated
  2026-10-18: serverless compute only, with custom compute configurations unsupported; one active Lakeflow pipeline per
  pipeline type; Jobs limited to 5 concurrent tasks; no R or Scala; custom workspace storage locations unsupported.
  Lakeflow pipelines are not listed as unsupported.
- Bundle resource fields were taken from the JSON schema of the installed CLI (v1.24.0), not from the docs pages, which
  do not list fields. Pipeline: `serverless`, `catalog`, `schema`, `libraries` (`glob.include`), `environment`
  (`dependencies`), `configuration`. Volume: `catalog_name`, `schema_name`, `name`, `volume_type`. Schema: `catalog_name`,
  `name`.
- `databricks bundle validate -t dev` passes (CLI v1.24.0, profile authenticated, no host in the files).

**Not verified from the docs** (both confirmed later by the first deployment, ADR-016).
- Unity Catalog volumes and bundle deployment are **not mentioned** on the Free Edition page. They are not listed as
  unsupported either.
- Whether `databricks fs cp` to a `dbfs:/Volumes/...` path works with CLI v1.24.0.

**Trade-offs.** Documentation alone left two points open; the first `bundle deploy` and `bundle run` on Free Edition
closed them (ADR-016).

## ADR-012 · Two Spark backends, one code path

*2026-11-10 · Accepted*

**Decision.** The package uses only the PySpark DataFrame API. Two dependency groups select the backend:
`local` (`pyspark` 4.x, used in CI) and `databricks` (`databricks-connect` 19.3, serverless Spark 4.2). They are
declared in conflict, so they cannot be installed together. `conftest.py` selects the backend with `SPARK_BACKEND`
and takes the profile from `DATABRICKS_CONFIG_PROFILE`. No profile name is written in the repository.

**Trade-offs.** Databricks Connect runs readers on the server, which cannot see local paths. The beds and population
files are therefore parsed with pandas in tests on that backend. The Spark CSV reader itself is exercised only on the
local backend (CI). The local backend could not be run in this environment, because Java is not installed here.

## ADR-013 · Parity test: Spark gold equals the pandas sample

*2026-11-10 · Accepted*

**Decision.** `tests/test_parity.py` builds all six gold tables for 2021 to 2023 with the PySpark functions and
compares them with `sample-data/gold/*.csv` cell by cell, after rendering values the way the CSV writer does.

**Result.** Run on the Databricks backend, the parity test passed for all six tables, together with the rest of the
suite. No value differs from the pandas reference.

**CI.** The workflow fetches the three beds files and the population file, as a non-blocking step. If the fetch fails,
the parity tests are skipped and `pytest -rs` shows the reason. Raw data is never committed. Rejected alternative: a
committed raw extract, which would add licensed data to the repository.

## ADR-014 · Packaging the code for the pipeline

*2026-11-10 · Accepted*

**Decision.** The package is built as a wheel with `uv build --wheel`. The wheel is uploaded to
`landing/_libs/` in the volume, and the pipeline's `environment.dependencies` names it. The region seed
(`reference/regions.csv`) is packaged inside the wheel, and a test keeps its rows identical to the contract copy. The
contract copy is CRLF and the packaged copy is LF, so the test compares parsed rows, not bytes.

**Rejected.** A wheel reference through a bundle artifact or a glob in the pipeline environment: neither is shown in
the pages consulted (external-dependencies and pipelines-tutorial, last updated 2026-09-11). A `sys.path` workaround in
the pipeline file: fragile and not supported.

**Trade-offs.** The upload is a manual step. `bundle validate` does not check that the wheel exists in the volume.

## ADR-015 · Data-quality rules: what the expectations cover

*2026-11-10 · Accepted*

**Decision.** Rule C7 is split between the pipeline expectations and the construction of the tables.
- **Expectations (fail the update):** non-null keys and counts, counts ≥ 0, `beds_total` equal to the sum of the four
  regimes, region codes in the reference list, population ≥ 1, `first_year` ≤ `last_year`, `discipline_type` in its
  three values, `health_authority_type` known.
- **Enforced by construction and tested:** unique primary keys (one row per key from window functions), foreign keys
  (inner joins to the dimensions), and "population present for every year × region" (the contract and parity tests).

**Trade-off.** Row-level expectations cannot express the table-level rules. These are documented gaps, not hidden ones.

---

## ADR-016 · First deployment on Free Edition: three fixes

*2026-11-10 · Accepted*

**Context.** The first `bundle deploy` and `bundle run` on Databricks Free Edition exposed three problems
that local tests could not catch, because tests never deploy and never write Delta tables.

**Decision.**
- **Schema name in development mode.** The `dev` target uses `mode: development`, which prefixes the
  schema (`dev_<user>_healthcare_lakehouse`). Paths built from `${var.schema}` pointed to a schema that
  did not exist. The volume path in `environment.dependencies` and `lakehouse.raw_path` now use
  `${resources.schemas.lakehouse_schema.name}`, resolved at deploy time; checked on the deployed pipeline.
- **Wheel upload.** `databricks fs cp <wheel> .../_libs/` created a *file* named `_libs` when the folder did
  not exist. The README now creates the folder first and copies to the full file name.
- **Column names in bronze.** Bronze keeps the Ministry headers ("Codice Regione", "N° Reparti"), which
  Delta rejects without column mapping (`DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`). `bronze_beds` sets
  `delta.columnMapping.mode = name` through `table_properties`. A table registered by a failed update keeps
  its old properties, so the first run after the change needs `--full-refresh-all`.

**Trade-offs.** Renaming the columns in bronze would avoid column mapping, but bronze would no longer mirror
the source. Column mapping is supported on Databricks Delta; readers outside Databricks need a Delta
reader that supports it.

## ADR-017 · Contract 1.1.0: the 20 ISTAT regions next to the 21 Ministry codes

*2026-11-11 · Accepted (raised while building the report)*

**Context.** The Ministry codes the autonomous provinces of Bolzano and Trento separately (21 rows). Most
readers expect the 20 Italian regions, and charts with 21 panels leave an uneven grid.

**Decision.** `dim_region` gains `istat_region_name`, equal to `region_name` except for 041 and 042,
which both read Trentino-Alto Adige. The key, the rows and the population mapping do not change, so the
change is additive and existing consumers keep working. The value lives in the region seed
(`docs/contract/reference/regions.csv` and its packaged copy); a test checks the 20 values and the
grouping of the two provinces.

**Trade-offs.** The two provinces stay available as rows, as data publishers report them. Per-capita
figures for Trentino-Alto Adige must sum beds and population of both provinces before dividing; averaging
the two ratios would be wrong. The deployed pipeline was redeployed with a full refresh on 2026-11-11 to publish the column (verification log).

## Open items to verify

| Item | Owner |
|---|---|
| Free Edition supports Lakeflow Declarative Pipelines, UC Volumes and bundle deployment. **Resolved (ADR-016):** deployed and run end to end on Free Edition on 2026-11-10 | Lakehouse |
| Current Lakeflow Python API (`pyspark.pipelines` vs `dlt`) and the expectation decorators. **Resolved (ADR-010)** | Lakehouse |
| **Parity test**: the PySpark gold functions, run on the raw 2021–2023 files, must reproduce `sample-data/gold/*.csv` value for value. **Resolved (ADR-013):** all six tables pass on the Databricks backend | Lakehouse |

Open items for the consumers live in their own `docs/decisions.md`.

## Verification log

| Date | Topic | Version / fact | Source |
|---|---|---|---|
| 2026-11-10 | Databricks serverless Python | env v3–v6 → Python 3.12.3; v6 released 2026-09-03 | [docs.databricks.com](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/) |
| 2026-11-10 | Databricks CLI | v1.24.0 (VS Code extension 2.23.1) | `databricks --version` |
| 2026-11-10 | Beds dataset format | 23 cols, `;`, ISO-8859-1, identical 2010–2023 | downloaded files and the [data dictionary v2.0](https://www.dati.salute.gov.it/dati/documenti/Dataset-PostiLettoStabilimentoOspedalieroDisciplina2020.pdf) |
| 2026-11-10 | ISTAT population | dataflow `IT1,22_289,1.0`, 2019–2025, codes `ITC/ITD/ITE/ITF/ITG` | SDMX REST queries and codelist `CL_ITTER107` |
| 2026-11-10 | Licenses | Ministry IODL 2.0 (dataset pages); ISTAT CC BY 4.0 | [ISTAT legal notes](https://www.istat.it/note-legali/) |
| 2026-11-10 | pre-commit hooks | pre-commit-hooks v6.0.0, gitleaks v8.30.1, ruff-pre-commit v0.17.4 | GitHub releases API |
| 2026-11-10 | Lakeflow Python API | `from pyspark import pipelines as dp`; `materialized_view`, `expect_all_or_fail` (page updated 2026-10-15 and 2026-09-11) | [python-dev](https://docs.databricks.com/aws/en/ldp/developer/python-dev), [expectations](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-expectations) |
| 2026-11-10 | Free Edition limits | serverless only; one active pipeline per type; Volumes and bundles not mentioned (page updated 2026-10-18) | [free-edition-limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) |
| 2026-11-10 | Bundle schema | `pipelines` and `volumes` field names, from the CLI schema | `databricks bundle schema` (CLI v1.24.0) |
| 2026-11-10 | Databricks Connect | databricks-connect 19.3 (`pyspark` 4.4.0.dev0 bundled); parity and contract tests pass on serverless | `uv run --group databricks pytest` |
| 2026-11-10 | First deploy on Free Edition | `bundle deploy -t dev` creates schema `dev_<user>_healthcare_lakehouse`, volume `landing` and the serverless pipeline; files synced respect `.gitignore` (no raw data) | CLI v1.24.0, `bundle summary`, `pipelines get` |
| 2026-11-10 | Pipeline run | `bundle run --full-refresh-all` completes all bronze, silver and gold datasets with expectations; the wheel installs from the volume | pipeline update `6794e114` |
| 2026-11-10 | Gold on Databricks vs sample | 2021-2023 identical to `sample-data/gold`: rows 11,664 / 11,736 / 11,653, beds 230,992 / 221,671 / 212,768, ordinary 209,568 / 200,428 / 191,547, facilities 1,285 / 1,277 / 1,272, population equal. Window 2019-2023: 5 years, 1,360 facilities, 21 regions, 69 disciplines; the U+00A0 site name is cleaned | SQL warehouse, statement API (read-only) |
| 2026-11-11 | Redeploy for contract 1.1.0 | `scripts/deploy_dev.sh <profile> --full-refresh`: pipeline update `6b5b2c80` completed; `dim_region` has 21 rows and 20 `istat_region_name` values (041 and 042 = Trentino-Alto Adige); beds 2021-2023 unchanged (230,992 / 221,671 / 212,768). The re-uploaded wheel with the same version was picked up. Script fix: strip the CR that Windows Python adds to the bundle-name lookup | SQL warehouse, statement API (read-only) |
