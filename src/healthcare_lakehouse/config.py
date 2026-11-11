"""Static mappings from the source files to the staging names used by the silver layer."""

from __future__ import annotations

from typing import Final

# Default gold window: the years covered by both sources (beds 2010-2023, population 2019-2025).
GOLD_WINDOW_DEFAULT: Final[tuple[int, int]] = (2019, 2023)

# Ministry beds CSV header (after stripping) -> staging name. Same 23 columns in every edition.
BEDS_COLUMNS: Final[dict[str, str]] = {
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

COUNT_COLUMNS: Final[tuple[str, ...]] = (
    "ward_count",
    "beds_ordinary",
    "beds_paying",
    "beds_day_hospital",
    "beds_day_surgery",
    "beds_total",
)

# Labels verbatim from the Ministry data dictionary (v2.0, field TIPO AZIENDA).
AUTHORITY_TYPES: Final[dict[str, str]] = {
    "1": "Azienda sanitaria locale (ASL)",
    "2": "Struttura sanitaria aziendale di tipo ospedaliero",
}

# Columns copied from the latest beds row into dim_facility (SCD type 1, rule C3).
FACILITY_ATTRIBUTES: Final[tuple[str, ...]] = (
    "structure_code",
    "site_subcode",
    "facility_name",
    "address",
    "municipality_code",
    "municipality_name",
    "province_code",
    "region_code",
    "authority_code",
    "authority_type_code",
    "facility_type_code",
    "facility_type_name",
)

# ISTAT SDMX-CSV header columns used by the population source.
POPULATION_COLUMNS: Final[dict[str, str]] = {
    "REF_AREA": "istat_area_code",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "population",
}
