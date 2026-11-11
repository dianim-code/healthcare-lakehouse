# Data sources

Every dataset used in this repository is public open data. Raw files are downloaded by
[`scripts/fetch_sources.py`](../scripts/fetch_sources.py) into `data/raw/` (git-ignored); the script
also writes `data/raw/manifest.json` with the URL, size and SHA-256 of each file. Only the small
derived extract in [`sample-data/gold/`](../sample-data/gold/) is committed.

## Attribution

> Contains data from the **Ministero della Salute** (Italian Ministry of Health), licensed under the
> [Italian Open Data License v2.0](https://www.dati.gov.it/iodl/2.0/), and from **ISTAT** (Italian
> National Institute of Statistics), licensed under
> [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The data has been cleaned, re-keyed and
> aggregated (see [`data-contract.md`](data-contract.md)); neither publisher endorses this project.

## 1. Hospital beds by site and discipline (Ministero della Salute)

**Dataset:** *Posti letto per stabilimento ospedaliero e disciplina* — beds of public and
equivalent hospitals, and accredited beds of private clinics, active on **1 January** of each year,
by hospital site and discipline.

| Edition | Landing page | File | Published |
|---|---|---|---|
| 2010–2019 | [page](https://www.dati.salute.gov.it/it/dataset/posti-letto-stabilimento-ospedaliero-e-disciplina-dal-2010-al-2019/) | `imported/C_17_dataset_96_0_upFile.csv` | 31/12/2018 (as stated on the page) |
| 2020 | [page](https://www.dati.salute.gov.it/it/dataset/posti-letto-stabilimento-ospedaliero-e-disciplina-2020/) | `imported/C_17_dataset_115_0_upFile.csv` | 31/05/2022 |
| 2021 | [page](https://www.dati.salute.gov.it/it/dataset/posti-letto-stabilimento-ospedaliero-e-disciplina-2021/) | `imported/C_17_dataset_153_0_upFile.csv` | 03/04/2023 |
| 2022 | [page](https://www.dati.salute.gov.it/it/dataset/posti-letto-stabilimento-ospedaliero-e-disciplina-2022/) | `2024-12/Posti letto per stabilimento ospedaliero e disciplina_2022.csv` | 06/05/2024 |
| 2023 | [page](https://www.dati.salute.gov.it/it/dataset/posti-letto-stabilimento-ospedaliero-e-disciplina-2023/) | `2025-07/Posti letto per stabilimento ospedaliero e disciplina_2023_0.csv` | 29/07/2025 |

File paths are relative to `https://www.dati.salute.gov.it/sites/default/files/`.

- **Publisher:** Ministero della Salute — Direzione generale della digitalizzazione, del sistema
  informativo sanitario e della statistica, Ufficio di Statistica.
- **License:** Italian Open Data License v2.0 (IODL 2.0), as stated on every landing page. The
  national catalogue [dati.gov.it](https://www.dati.gov.it/) lists the same dataset as CC BY 4.0;
  this repository follows the publisher's own page. Both licenses require attribution.
- **Data dictionary:** [Dataset-PostiLettoStabilimentoOspedalieroDisciplina2020.pdf](https://www.dati.salute.gov.it/dati/documenti/Dataset-PostiLettoStabilimentoOspedalieroDisciplina2020.pdf)
  (v2.0, 23/05/2022).
- **Underlying survey:** forms HSP11, HSP11bis, HSP12 and HSP13 (Ministerial Decree of 5 December 2006).
- **Downloaded:** 2026-11-10.

**Format (verified on the downloaded files):** CSV, `;` separator, ISO-8859-1 encoding, CRLF line
endings, one header row. Text fields are right-padded with spaces (fixed width) and some contain
`"`-quoted values with doubled quotes. The 23 columns are identical in every edition:

`Anno; Codice Regione; Descrizione Regione; Codice Azienda; Tipo Azienda; Codice struttura;
Subcodice; Denominazione Struttura/Stabilimento; Indirizzo; Codice Comune; Comune; Sigla Provincia;
Codice tipo struttura; Descrizione tipo struttura; Codice disciplina; Descrizione disciplina;
Tipo di Disciplina; N° Reparti; Posti letto degenza ordinaria; Posti letto degenza a pagamento;
Posti letto Day Hospital; Posti letto Day Surgery; Totale posti letto`

**Profiling (2010–2023, 164,294 rows):** no duplicate on
*year × structure code × subcode × discipline*; no negative or non-numeric counts; the total always
equals the sum of the four regimes; 21 region codes; 69 discipline codes, each with a single
discipline type. In 2021–2023 there are 1,319 distinct hospital sites, about 11,700 rows per year.

**Known caveats:**
- Beds describe the **declared size** of wards on 1 January, not day-to-day availability.
- In Lombardia most directly-managed hospitals belong to hospital authorities rather than to the
  local health authority (note in the data dictionary).
- Some sites change attributes across years. In 2021–2023, out of 1,319 sites, 56 change name, 52
  health-authority code, 4 facility type and 3 municipality. The gold layer keeps the latest values
  (SCD type 1).
- One site name contains a no-break space (U+00A0). Cleaning rule C1 handles it explicitly.
- The structure *code* never changes region, and no structure is published both as a whole (blank
  subcode) and by site in the same year, so there is no double counting.
- The `N° Reparti` value for private clinics is only a 0/1 flag (see the data dictionary).

## 2. Resident population by region (ISTAT)

**Dataset:** *Popolazione residente al 1° gennaio* (Resident population on 1st January),
SDMX dataflow `IT1,22_289,1.0`, data structure `DCIS_POPRES1`.

- **Query used:** `A.<21 areas>.JAN.9.TOTAL.99` from 2019 onwards — annual, 1 January, both sexes,
  all ages, all marital statuses. The 21 area codes are in
  [`contract/reference/regions.csv`](contract/reference/regions.csv).
- **Endpoint:** `https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.0/...`, requested as
  SDMX-CSV (`Accept: application/vnd.sdmx.data+csv;version=1.0.0`).
- **Explorer:** [esploradati.istat.it](https://esploradati.istat.it/databrowser/)
- **Publisher:** ISTAT — Istituto Nazionale di Statistica.
- **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), per the
  [ISTAT legal notes](https://www.istat.it/note-legali/) ("salvo diversa indicazione, tutti i
  contenuti pubblicati su questo sito sono soggetti alla licenza Creative Commons – Attribuzione –
  versione 4.0"). Some older ISTAT pages still mention CC BY 3.0.
- **Coverage verified:** 2019–2025, all 21 areas.
- **Downloaded:** 2026-11-10.

**Area codes:** ISTAT uses the territory codelist `CL_ITTER107`, which keeps the pre-2021 NUTS
codes for the North-East and Centre (`ITD1`–`ITD5`, `ITE1`–`ITE4`) instead of the current NUTS
2021 codes (`ITH*`, `ITI*`). Queries with `ITH*`/`ITI*` return no data. The labels of every code
were checked against the codelist (for example, `ITD1` = Provincia Autonoma Bolzano/Bozen).

## Not used (evaluated)

- *Posti letto per struttura ospedaliera* and *Posti letto per Regione e disciplina*: coarser
  versions of source 1.
- *Strutture di ricovero pubbliche e equiparate presenti nel territorio della ASL*: a single
  snapshot. Source 1 already carries the site attributes we need, for every year.
- *Corrispondenze ASL-comuni e popolazione residente* (Ministry): a single year (2024), so it cannot
  provide a population time series.
