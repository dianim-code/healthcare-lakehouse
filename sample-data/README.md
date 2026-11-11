# Sample data

`gold/` holds a small extract of the gold layer: years **2021–2023**, all 21 regions, about 1.2 MB.
It matches [`docs/contract/gold_schema.yml`](../docs/contract/gold_schema.yml) exactly.
`scripts/vendor_contract.py` copies it into `healthcare-semantic-model` and `deneb-gallery`, so
neither of them needs this repository or a Databricks workspace.

Rebuild and validate it:

```bash
uv run scripts/fetch_sources.py
uv run scripts/build_sample.py
```

**Attribution.** Derived from data published by the Ministero della Salute (IODL 2.0) and ISTAT
(CC BY 4.0). Sources, licenses and caveats are in
[`docs/data-sources.md`](../docs/data-sources.md).
