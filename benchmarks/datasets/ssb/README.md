# Star Schema Benchmark data

This directory contains the schema and conversion tooling for Star Schema
Benchmark data. It does not download or build an SSB data generator; generate
the `.tbl` files with an external SSB dbgen-compatible tool first.

## Convert `.tbl` files

Expected input files:

- `lineorder.tbl`
- `customer.tbl`
- `supplier.tbl`
- `part.tbl`
- `date.tbl`

Convert them to the CSV format used by this project:

```sh
python3 benchmarks/datasets/ssb/convert_tbl_to_csv.py \
  --input-dir /path/to/ssb/tbl \
  --output-dir benchmarks/datasets/ssb/generated/sf1
```

The converter reads `schema.json` for table and column definitions. Output CSV
headers use `column:type`, for example `lo_orderkey:int`. String columns are
preserved as `string` values in the generated CSV.

Generated full-scale data should stay under `benchmarks/datasets/ssb/generated/`
or another ignored location.

## Current engine limitation

This first slice only adds data layout and conversion. The current C++ scanner
supports `int` and `NULL` values, so SSB CSV files containing `string` columns
are not executable until string type support is added to the parser, scanner,
executor, and output formatting.

## Test the converter

```sh
make test-ssb-converter
```
