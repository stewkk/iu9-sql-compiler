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

## Queries

The standard 13 SSB query templates are in `queries/`. They are written in the
subset supported by this engine: explicit joins, table aliases, string
predicates, `BETWEEN`, list `IN`, `SUM`, `COUNT`, `GROUP BY`, and `ORDER BY`.

Run one query through the CLI:

```sh
./build/bin/sql --data-dir benchmarks/datasets/ssb/generated/sf1 \
  < benchmarks/datasets/ssb/queries/q1.1.sql
```

Run SSB benchmarks after generating data:

```sh
SSB_DATA_DIR=benchmarks/datasets/ssb/generated/sf1 \
  ./build/bin/benchmarks --benchmark_filter='SSB/'
```

The JIT expression executor still rejects string and `IN` expressions, so the
SSB benchmark registrations currently use the interpreted expression executor.

## Test the converter

```sh
make test-ssb-converter
```
