#!/usr/bin/env bash
# Generates three CSV tables for multi-way join benchmarking with skewed
# cardinalities: regions (10) << customers (500) << orders (5000).
#
# Schema (all int):
#   regions(id)
#   customers(id, region_id)        -- region_id is FK into regions
#   orders(id, customer_id)         -- customer_id is FK into customers
#
# Idempotent: overwrites existing files at OUT_DIR.

set -euo pipefail

OUT_DIR="${1:-$(dirname "$0")/../test/static/executor/test_data}"

n_regions=10
n_customers=500
n_orders=5000

regions="$OUT_DIR/regions.csv"
customers="$OUT_DIR/customers.csv"
orders="$OUT_DIR/orders.csv"

{
  echo 'id:int'
  seq 1 "$n_regions"
} > "$regions"

{
  echo 'id:int,region_id:int'
  awk -v n="$n_customers" -v r="$n_regions" 'BEGIN {
    srand(1);
    for (i = 1; i <= n; ++i) printf "%d,%d\n", i, 1 + int(rand() * r);
  }'
} > "$customers"

{
  echo 'id:int,customer_id:int'
  awk -v n="$n_orders" -v c="$n_customers" 'BEGIN {
    srand(2);
    for (i = 1; i <= n; ++i) printf "%d,%d\n", i, 1 + int(rand() * c);
  }'
} > "$orders"

echo "wrote $regions ($(wc -l < "$regions") lines)"
echo "wrote $customers ($(wc -l < "$customers") lines)"
echo "wrote $orders ($(wc -l < "$orders") lines)"
