#!/bin/bash -e

# Usage: ./go.sh > results.txt

dropdb -U postgres challenge || true
createdb --encoding=UTF8 -U postgres challenge

psql -Atq challenge postgres \
  --variable="products_txt='$(pwd)/products.txt'" \
  --variable="listings_txt='$(pwd)/listings.txt'" \
  < schema.sql
