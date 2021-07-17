#!/usr/bin/env bash

cd "$(dirname "$0")"
source venv/bin/activate
BASE_DIR="$(pwd)"

for f in "$BASE_DIR/exporter/*";
do
  echo "Starting $f file..."
#   python --version
  python $f
done