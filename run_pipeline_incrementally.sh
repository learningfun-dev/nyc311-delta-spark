#!/bin/bash

# This script processes files from the input directory one-by-one through the full Spark pipeline.
# If a CSV file is already present in the raw directory (e.g., due to an interrupted run),
# it resumes from there without moving a new file from input.

set -e  # Exit immediately if a command exits with a non-zero status.

# -------------------------------
# Directory Configuration
# -------------------------------
BASE_DIR=$(dirname "$(readlink -f "$0")")
INPUT_DIR="$BASE_DIR/data/input"
RAW_DIR="$BASE_DIR/data/raw"
PROCESSED_DIR="$BASE_DIR/data/processed"

mkdir -p "$RAW_DIR" "$PROCESSED_DIR"

# -------------------------------
# Utility Functions
# -------------------------------
log() {
  echo "----------------------------------------------------------------------"
  echo "$1"
  echo "----------------------------------------------------------------------"
}

restart_spark_cluster() {
  log "Stopping existing Docker containers (if any)..."
  docker compose down

  log "Starting Spark cluster..."
  docker compose --profile spark up -d
}

run_pipeline() {
  local filename="$1"
  log "Processing file: $filename"

  echo "Running full Spark pipeline for '$filename'..."
  docker-compose exec -T spark-master-1 /opt/bitnami/spark/scripts/run_spark.sh \
    /opt/bitnami/spark/apps/process_all.py

  echo "Archiving '$filename'..."
  mv "$RAW_DIR/$filename" "$PROCESSED_DIR/"
}

# -------------------------------
# Check for Input Files
# -------------------------------
csv_files=("$INPUT_DIR"/*.csv)

if [ ! -d "$INPUT_DIR" ] || [ ${#csv_files[@]} -eq 0 ]; then
  log "Input directory $INPUT_DIR has no CSV files or does not exist. Nothing to process."
  exit 0
fi

log "Starting incremental pipeline run..."
log "Building Docker images (if needed)..."
docker compose build

# -------------------------------
# Determine if resuming from raw
# -------------------------------
raw_file=$(find "$RAW_DIR" -maxdepth 1 -type f -name "*.csv" | head -n 1)
resuming=true

if [ -z "$raw_file" ]; then
  resuming=false
fi

# -------------------------------
# Process Files
# -------------------------------
for file_path in "$INPUT_DIR"/*.csv; do
  [ -f "$file_path" ] || continue

  restart_spark_cluster

  if $resuming; then
    raw_file=$(find "$RAW_DIR" -maxdepth 1 -type f -name "*.csv" | head -n 1)
    filename=$(basename "$raw_file")
    resuming=false
  else
    filename=$(basename "$file_path")
    echo "Moving '$filename' to raw directory..."
    mv "$file_path" "$RAW_DIR/"
  fi

  run_pipeline "$filename"
done

log "All CSV files from $INPUT_DIR have been processed."
