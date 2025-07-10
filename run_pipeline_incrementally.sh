#!/bin/bash

# This script processes files from the input directory one-by-one.
# It maintains persistent state for ChromaDB and Zookeeper while restarting
# the Spark cluster for each file to ensure a clean computational environment.

set -e  # Exit immediately if a command exits with a non-zero status.

# -------------------------------
# Directory and Service Configuration
# -------------------------------
BASE_DIR=$(dirname "$(readlink -f "$0")")
INPUT_DIR="$BASE_DIR/data/input"
RAW_DIR="$BASE_DIR/data/raw"
PROCESSED_DIR="$BASE_DIR/data/processed"

# Define the service names for clarity and easy modification
PERSISTENT_SERVICES="chromadb zookeeper"
SPARK_SERVICES="spark-master-1 spark-master-2 spark-worker-1 spark-worker-2 spark-worker-3"

mkdir -p "$RAW_DIR" "$PROCESSED_DIR"

# -------------------------------
# Utility Functions
# -------------------------------
log() {
  echo "======================================================================"
  echo "=> $1"
  echo "======================================================================"
}

# -------------------------------
# Main Script Logic
# -------------------------------

# Check for Input Files
csv_files=("$INPUT_DIR"/*.csv)
if [ ! -d "$INPUT_DIR" ] || [ ${#csv_files[@]} -eq 0 ] || [ ! -e "${csv_files[0]}" ]; then
  log "Input directory $INPUT_DIR has no CSV files or does not exist. Nothing to process."
  exit 0 # The cleanup trap will run
fi

# --- Start persistent services ONCE ---
log "Building Docker images (if needed)..."
docker compose build

log "Starting persistent services: $PERSISTENT_SERVICES"
docker compose up -d $PERSISTENT_SERVICES

# --- Process each file in a loop ---
for file_path in "$INPUT_DIR"/*.csv; do
  [ -f "$file_path" ] || continue

  filename=$(basename "$file_path")
  
  # --- Restart Spark services for a clean slate ---
  log "Restarting Spark cluster for file: $filename"
  docker compose stop $SPARK_SERVICES
  docker compose rm -f $SPARK_SERVICES # Force remove to be sure
  docker compose up -d $SPARK_SERVICES # Start fresh Spark containers

  # --- Health check loop to wait for Spark Master to be ready ---
  log "Waiting for Spark Master to be healthy..."
  until docker-compose exec spark-master-1 curl -s -f "http://localhost:8080" > /dev/null; do
    echo "Spark Master is not ready yet. Waiting 5 seconds..."
    sleep 5
  done
  log "Spark Master is healthy. Proceeding with job submission."
  # --- End of Health Check ---

  log "Moving '$filename' to raw directory for processing..."
  mv "$file_path" "$RAW_DIR/"

  log "Running full Spark pipeline for '$filename'..."
  docker-compose exec -T spark-master-1 /opt/bitnami/spark/scripts/run_spark.sh \
    /opt/bitnami/spark/apps/process_all.py

  log "Archiving '$filename'..."
  mv "$RAW_DIR/$filename" "$PROCESSED_DIR/"
done

log "All CSV files have been successfully processed."
# The cleanup trap will now execute, stopping all services.
