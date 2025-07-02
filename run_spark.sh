#!/bin/bash

# File: run_spark.sh
# Usage: docker-compose exec spark-master-1 /opt/bitnami/spark/scripts/run_spark.sh /opt/bitnami/spark/apps/process_embedding.py

APP_PATH=$1

if [[ -z "$APP_PATH" ]]; then
  echo "❌ Error: No Spark app path provided."
  echo "Usage: $0 <path_to_app.py_inside_container>"
  exit 1
fi

# Dynamically build JAR list
DELTA_JARS_DIR="/opt/bitnami/spark/delta_jars"
DELTA_JARS=$(find "$DELTA_JARS_DIR" -name "*.jar" | paste -sd "," -)

echo "✅ Submitting Spark job: $APP_PATH"
echo "📦 Delta JARs: $DELTA_JARS"

spark-submit \
  --master spark://spark-master-1:7077,spark-master-2:7077 \
  --deploy-mode client \
  --jars "$DELTA_JARS" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  "$APP_PATH"
