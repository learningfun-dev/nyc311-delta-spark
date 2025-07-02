# Use the official Bitnami Spark image as a base, which is configured for cluster usage.
FROM bitnami/spark:4.0

# Switch to the root user to have permissions to install packages
USER root

# Install curl for downloading JARs
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy the requirements file first to leverage Docker's layer caching.
# This layer will only be rebuilt if requirements.txt changes.
COPY requirements.txt .

# Install all Python dependencies from the requirements file.
# This ensures all workers and the master have the same libraries.
RUN pip install --no-cache-dir -r requirements.txt

# --- Pre-download Delta Lake JARs ---
# This avoids runtime dependency resolution issues with Ivy inside containers.
RUN mkdir -p /opt/bitnami/spark/delta_jars && \
    cd /opt/bitnami/spark/delta_jars && \
    curl -L -o delta-spark_2.13-4.0.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar && \
    curl -L -o delta-storage-4.0.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar && \
    curl -L -o antlr4-runtime-4.13.1.jar https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.13.1/antlr4-runtime-4.13.1.jar

# --- Pre-download ML Model ---
# Create a directory for the model cache that is writable by the spark user (UID 1001).
RUN mkdir -p /opt/bitnami/spark/models && \
    chown -R 1001:0 /opt/bitnami/spark/models && \
    chmod -R g+w /opt/bitnami/spark/models

# Set the cache directory for Hugging Face transformers.
# The sentence-transformers library will use this path.
ENV HF_HOME=/opt/bitnami/spark/models

# Set the HOME directory for the spark user to ensure processes like Ivy can resolve paths.
ENV HOME=/opt/bitnami/spark

# Copy the application code into the working directory of the image.
COPY ./apps ./apps

# Add the application's directory to PYTHONPATH so that its modules can be found by workers.
ENV PYTHONPATH "${PYTHONPATH}:/opt/bitnami/spark/apps"

# Run the download script as root to populate the cache.
RUN python /opt/bitnami/spark/apps/utils/download_model.py

# Add spark user entry for UID 1001
RUN echo "spark:x:1001:0::/home/spark:/bin/bash" >> /etc/passwd && \
    mkdir -p /home/spark && \
    chown -R 1001:0 /home/spark

ENV JAVA_TOOL_OPTIONS="-Dhadoop.security.authentication=simple"

COPY run_spark.sh ./scripts/run_spark.sh
RUN chmod +x ./scripts/run_spark.sh

# Switch back to the default non-root 'spark' user for better security.
USER 1001