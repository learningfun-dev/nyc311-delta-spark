# Use the official Bitnami Spark image as a base, which is configured for cluster usage.
FROM bitnami/spark:3.5

# Switch to the root user to have permissions to install packages
USER root

# Copy the requirements file first to leverage Docker's layer caching.
# This layer will only be rebuilt if requirements.txt changes.
COPY requirements.txt .

# Install all Python dependencies from the requirements file.
# This ensures all workers and the master have the same libraries.
RUN pip install --no-cache-dir -r requirements.txt

# --- Pre-download ML Model ---
# Create a directory for the model cache that is writable by the spark user (UID 1001).
RUN mkdir -p /opt/bitnami/spark/models && \
    chown -R 1001:0 /opt/bitnami/spark/models && \
    chmod -R g+w /opt/bitnami/spark/models

# Set the cache directory for Hugging Face transformers.
# The sentence-transformers library will use this path.
ENV HF_HOME=/opt/bitnami/spark/models

# Copy the application code into the working directory of the image.
COPY ./apps ./apps

# Add the application's directory to PYTHONPATH so that its modules can be found by workers.
ENV PYTHONPATH "${PYTHONPATH}:/opt/bitnami/spark/apps"

# Run the download script as root to populate the cache.
RUN python /opt/bitnami/spark/apps/utils/download_model.py

# Switch back to the default non-root 'spark' user for better security.
USER 1001