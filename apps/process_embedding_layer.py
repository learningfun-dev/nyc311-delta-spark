'''
    Process embedding layer using a Spark Structured Streaming approach.
'''
import sys
import os
from itertools import islice
import chromadb
from chromadb.utils import embedding_functions
from pyspark.sql.functions import col, lit, concat_ws, sha2, coalesce, when
from constant import constants
from utils.spark_utils import get_spark_session
from utils.logging_utils import get_logger

EMBEDDING_BANNER = """
   ▄████████   ▄▄▄▄███▄▄▄▄   ▀█████████▄     ▄████████ ████████▄  ████████▄   ▄█  ███▄▄▄▄      ▄██████▄        ▄█          ▄████████ ▄██   ▄      ▄████████    ▄████████ 
  ███    ███ ▄██▀▀▀███▀▀▀██▄   ███    ███   ███    ███ ███   ▀███ ███   ▀███ ███  ███▀▀▀██▄   ███    ███      ███         ███    ███ ███   ██▄   ███    ███   ███    ███ 
  ███    █▀  ███   ███   ███   ███    ███   ███    █▀  ███    ███ ███    ███ ███▌ ███   ███   ███    █▀       ███         ███    ███ ███▄▄▄███   ███    █▀    ███    ███ 
 ▄███▄▄▄     ███   ███   ███  ▄███▄▄▄██▀   ▄███▄▄▄     ███    ███ ███    ███ ███▌ ███   ███  ▄███             ███         ███    ███ ▀▀▀▀▀▀███  ▄███▄▄▄      ▄███▄▄▄▄██▀ 
▀▀███▀▀▀     ███   ███   ███ ▀▀███▀▀▀██▄  ▀▀███▀▀▀     ███    ███ ███    ███ ███▌ ███   ███ ▀▀███ ████▄       ███       ▀███████████ ▄██   ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀   
  ███    █▄  ███   ███   ███   ███    ██▄   ███    █▄  ███    ███ ███    ███ ███  ███   ███   ███    ███      ███         ███    ███ ███   ███   ███    █▄  ▀███████████ 
  ███    ███ ███   ███   ███   ███    ███   ███    ███ ███   ▄███ ███   ▄███ ███  ███   ███   ███    ███      ███▌    ▄   ███    ███ ███   ███   ███    ███   ███    ███ 
  ██████████  ▀█   ███   █▀  ▄█████████▀    ██████████ ████████▀  ████████▀  █▀    ▀█   █▀    ████████▀       █████▄▄██   ███    █▀   ▀█████▀    ██████████   ███    ███ 
                                                                                                              ▀                                               ███    ███ 
"""

def _upsert_partition_to_chromadb(iterator: iter) -> None:
    """
    Initializes a ChromaDB client and upserts a partition of data in batches.
    This function is executed on each Spark executor.
    """
    # Initialize client and embedding function on the executor
    chroma_client = chromadb.HttpClient(host=constants.EMBEDDING_CHROMA_HOST, port=constants.EMBEDDING_CHROMA_PORT)
    embedding_function = embedding_functions.OllamaEmbeddingFunction(
        url=f"{constants.OLLAMA_BASE_URL}/api/embeddings",
        model_name=constants.EMBEDDING_MODEL,
    )
    collection = chroma_client.get_or_create_collection(
        name=constants.EMBEDDING_COLLECTION_NAME,
        embedding_function=embedding_function,
        metadata={"hnsw:space": "cosine"}
    )

    batch_size = constants.EMBEDDING_BATCH_SIZE
    while True:
        # Process rows in batches to avoid holding the entire partition in memory
        batch = list(islice(iterator, batch_size))
        if not batch:
            break

        # Filter out rows with null/empty IDs or documents before processing.
        valid_batch = [row for row in batch if row.id and row.document and row.document.strip()]
        
        if not valid_batch:
            print("Skipping batch as it contains no valid documents after filtering.", file=sys.stderr)
            continue

        ids_to_upsert = [row.id for row in valid_batch]
        documents_to_upsert = [row.document for row in valid_batch]
        metadatas_to_upsert = [
            # Filter out None values from metadata, as ChromaDB does not support them.
            {k: v for k, v in row.asDict().items() if k not in ['id', 'document'] and v is not None}
            for row in valid_batch
        ]

        print(f"Upserting batch of {len(ids_to_upsert)} documents from a partition.", file=sys.stderr)
        collection.upsert(documents=documents_to_upsert, metadatas=metadatas_to_upsert, ids=ids_to_upsert)


def main() -> None:
    '''
    Main function to stream aggregated data from Delta tables, transform it, and ingest it into ChromaDB.
    '''
    spark = get_spark_session(constants.EMBEDDING_APP_NAME)
    logger = get_logger(spark, "Embedding Layer Streaming")

    logger.info(EMBEDDING_BANNER)
    logger.info("--- Starting Embedding Layer Streaming Processing ---")
    
    checkpoint_path = os.path.join(os.path.dirname(constants.EMBEDDING_PATH), "checkpoints/embedding_streaming")

    try:
        # 1. Read from both gold Delta tables as streams
        top_complaints_stream = spark.readStream.format("delta").load(constants.EMBEDDING_PATH_TOP_COMPLAINTS)
        by_borough_stream = spark.readStream.format("delta").load(constants.EMBEDDING_PATH_BY_BOROUGH)

        # 2. Transform each stream to create documents and ensure schema compatibility
        
        # --- Create a mapping from numeric month to month name ---
        month_name_expression = (
            when(col("month") == 1, "January")
            .when(col("month") == 2, "February")
            .when(col("month") == 3, "March")
            .when(col("month") == 4, "April")
            .when(col("month") == 5, "May")
            .when(col("month") == 6, "June")
            .when(col("month") == 7, "July")
            .when(col("month") == 8, "August")
            .when(col("month") == 9, "September")
            .when(col("month") == 10, "October")
            .when(col("month") == 11, "November")
            .when(col("month") == 12, "December")
            .otherwise("Unknown Month")
        )

        # Example document: "In January of year 2023 the complaint type HEATING had 12345 total complaints."
        top_complaints_docs = top_complaints_stream.withColumn("month_name", month_name_expression).withColumn(
            "document", concat_ws(" ", lit("In"), col("month_name"), lit("of year"), col("year"), lit("the complaint type"), coalesce(col("complaint_type"), lit("unknown_complaint")), lit("had"), col("count"), lit("total complaints."))
        ).withColumn(
            "id", concat_ws("-", lit("complaint"), col("year"), col("month"), sha2(coalesce(col("complaint_type"), lit("unknown_complaint")), 256))
        ).withColumn("source", lit("top_complaints")).withColumn("borough", lit(None).cast("string"))

        # Example document: "In January of year 2023 the borough of STATEN ISLAND had 10636 total complaints"
        by_borough_docs = by_borough_stream.withColumn("month_name", month_name_expression).withColumn(
            "document", concat_ws(" ", lit("In"), col("month_name"), lit("of year"), col("year"), lit("the borough of"), coalesce(col("borough"), lit("unknown_borough")), lit("had"), col("count"), lit("total complaints"))
        ).withColumn(
            "id", concat_ws("-", lit("borough"), col("year"), col("month"), sha2(coalesce(col("borough"), lit("unknown_borough")), 256))
        ).withColumn("source", lit("by_borough")).withColumn("complaint_type", lit(None).cast("string"))

        # 3. Union the two streams into one (Simplified)
        all_docs_stream = top_complaints_docs.unionByName(by_borough_docs)

        # 4. Define the function to be executed on each micro-batch
        def upsert_micro_batch(micro_batch_df, batch_id):
            logger.info(f"--- Processing Micro-Batch ID: {batch_id} ---")
            if micro_batch_df.rdd.isEmpty():
                logger.info(f"Micro-Batch {batch_id} is empty, skipping.")
                return

            logger.info(f"Found {micro_batch_df.count()} new or updated documents to upsert into ChromaDB.")
            # Use foreachPartition for distributed ingestion of the micro-batch
            micro_batch_df.foreachPartition(_upsert_partition_to_chromadb)
            logger.info(f"--- Finished processing Micro-Batch ID: {batch_id} ---")

        # 5. Configure and start the streaming query
        logger.info(f"Starting stream to upsert embeddings. Checkpoint: {checkpoint_path}")
        query = (
            all_docs_stream.writeStream
            .foreachBatch(upsert_micro_batch)
            .outputMode("update")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True) # Use availableNow for batch-like execution, or processingTime for continuous
            .start()
        )

        query.awaitTermination()

    except Exception as e:
        logger.error(f"An error occurred during the embedding streaming process: {e}", exc_info=True)
        raise
    finally:
        logger.info("--- Embedding Layer Streaming Processing Finished ---")
        spark.stop()


if __name__ == "__main__":
    main()
