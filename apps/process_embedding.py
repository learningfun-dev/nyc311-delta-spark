'''
    Process embedding layer
'''
import os
from typing import Optional
from itertools import islice
import chromadb
from chromadb.utils import embedding_functions
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, concat_ws, sha2, coalesce
from constant import constants
from pystyle import Colors, Colorate
from utils.spark_utils import get_spark_session

def _load_and_transform_data(spark: SparkSession) -> Optional[DataFrame]:
    """Loads gold layer data, transforms it for embedding, and returns a Spark DataFrame."""
    # Read data from Gold Delta Tables
    print(f"Embedding Layer: Reading 'top_complaints' from: {constants.EMBEDDING_PATH_TOP_COMPLAINTS}")
    if not os.path.exists(constants.EMBEDDING_PATH_TOP_COMPLAINTS):
        print(f"Error: Path does not exist - {constants.EMBEDDING_PATH_TOP_COMPLAINTS}")
        return None
    top_complaints_df = spark.read.format("delta").load(constants.EMBEDDING_PATH_TOP_COMPLAINTS)

    print(f"Embedding Layer: Reading 'by_borough' from: {constants.EMBEDDING_PATH_BY_BOROUGH}")
    if not os.path.exists(constants.EMBEDDING_PATH_BY_BOROUGH):
        print(f"Error: Path does not exist - {constants.EMBEDDING_PATH_BY_BOROUGH}")
        return None
    by_borough_df = spark.read.format("delta").load(constants.EMBEDDING_PATH_BY_BOROUGH)

    # Create descriptive sentences and deterministic IDs for embedding.
    top_complaints_docs = top_complaints_df.withColumn(
        "document",
        concat_ws(" ", lit("In month"), col("month"), lit("of year"), col("year"),
                  lit("the complaint type"), col("complaint_type"), lit("had"),
                  col("count"), lit("reports."))
    ).withColumn(
        "id",
        concat_ws("-", lit("complaint"), col("year"), col("month"),
                  sha2(coalesce(col("complaint_type"), lit("unknown_complaint")), 256))
    ).withColumn("source", lit("top_complaints"))

    by_borough_docs = by_borough_df.withColumn(
        "document",
        concat_ws(" ", lit("In month"), col("month"), lit("of year"), col("year"),
                  lit("the borough of"), coalesce(col("borough"), lit("unknown_borough")), lit("had"),
                  col("count"), lit("total 311 reports."))
    ).withColumn(
        "id",
        concat_ws("-", lit("borough"), col("year"), col("month"),
                  sha2(coalesce(col("borough"), lit("unknown_borough")), 256))
    ).withColumn("source", lit("by_borough"))

    print("Embedding Layer: Transformed data into document format with IDs and metadata.")
    
    # Union dataframes and collect to driver
    top_complaints_final = top_complaints_docs.withColumn("borough", lit(None).cast("string"))
    by_borough_final = by_borough_docs.withColumn("complaint_type", lit(None).cast("string"))

    all_docs_df = top_complaints_final.select(
        "id", "document", "year", "month", "count", "complaint_type", "borough", "source"
    ).unionByName(by_borough_final.select(
        "id", "document", "year", "month", "count", "complaint_type", "borough", "source"
    ))

    return all_docs_df


def _ingest_partition_to_chromadb(iterator: iter) -> None:
    """
    Initializes a ChromaDB client and ingests a partition of data in batches.
    This function is intended to be used with Spark's foreachPartition.
    """
    
    # Initialize client and embedding function on the executor
    chroma_client = chromadb.HttpClient(host=constants.EMBEDDING_CHROMA_HOST, port=constants.EMBEDDING_CHROMA_PORT)
    sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=constants.EMBEDDING_MODEL)
    collection = chroma_client.get_or_create_collection(
        name=constants.EMBEDDING_COLLECTION_NAME,
        embedding_function=sentence_transformer_ef,
        metadata={"hnsw:space": "cosine"}
    )

    batch_size = constants.EMBEDDING_BATCH_SIZE
    while True:
        # Process rows in batches to avoid holding the entire partition in memory
        batch = list(islice(iterator, batch_size))
        if not batch:
            break

        ids_to_ingest = [row.id for row in batch]
        documents_to_ingest = [row.document for row in batch]
        metadatas_to_ingest = []
        for row in batch:
            metadata_dict = row.asDict()
            del metadata_dict['id']
            del metadata_dict['document']
            # ChromaDB metadata values cannot be None. Filter out keys with None values.
            filtered_metadata = {k: v for k, v in metadata_dict.items() if v is not None}
            metadatas_to_ingest.append(filtered_metadata)

        print(f"Ingesting batch of {len(ids_to_ingest)} documents from a partition.")
        collection.add(documents=documents_to_ingest, metadatas=metadatas_to_ingest, ids=ids_to_ingest)


def main() -> None:
    '''
    Main function to read aggregated data from Delta tables, transform it, and ingest it into ChromaDB.
    '''
    print(Colorate.Vertical(Colors.blue_to_green, """


   ▄████████   ▄▄▄▄███▄▄▄▄   ▀█████████▄     ▄████████ ████████▄  ████████▄   ▄█  ███▄▄▄▄      ▄██████▄        ▄█          ▄████████ ▄██   ▄      ▄████████    ▄████████ 
  ███    ███ ▄██▀▀▀███▀▀▀██▄   ███    ███   ███    ███ ███   ▀███ ███   ▀███ ███  ███▀▀▀██▄   ███    ███      ███         ███    ███ ███   ██▄   ███    ███   ███    ███ 
  ███    █▀  ███   ███   ███   ███    ███   ███    █▀  ███    ███ ███    ███ ███▌ ███   ███   ███    █▀       ███         ███    ███ ███▄▄▄███   ███    █▀    ███    ███ 
 ▄███▄▄▄     ███   ███   ███  ▄███▄▄▄██▀   ▄███▄▄▄     ███    ███ ███    ███ ███▌ ███   ███  ▄███             ███         ███    ███ ▀▀▀▀▀▀███  ▄███▄▄▄      ▄███▄▄▄▄██▀ 
▀▀███▀▀▀     ███   ███   ███ ▀▀███▀▀▀██▄  ▀▀███▀▀▀     ███    ███ ███    ███ ███▌ ███   ███ ▀▀███ ████▄       ███       ▀███████████ ▄██   ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀   
  ███    █▄  ███   ███   ███   ███    ██▄   ███    █▄  ███    ███ ███    ███ ███  ███   ███   ███    ███      ███         ███    ███ ███   ███   ███    █▄  ▀███████████ 
  ███    ███ ███   ███   ███   ███    ███   ███    ███ ███   ▄███ ███   ▄███ ███  ███   ███   ███    ███      ███▌    ▄   ███    ███ ███   ███   ███    ███   ███    ███ 
  ██████████  ▀█   ███   █▀  ▄█████████▀    ██████████ ████████▀  ████████▀  █▀    ▀█   █▀    ████████▀       █████▄▄██   ███    █▀   ▀█████▀    ██████████   ███    ███ 
                                                                                                              ▀                                               ███    ███ 

    """, 1))

    print("{:*<120}".format("*"))
    print("{: ^120}".format("4. EMBEDDING LAYER PROCESSING: Ingest Data into ChromaDB"))
    print("{:*<120}".format("*"))

    spark = get_spark_session(constants.EMBEDDING_APP_NAME)
    try:
        all_docs_df = _load_and_transform_data(spark)
        if all_docs_df is not None:
            # Use foreachPartition to process data on each worker node
            print("Starting distributed ingestion into ChromaDB...")
            all_docs_df.foreachPartition(_ingest_partition_to_chromadb)
            print('{:-^100}'.format("Embedding Layer: Data Ingestion Complete!"))

            # Verify final count by querying ChromaDB directly from the driver
            print("Verifying total document count in ChromaDB...")
            chroma_client = chromadb.HttpClient(host=constants.EMBEDDING_CHROMA_HOST, port=constants.EMBEDDING_CHROMA_PORT)
            collection = chroma_client.get_collection(name=constants.EMBEDDING_COLLECTION_NAME)
            print("{:*<120}".format("*"))
            print(f"Total documents in dataframes: {all_docs_df.count()}")
            print(f"Total documents in collection '{constants.EMBEDDING_COLLECTION_NAME}': {collection.count()}")
            print("{:*<120}".format("*"))

    except Exception as e:
        print(f"An error occurred during the embedding process: {e}")
        raise e
    finally:
        print("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    main()
