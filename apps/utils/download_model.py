"""
A simple script to pre-download the sentence transformer model.
This is intended to be run during the Docker image build process.
"""
from sentence_transformers import SentenceTransformer
from constant import constants

def download():
    """Downloads the model specified in the constants file."""
    model_name = constants.EMBEDDING_MODEL
    print(f"Downloading sentence transformer model: {model_name}...")
    # This line will download the model to the cache directory specified by
    # the TRANSFORMERS_CACHE environment variable.
    SentenceTransformer(model_name)
    print("Model download complete.")

if __name__ == "__main__":
    download()