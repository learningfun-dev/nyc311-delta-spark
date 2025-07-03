"""
    API to serve the RAG 2.0 application
"""
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import chromadb
from chromadb.config import Settings
from langchain_chroma import Chroma
from langchain_ollama import OllamaLLM
from langchain_ollama import OllamaEmbeddings # <-- MODIFIED: New import for Ollama embeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel
from langchain_core.output_parsers import StrOutputParser
from .constant import constants
from starlette.responses import StreamingResponse

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    A lifespan function to log configuration on startup.
    This helps verify that environment variables are loaded correctly.
    """
    print("\n--- API Lifespan Start ---")
    print(f"ChromaDB Host: {constants.EMBEDDING_CHROMA_HOST}")
    print(f"ChromaDB Port: {constants.EMBEDDING_CHROMA_PORT}")
    print(f"Ollama Base URL: {constants.OLLAMA_BASE_URL}")
    print("--------------------------\n")
    yield
    print("\n--- API Lifespan End ---\n")

# --- FastAPI App Initialization ---
app = FastAPI(
    title=constants.API_TITLE,
    description=constants.API_DESCRIPTION,
    version=constants.API_VERSION,
    lifespan=lifespan
)

class ChatHistoryItem(BaseModel):
    """Pydantic model for a single item in the chat history."""
    role: str
    content: str

class QueryRequest(BaseModel):
    """
    Pydantic model for the request body.
    """
    question: str
    chat_history: List[ChatHistoryItem] = []

# --- LangChain and ChromaDB Setup ---
def get_retriever():
    """
    Initializes and returns a Chroma vector store retriever.
    Connects to the existing ChromaDB instance using settings from the constants file.
    """
    chroma_client = chromadb.HttpClient(
        host=constants.EMBEDDING_CHROMA_HOST, 
        port=constants.EMBEDDING_CHROMA_PORT,
        settings=Settings(anonymized_telemetry=False)
    )

    # Use embedding model from ollama (locally)
    embedding_function = OllamaEmbeddings(
        model=constants.EMBEDDING_MODEL,
        base_url=constants.OLLAMA_BASE_URL
    )
    

    vector_store = Chroma(
        client=chroma_client,
        collection_name=constants.EMBEDDING_COLLECTION_NAME,
        embedding_function=embedding_function, # Use the new Ollama embedding function
    )
    
    return vector_store.as_retriever(search_kwargs={"k": 5})

def format_docs(docs):
    """
    Formats the retrieved documents into a single string for the context.
    """
    return "\n\n".join(doc.page_content for doc in docs)

def format_chat_history(chat_history: List[dict]) -> str:
    """
    Formats the chat history into a readable string for the prompt context.
    """
    if not chat_history:
        return "No previous conversation."
    return "\n".join(f"{msg['role'].capitalize()}: {msg['content']}" for msg in chat_history)


# Initialize components from constants
retriever = get_retriever()

# --- LLM Initialization ---
# The current setup uses a local Ollama instance for development.
# To deploy this to production, I would choose one of the following options:
#
# --- Option 1: Use a Managed LLM Service (e.g., OpenAI, Google, Anthropic) ---
#   - Why I'd choose this: It's the fastest way to get a scalable, production-ready LLM without managing infrastructure.
#   - What I'd need to do:
#     1. Uncomment the following lines.
#     2. Get an API key from the provider.
#     3. Replace the OllamaLLM line below with this code.
#
#     # from langchain_openai import ChatOpenAI
#     # llm = ChatOpenAI(model="gpt-4o", openai_api_key="OUR_API_KEY")
#
# --- Option 2: Self-Host an Open-Source LLM (e.g., on AWS SageMaker) ---
#   - Why I'd choose this: For maximum control over the model, data privacy, and to potentially lower costs.
#   - What I'd need to do:
#     1. Deploy an open-source model (like Llama 3) to a cloud service to get a private API endpoint.
#     2. Uncomment the following lines.
#     3. Replace the OllamaLLM line below with this code.
#
#     # from langchain_community.llms import HuggingFaceEndpoint
#     # llm = HuggingFaceEndpoint(
#     #     endpoint_url="YOUR_SAGEMAKER_ENDPOINT_URL",
#     #     huggingfacehub_api_token="YOUR_HF_TOKEN"
#     # )
#
# --- Current Setup (for local development) ---
llm = OllamaLLM(
    model=constants.LOCAL_LLM_MODEL,
    base_url=constants.OLLAMA_BASE_URL
)

# Rephrasing Prompt and Chain
# This chain takes the user's question and rephrases it to be more specific for vector search.
REPHRASE_TEMPLATE = """
Given the following conversation history and a follow-up question, rephrase the follow-up question to be a standalone query that is optimized for a vector database search.
The standalone query should be self-contained and not require the conversation history to be understood.

Chat History:
{chat_history}

Follow-up Question:
{question}

Standalone Search Query:
"""
rephrase_prompt = ChatPromptTemplate.from_template(REPHRASE_TEMPLATE)

# This chain now takes the user's question and chat history and rephrases it.
rephrase_chain = (
    {
        "question": lambda x: x["question"],
        "chat_history": lambda x: format_chat_history(x["chat_history"]),
    }
    | rephrase_prompt
    | llm
    | StrOutputParser()
)


# Final Answer Prompt
# This chain uses the conversation history and the retrieved context to generate the final answer.
ANSWER_TEMPLATE = """
You are an expert AI assistant for analyzing NYC 311 service request data.
Answer the user's question based on the conversation history and the following context.
If the context does not contain the answer, state that you don't have enough information.
Do not use any prior knowledge.

Context:
{context}

Chat History:
{chat_history}

Question:
{question}

Answer:
"""
answer_prompt = ChatPromptTemplate.from_template(ANSWER_TEMPLATE)

# The Full RAG Chain with Conversational Memory
# This LCEL chain orchestrates the entire process:
#   a. It takes the original question and chat history.
#   b. It runs the rephrasing chain to generate a standalone query for retrieval.
#   c. It uses the rephrased query to retrieve documents.
#   d. It passes the retrieved context, original question, and chat history to the final LLM call.

# This setup runs the retrieval chain in parallel with passing through the original question and history.
setup_and_retrieval = RunnableParallel(
    {
        "context": rephrase_chain | retriever | format_docs,
        "question": lambda x: x["question"],
        "chat_history": lambda x: format_chat_history(x["chat_history"]),
    }
)

rag_chain = (
    setup_and_retrieval
    | answer_prompt
    | llm
    | StrOutputParser()
)

@app.post("/stream_chat")
async def stream_chat(request: QueryRequest):
    try:
        chat_history_dicts = [item.dict() for item in request.chat_history]
        chain_input = {"question": request.question, "chat_history": chat_history_dicts}

        async def wrapped_generator():
            async for chunk in rag_chain.astream(chain_input):
                yield chunk + "\n"
        
        return StreamingResponse(wrapped_generator(), media_type="text/plain")

    except Exception as e:
        print(f"ERROR: {e}")
        raise e

@app.get("/")
def read_root():
    """
    Root endpoint for the API.
    """
    return {"message": "NYC 311 RAG 2.0 API is running. Use the /stream_chat endpoint."}
