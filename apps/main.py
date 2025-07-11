"""
    API to serve the RAG 2.0 application
"""
import json
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import chromadb
from chromadb.config import Settings
from langchain_chroma import Chroma
from langchain_ollama import OllamaLLM
from langchain_ollama import OllamaEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from .constant import constants

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
    Connects to the existing ChromaDB instance using settings from the constants.
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
        embedding_function=embedding_function,
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

# Initialize components
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
llm = OllamaLLM(model=constants.LOCAL_LLM_MODEL, base_url=constants.OLLAMA_BASE_URL)


# --- Chains Definition ---

# Rephrasing prompt to include document structure examples.
REPHRASE_TEMPLATE = """
You are a search query rewriter. Your only job is to rephrase a question into a standalone, descriptive sentence for a vector database search.
Follow the examples below exactly. Do not add any conversational text or explanations.

---
EXAMPLES:

User's Question: how many heat/hot water complaints in jan 2023?
Standalone Search Query: Reports for HEAT/HOT WATER complaints in month 1 of year 2023.

User's Question: what about brooklyn?
Standalone Search Query: Reports for complaints in the borough of BROOKLYN.

User's Question: show me the top complaints for 2024
Standalone Search Query: Top complaints for the year 2024.
---

Chat History:
{chat_history}

User's Question:
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

# Final answer prompt and chain
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

# The full RAG chain is restored using RunnableParallel.
# This ensures that the 'context' from the retriever and the original 'question'
# and 'chat_history' are all passed to the final answer prompt correctly.
rag_chain = (
    RunnableParallel(
        {
            "context": rephrase_chain | retriever | format_docs,
            "question": lambda x: x["question"],
            "chat_history": lambda x: format_chat_history(x["chat_history"]),
        }
    )
    | answer_prompt
    | llm
    | StrOutputParser()
)

# A separate chain to get the source documents for debugging/display
source_retrieval_chain = rephrase_chain | retriever

@app.post("/stream_chat")
async def stream_chat(request: QueryRequest):
    """
    Handles the chat request by first rephrasing the question, retrieving documents,
    sending both to the client, and then streaming the final answer.
    """
    chat_history_dicts = [item.dict() for item in request.chat_history]
    chain_input = {"question": request.question, "chat_history": chat_history_dicts}

    async def response_generator():
        try:
            # 1. Rephrase the question to be a standalone query
            rephrased_question = await rephrase_chain.ainvoke(chain_input)
            yield json.dumps({"rephrased_question": rephrased_question}) + "\n"

            # 2. Retrieve documents using the rephrased question
            retrieved_docs = await retriever.ainvoke(rephrased_question)
            
            source_documents_json = [
                {"page_content": doc.page_content, "metadata": doc.metadata} 
                for doc in retrieved_docs
            ]
            yield json.dumps({"source_documents": source_documents_json}) + "\n"

            # 3. Stream the final answer using the full, correct rag_chain
            async for chunk in rag_chain.astream(chain_input):
                yield json.dumps({"answer_chunk": chunk}) + "\n"

        except Exception as e:
            print(f"ERROR in stream_chat: {e}")
            error_payload = json.dumps({"error": str(e)}) + "\n"
            yield error_payload

    return StreamingResponse(response_generator(), media_type="text/plain")


@app.get("/")
def read_root():
    """
    Root endpoint for the API.
    """
    return {"message": "NYC 311 RAG 2.0 API is running. Use the /stream_chat endpoint."}
