'''
    Streamlit UI for the AI-Powered NYC 311 Analyst
'''
import os
import json
import sys
import streamlit as st
import requests

def main():
    '''
    The main entry point for the Streamlit application.
    This app provides a chat interface to the RAG API and displays
    both the AI-generated answer and the source documents used.
    '''
    st.set_page_config(page_title="NYC 311 Analyst", page_icon="🗽")
    
    # --- Configuration and Connection ---
    try:
        api_host = os.getenv("API_HOST", "localhost")
        api_port = int(os.getenv("API_PORT", 8001))
        api_url = f"http://{api_host}:{api_port}/stream_chat"
    except (ValueError, TypeError) as e:
        st.error(
            f"Configuration Error: Could not parse environment variables. "
            f"Please ensure API_HOST and API_PORT are set correctly.\n\n"
            f"Error: {e}"
        )
        st.stop()

    st.title("🗽 AI-Powered NYC 311 Analyst")
    st.write(
        "Ask a question about NYC 311 service requests. "
        "The AI will analyze the data to find an answer using Retrieval-Augmented Generation."
    )

    # --- Chat History Initialization ---
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # --- Display Past Messages ---
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            # If the assistant message has source documents, display them in an expander
            if "sources" in message and message["sources"]:
                with st.expander("Retrieved Context"):
                    # **FIX**: Display the rephrased question that was used for retrieval
                    if "rephrased_question" in message:
                        st.write("**Rephrased Query for Retrieval:**")
                        st.info(message["rephrased_question"])
                    
                    st.write("**Retrieved Documents:**")
                    for i, source in enumerate(message["sources"]):
                        # **FIX**: Use 'page_content' which matches the API response key
                        st.info(f"**Source {i+1}**: {source.get('page_content', 'No document text available.')}")

    # --- Handle New User Input ---
    if prompt := st.chat_input("e.g., How many noise complaints were there in Brooklyn last year?"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # --- Call API and Stream Response ---
        with st.chat_message("assistant"):
            answer_placeholder = st.empty()
            full_response = ""
            retrieved_sources = []
            rephrased_question_for_display = "" # To store the rephrased question
            json_decode_errors = []

            try:
                history_to_send = [
                    {"role": msg["role"], "content": msg["content"]} 
                    for msg in st.session_state.messages[:-1]
                ]

                with requests.post(
                    api_url, json={"question": prompt, "chat_history": history_to_send}, stream=True, timeout=120
                ) as r:
                    r.raise_for_status()
                    
                    for line in r.iter_lines(decode_unicode=True):
                        if line:
                            try:
                                data = json.loads(line)
                                if "answer_chunk" in data:
                                    full_response += data["answer_chunk"]
                                    answer_placeholder.markdown(full_response + "▌")
                                if "source_documents" in data:
                                    retrieved_sources = data["source_documents"]
                                # **FIX**: Capture the rephrased question
                                if "rephrased_question" in data:
                                    rephrased_question_for_display = data["rephrased_question"]
                            except json.JSONDecodeError:
                                json_decode_errors.append(line)
                                print(f"Warning: Could not decode JSON line from stream: {line}", file=sys.stderr)
                
                answer_placeholder.markdown(full_response)
                
                if json_decode_errors:
                    st.error(
                        "The API returned data in an unexpected format. This usually indicates an error in the backend service. "
                        "Here are the raw lines that could not be parsed:"
                    )
                    st.code("\n".join(json_decode_errors), language="text")

                if rephrased_question_for_display or retrieved_sources:
                    with st.expander("Retrieved Context"):
                        if rephrased_question_for_display:
                            st.write("**Rephrased Query for Retrieval:**")
                            st.info(rephrased_question_for_display)
                        
                        if retrieved_sources:
                            st.write("**Retrieved Documents:**")
                            for i, source in enumerate(retrieved_sources):
                                # **FIX**: Use 'page_content' which matches the API response key
                                st.info(f"**Source {i+1}**: {source.get('page_content', 'No document text available.')}")

            except requests.exceptions.RequestException as e:
                error_message = f"Failed to connect to the API at `{api_url}`. Please ensure the API service is running. Error: {e}"
                st.error(error_message)
                full_response = "Sorry, I couldn't connect to the analysis service. Please try again later."
        
        # Add the complete assistant response to history for redisplay
        st.session_state.messages.append({
            "role": "assistant", 
            "content": full_response,
            "sources": retrieved_sources,
            "rephrased_question": rephrased_question_for_display
        })

if __name__ == "__main__":
    main()
