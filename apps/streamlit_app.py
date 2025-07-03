'''
    Streamlit UI for the AI-Powered NYC 311 Analyst
'''
import os
import streamlit as st
import requests

def main():
    '''
    The main entry point for the Streamlit application.
    This app provides a chat interface to the RAG API.
    '''
    st.set_page_config(page_title="NYC 311 Analyst", page_icon="🗽")
    st.title("🗽 AI-Powered NYC 311 Analyst")
    st.write(
        "Ask a question about NYC 311 service requests. "
        "The AI will analyze the data to find an answer using Retrieval-Augmented Generation."
    )

    # --- Configuration ---
    # Get API connection details directly from environment variables for clarity
    # and better container-based deployment.
    api_host = os.getenv("API_HOST", "localhost")
    api_port = int(os.getenv("API_PORT", 8001))
    api_url = f"http://{api_host}:{api_port}/stream_chat"

    # --- Chat History ---
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # --- User Input ---
    if prompt := st.chat_input("e.g., How many noise complaints were there in Brooklyn last year?"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # --- Call API and Stream Response ---
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""
            try:
                # Send the history, but exclude the latest user prompt which is passed separately
                history_to_send = st.session_state.messages[:-1]

                with requests.post(
                    api_url, json={"question": prompt, "chat_history": history_to_send}, stream=True, timeout=120
                ) as r:
                    r.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
                    for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
                        full_response += chunk
                        message_placeholder.markdown(full_response + "▌")
                message_placeholder.markdown(full_response)
            except requests.exceptions.RequestException as e:
                error_message = f"Failed to connect to the API at `{api_url}`. Please ensure the API service is running. Error: {e}"
                st.error(error_message)
                full_response = "Sorry, I couldn't connect to the analysis service. Please try again later."
        
        st.session_state.messages.append({"role": "assistant", "content": full_response})

if __name__ == "__main__":
    main()
