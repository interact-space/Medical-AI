import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

def get_llm():
    mode = os.getenv("LLM_MODE", "local")
    if mode == "local":
        base_url = os.getenv("LLM_BASE_URL", "http://127.0.0.1:11434/v1")
        api_key  = os.getenv("LLM_API_KEY", "ollama")
        model    = os.getenv("LLM_MODEL", "llama3:latest")
        client = OpenAI(base_url=base_url, api_key=api_key)
        return client, model
    else:
        api_key  = os.getenv("LLM_API_KEY")
        model    = os.getenv("LLM_MODEL", "gpt-4o-mini")
        client = OpenAI(api_key=api_key)
        return client, model
