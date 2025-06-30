from langchain.agents import initialize_agent, AgentType
from langchain.prompts import SystemMessagePromptTemplate, MessagesPlaceholder, HumanMessagePromptTemplate
from langchain.prompts.chat import ChatPromptTemplate
from langchain.tools import tool
from langchain_openai import AzureChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import InMemoryChatMessageHistory
from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate , HumanMessagePromptTemplate
from pydantic import BaseModel, Field, field_validator
from agents.base import BaseAgent
from openai import AzureOpenAI
from typing import List
from typing import Optional
from datetime import datetime
import time
import logging
from openai import RateLimitError
from dotenv import load_dotenv
import os
import json
import streamlit as st
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from Dataloader import *
from agents.tools import *
from collections import defaultdict

load_dotenv()

def detect_local_environment():
    """Detect if running locally or in production."""
    return os.getenv("RUNNING_IN_PRODUCTION") != "true"


global_chat_map = {}
def get_chat_history(session_id: str, use_streamlit: bool = True) -> InMemoryChatMessageHistory:
    """
    Get memory based on environment.
    If use_streamlit=True and Streamlit is available, use st.session_state.
    Else, use in-memory global storage.
    """
    if use_streamlit:
        try:
            import streamlit as st
            if session_id not in st.session_state:
                st.session_state[session_id] = InMemoryChatMessageHistory()
            return st.session_state[session_id]
        except ImportError:
            pass  # fallback

    if session_id not in global_chat_map:
        global_chat_map[session_id] = InMemoryChatMessageHistory()
    return global_chat_map[session_id]

    
class ExtractorAgent(BaseAgent):

    def __init__(self,vector_database , use_streamlit = True):
        super().__init__()
        self.llm = AzureChatOpenAI(
                    azure_endpoint = os.getenv("AZURE_ENDPOINT"),
                    api_key = os.getenv("OPENAI_API_KEY"),
                    api_version="2024-08-01-preview",
                    azure_deployment="gpt-4o-mini",
                    temperature=0.0,
                )
        self.embedding_model = AzureOpenAI(azure_endpoint = os.getenv("AZURE_ENDPOINT"), api_key = os.getenv("OPENAI_API_KEY"), api_version = "2024-08-01-preview")

        self.use_streamlit = use_streamlit
        self.tool = make_search_entities_tool(self.embedding_model, vector_database)
        self.context_agent = initialize_agent(tools = [self.tool], llm = self.llm, agent=AgentType.OPENAI_FUNCTIONS,verbose=True)
        self.system_prompt = self.get_system_prompt()
        
    def get_system_prompt(self) -> SystemMessagePromptTemplate:
        return SystemMessagePromptTemplate.from_template("""
        You are an Entity Extraction and Context Understanding Agent in a multi-agent system.
        Your job is to extract potential business entity names from the user's question and call the tool `search_entities_in_vdb` with those names.

        Instructions:
        Extract proper business entity names from the user's question. These include:
        - Brand names (e.g., פרינגלס)
        - Retail chains (e.g., רמי לוי, שופרסל)
        - Customers (e.g , רשת חנויות רמי לוי שיווק השיקמה)
        - Distributors or suppliers (e.g., דיפלומט)
        - Specific product names (multi-word)
        - Keep compound names intact (e.g., "פרינגלס שמנת בצל").
        - DO NOT include words like 'customer', 'profit', 'רווח', 'לקוח' or generic descriptors.
        - If any extracted name is written in English or other non-Hebrew languages, translate it into Hebrew before calling the tool.
        - Call the tool `search_entities_in_vdb` with the final list of names.

        Tool Behavior:
        - Accepts a list of Hebrew names
        - Returns up to 5 best matches for each name including type, score, and metadata

        Your output MUST be
        {{
          "ExtractedNames": [...],
          "EntityCandidates": raw results from the tool
        }}

        """)


    def response(self, user_question: str, max_retries: int = 5, delay: float = 1.0) -> dict:
        prompt_template = ChatPromptTemplate.from_messages([
            self.system_prompt,
            MessagesPlaceholder(variable_name="history"),
            HumanMessagePromptTemplate.from_template("{user_question}")
        ])

        pipeline = prompt_template | self.context_agent
    
        pipeline_with_history = RunnableWithMessageHistory(
            pipeline,
            get_session_history=lambda session_id: get_chat_history(session_id, self.use_streamlit),
            history_messages_key="history",
            input_messages_key="user_question"
        )
        self.reset_fields()
        self.set_question(user_question)
        self.start_timer()

        last_error = None
        for attempt in range(1, max_retries + 1):
            self.increment_attempts()
            try:
                self.increment_calls()
                answer = pipeline_with_history.invoke({"user_question": user_question},config={"session_id": 'ExtractorHistory'})
                self.stop_timer()
                self.set_answer(answer['output'])
                return answer['output']
            
            except RateLimitError as e:
                logging.warning(f"[Retry {attempt}/{max_retries}] Rate limit error: {e}")
                self.set_error(e)

            except Exception as e:
                logging.error(f"[Attempt {attempt}] Unexpected error: {e}")
                self.set_error(str(e))
                self.stop_timer()
                raise e  # for other exceptions, fail immediately

        self.stop_timer()
        # if we reached here, all retries failed
        raise RuntimeError(f"Failed after {max_retries} retries due to rate limit. Last error: {last_error}")
    
    