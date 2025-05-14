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

load_dotenv()

def detect_local_environment():
    """Detect if running locally or in production."""
    return os.getenv("RUNNING_IN_PRODUCTION") != "true"

def get_chat_history() -> InMemoryChatMessageHistory:
    return st.session_state['ExtractorHistory']

    
class ExtractorAgent(BaseAgent):

    def __init__(self):
        super().__init__()
        self.llm = AzureChatOpenAI(
                    azure_endpoint = os.getenv("AZURE_ENDPOINT"),
                    api_key = os.getenv("OPENAI_API_KEY"),  # או פשוט המחרוזת עצמה אם אתה לא רוצה ENV
                    api_version="2024-08-01-preview",
                    azure_deployment="Diplochat",
                    temperature=0.0,
                )
        
        self.tools = [identify_entity_type]
        self.context_agent = initialize_agent(tools = self.tools,llm = self.llm, agent=AgentType.OPENAI_FUNCTIONS,verbose=True)
        self.system_prompt = self.get_system_prompt()
        

    
    def get_system_prompt(self) -> SystemMessagePromptTemplate:
        return SystemMessagePromptTemplate.from_template(
                """
                You are a Context Extraction and Reasoning Agent.

                Your job is to:
                1. Extract structured entities mentioned in the user question.
                2. Understand the user's analytical intent (Meaning).
                3. Classify the question into a known analytical category (Question_Type).

                ---

                Your output must be a valid Python dictionary with these keys:
                - "Entities": A list of dictionaries with the following fields:
                    - type: One of ("Item_Name", "Brand_Name", "Category_Name", "Supplier_Name", "Holiday")
                    - name: The matched name from the dataset (if found by the tool `identify_entity_type`), or the original input.
                    - category: If available; otherwise do not include.
                    - metadata: Optional additional notes if relevant (e.g., "multiple items matched").

                - "Meaning": A short plain-English description of what the user wants to analyze.  
                - Be specific: Describe whether a percentage, comparison table, or total value is expected.
                - Include all relevant entity names (without translation or modification). Unless otherwise determined through your use of the 'identify_entity_type' tool

                - "Question_Type": One of the following:
                    - "Market_Share_Brand"
                    - "Market_Share_Item"
                    - "Market_Share_Brand_Excluding_Supplier"
                    - "Market_Share_Competitors"
                    - "Market_Share_Supplier"
                    - "Top_Selling_Items"
                    - "Average_Price_By_Chain"
                    - "Custom" (if none of the above apply)

                ---

                Instructions:

                - If a supplier is explicitly mentioned in a negative context (e.g., "excluding דיפלומט"), use the `_Excluding_Supplier` version of the Question_Type.
                - If the question compares דיפלומט to others, use "Market_Share_Competitors".
                - If the intent is ambiguous but involves a brand or product, default to "Market_Share_Brand".
                - Never translate any name or phrase.
                - If you are unsure about the entity type, use the tool `identify_entity_type`.
                - If no valid entities are found, still return "Meaning" and "Question_Type" based on the user's intent.

                Always return a valid Python dictionary.
                """
                )

    def response(self, user_question: str, max_retries: int = 5, delay: float = 1.0) -> dict:
        prompt_template = ChatPromptTemplate.from_messages([
            self.system_prompt,
            MessagesPlaceholder(variable_name="history"),
            HumanMessagePromptTemplate.from_template("{user_question}")
        ])

        pipeline = prompt_template | self.context_agent
    
        pipeline_with_history = RunnableWithMessageHistory(
            pipeline,
            get_session_history=get_chat_history,
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
                context = json.loads(answer['output'])
                context = json.dumps(context, ensure_ascii=False, separators=(",", ":"))
                self.set_answer(context)
                return context
            
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
    


@tool
def identify_entity_type(entity_name: str) -> dict:
    """ Smart identification of entity type. """
    entity_name = entity_name.strip()

    if detect_local_environment():
        stnx_items = pd.read_parquet("parquet_files/DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS.parquet")
    else:
        stnx_items = st.session_state['Dataframes']['stnx_items']

    result = {
        "type": "Unknown",
        "name": entity_name,
        "category": None,
        "metadata": None
    }
    print(entity_name)
    # --- 1. Exact match in Category_Name ---
    if entity_name in stnx_items['Category_Name'].dropna().unique():
        print("exact math in category")
        result["type"] = "Category_Name"
        result["category"] = entity_name
        return result

    # --- 2. Exact match in Brand_Name ---
    if entity_name in stnx_items['Brand_Name'].dropna().unique():
        print("exact math in brand name")
        result["type"] = "Brand_Name"
        cat = stnx_items.loc[stnx_items['Brand_Name'] == entity_name, 'Category_Name'].dropna().unique()
        if len(cat) > 0:
            result["category"] = cat[0]
        return result

    # --- 3. Exact match in Item_Name ---
    if entity_name in stnx_items['Item_Name'].dropna().unique():
        print("exact math in item name")
        result["type"] = "Item_Name"
        cat = stnx_items.loc[stnx_items['Item_Name'] == entity_name, 'Category_Name'].dropna().unique()
        if len(cat) > 0:
            result["category"] = cat[0]
        return result

    # --- 4. Partial match in Category_Name ---
    partial_cat = stnx_items[stnx_items['Category_Name'].str.contains(entity_name, na=False)]
    print("partial math in category")
    if not partial_cat.empty:
        result["type"] = "Category_Name"
        result["name"] = partial_cat.iloc[0]['Category_Name']
        result["category"] = partial_cat.iloc[0]['Category_Name']
        return result

    # --- 5. Partial match in Brand_Name ---
    partial_brand = stnx_items[stnx_items['Brand_Name'].str.contains(entity_name, na=False)]
    if not partial_brand.empty:
        print("partial math in brand")
        result["type"] = "Brand_Name"
        result["name"] = partial_brand.iloc[0]['Brand_Name']
        cat = partial_brand['Category_Name'].dropna().unique()
        if len(cat) > 0:
            result["category"] = cat[0]
        return result

    # --- 6. Partial match in Item_Name ---
    partial_item = stnx_items[stnx_items['Item_Name'].str.contains(entity_name, na=False)]
    if not partial_item.empty:
        print("partial math in item")
        result["type"] = "Item_Name"
        result["name"] = partial_item.iloc[0]['Item_Name']
        cat = partial_item['Category_Name'].dropna().unique()
        if len(cat) > 0:
            result["category"] = cat[0]
        count = partial_item['Barcode'].nunique()
        if count > 1:
            result["metadata"] = f"There are {count} items that contain that phrase"
        return result

    # --- 7. Exact match in Supplier_Name ---
    if entity_name in stnx_items['Supplier_Name'].dropna().unique():
        result = {
            "type": "Supplier_Name",
            "name": entity_name,
            "category": None
        }
        return result

    return result

