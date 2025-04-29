from langchain.agents import initialize_agent, AgentType
from langchain.prompts import SystemMessagePromptTemplate, MessagesPlaceholder, HumanMessagePromptTemplate
from langchain.prompts.chat import ChatPromptTemplate
from langchain.tools import tool
from langchain_openai import AzureChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate , HumanMessagePromptTemplate
from pydantic import BaseModel, Field, field_validator
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


class ExtractorAgent:

    def __init__(self):
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
            You are a Context Extraction Agent.

            Your task is to extract structured insights from the user's question without translating or modifying any entity names.

            Your output must be a Python dictionary with exactly two keys:

            1. "Entities": A list of dictionaries. Each dictionary must have:
                - type: (Item_Name, Brand_Name, Category_Name, Supplier_Name, Holiday)
                - name: (preserve exactly the user's input text)
                - category: (if available; otherwise null)

                Use the 'identify_entity_type' tool if you are unsure about the entity type.

                If the result from the tool is "Unknown":
                - Do not add that entity to the "Entities" list.
                - However, still analyze the user's question and produce a "Meaning" description based on the context.

                Rules:
                - When the user input contains multiple words:
                    - First try to match the entire phrase (all words together) in the Item_Name dataset.
                    - Only if no full match is found, try to match partial words individually.
                - Always prefer accuracy over guessing.
                - Always include the 'category' field if available, even if the entity type is Category_Name itself.

            2. "Meaning": A clear English description of the user's question.
                - Always mention entities exactly as they appear, in the original language.
                - If asking about a single item or brand, mention that the result should be a percentage.
                - If asking about competitors or multiple items:
                    - Mention that the result should be a table with relevant columns.
                    - Explicitly state that each competitor (based on Supplier_Name) should be listed as a separate row in the table.

            Important:
            - Never translate names.
            - Never omit output format expectations from the Meaning, even if no Entities are detected.
            - Always return valid Python dictionary syntax.
            """
        )

    def response(self, user_question: str, max_retries: int = 5, delay: float = 1.0) -> dict:
        
        prompt_template = ChatPromptTemplate.from_messages([
            self.system_prompt,
            HumanMessagePromptTemplate.from_template("{input}")
        ])

        full_chain = prompt_template | self.context_agent
    
        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                response = full_chain.invoke({"input": user_question})
                context = json.dumps(response['output'], ensure_ascii=False, indent=2)
                return context
            except RateLimitError as e:
                last_error = e
                logging.warning(f"[Retry {attempt}/{max_retries}] Rate limit error: {e}")
                time.sleep(delay * attempt)  # exponential backoff
            except Exception as e:
                logging.error(f"[Attempt {attempt}] Unexpected error: {e}")
                raise e  # for other exceptions, fail immediately

        # if we reached here, all retries failed
        raise RuntimeError(f"Failed after {max_retries} retries due to rate limit. Last error: {last_error}")
    



@tool
def identify_entity_type(entity_name: str) -> dict:
    """
    Smart identification of entity type.
    Priority:
    1. Exact match in Category_Name
    2. Exact match in Brand_Name
    3. Partial match in Category_Name
    4. Partial match in Brand_Name
    5. Partial match in Item_Name
    """
    stnx_items = pd.read_parquet("parquet_files/DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS.parquet")
    a = entity_name
    entity_name = entity_name.strip()

    result = {
        "type": "Unknown",
        "name": entity_name,
        "category": None
    }

    print(a)
    # 1. Exact match in Category_Name
    if entity_name in stnx_items['Category_Name'].dropna().unique():
        result["type"] = "Category_Name"
        result["category"] = entity_name
        return result

    # 2. Exact match in Brand_Name
    if entity_name in stnx_items['Brand_Name'].dropna().unique():
        result["type"] = "Brand_Name"
        cat = stnx_items.loc[stnx_items['Brand_Name'] == entity_name, 'Category_Name'].dropna().unique()
        if len(cat) > 0:
            result["category"] = cat[0]
        return result

    # 3. Partial match in Category_Name
    if stnx_items['Category_Name'].str.contains(entity_name, na=False).any():
        result["type"] = "Category_Name"
        result["category"] = entity_name
        return result

    # 4. Partial match in Brand_Name
    if stnx_items['Brand_Name'].str.contains(entity_name, na=False).any():
        result["type"] = "Brand_Name"
        cat = stnx_items.loc[stnx_items['Brand_Name'].str.contains(entity_name, na=False), 'Category_Name'].dropna().unique()
        if len(cat) > 0:
            result["category"] = cat[0]
        return result

    # 5. Partial match in Item_Name
    if stnx_items['Item_Name'].str.contains(entity_name, na=False).any():
        result["type"] = "Item_Name"
        cat = stnx_items.loc[stnx_items['Item_Name'].str.contains(entity_name, na=False), 'Category_Name'].dropna().unique()
        if len(cat) > 0:
            result["category"] = cat[0]
        return result

    return result

