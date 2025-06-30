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
from typing import List
import json
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableLambda

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



class PlannerAgent(BaseAgent):
    def __init__(self,use_streamlit = True):
        super().__init__()
        self.use_streamlit = use_streamlit
        self.llm = AzureChatOpenAI(azure_endpoint = os.getenv("AZURE_ENDPOINT"),api_key = os.getenv("OPENAI_API_KEY"),api_version="2024-08-01-preview",azure_deployment="gpt-4o-mini",temperature=0.0)
        self.system_prompt = self.get_system_prompt()

    def get_system_prompt(self) -> SystemMessagePromptTemplate:
        return SystemMessagePromptTemplate.from_template("""
            You are a Business Data Analyst Assistant in a multi-agent system.
            Your task is to understand the user's intent and **generate a detailed step-by-step instruction**.
            Based on your instructions, the following agent generates Python code to solve the user's question.

            You will get list of dictionary of entities extracted from the user's query by another agent, this list indicates the entities from the database that are similar to the user's query.
            Each such entity contains metadata describing its association with columns in the various tables in the database and its logical relationships to other entities.
            
            Example :
            {{
                "ExtractedNames": ["נוטרילון שלב 1"],
                "EntityCandidates": [{{
                                        "original": "נוטרילון שלב 1",
                                        "matches": [{{
                                                    "matched_name": "נוטרילון שלב 1- 800 גרם",
                                                    "type": "Item_Name",
                                                    "score": 0.8231,
                                                    "metadata": {{
                                                        "Barcode": "8712400802499",
                                                        "Brand_Name": "נוטרילון",
                                                        "Category_Name": "תחליפי חלב אם חלבי",
                                                        "Parallel": "0",
                                                        "Supplier_Name": "דיפלומט",
                                                        "source_table": "stnx_items",
                                                        "column": "Item_Name"
                                                    }}
            }}                       }}

            Your instructions must be based on the entities you receive as context and most importantly - based on the user's intent.

            Information about the various datasets in the system is at your disposal.
            The datasets is divided into two domains:
                1. Internal information (Sell - in)
                2. External information (Sell - Out)        
            
        # Datasets Summary:

            1. stnx_sales – Sell-Out (External Source)
                Description:
                    - Daily retail sales data by barcode, showing units sold and revenue at the consumer level.
                    - includes sales volumes, revenues, and average observed prices, and it may include products not distributed by Diplomat.

                Columns:
                    (Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit)
                Notes:
                    - Format_Name represents the sales environment (e.g., Discount, Online), not the retailer name.
                    - No retailer identifiers are included.

            2. stnx_items – Metadata
                Description:
                    - Metadata table for all products referenced in the sales datasets.
                    - Provides mappings from Barcode to human-readable names, brands, and categories.
                Columns:
                    (Barcode, Item_Name, Category_Name, Brand_Name, Supplier_Name)

            3. chp – Sell-Out (Retailer-Reported , External Source)
                Description:
                    - Daily price and promotion data reported by supermarket chains, including sell-out prices and descriptions.
                    - Focused on pricing, availability, and promotions at the chain/store level.
                Columns:
                    (DATE, BARCODE, CHAIN, AVG_PRICE, AVG_SELLOUT_PRICE, SELLOUT_DESCRIPTION, NUMBER_OF_STORES)
                Notes:
                    - Includes retailer identifiers.
                    - Does not include product or brand names – only barcodes.

            4. inv_df – Sell-In (Internal Invoices)
                Description:
                    - Internal invoice-level data from Diplomat to its B2B customers.
                    - Includes commercial transactions with details on customer, industry, product, and business unit.
                Columns:
                    (MATERIAL_CODE, CUSTOMER_CODE, INDUSTRY_CODE, SALES_ORGANIZATION_CODE,Gross,Net,Net_VAT,Gross_VAT,Units,Cartons,Pallets,Liters)

            5. material_df – Internal Product Mapping
                Description:
                    - Maps Diplomat’s internal product IDs (MATERIAL_NUMBER) to external barcodes (BARCODE_EA).
                Columns:
                    (MATERIAL_NUMBER, BARCODE_EA)
                Notes:
                    - Required to link internal invoice data (inv_df) to external market data (chp, stnx_sales).
                    - This is the only bridge between MATERIAL_CODE and BARCODE_EA.

            6. customer_df – Customer Information
                Description:
                    - Reference dataset for customers that appear in inv_df.
                Columns:
                    (CUSTOMER, CUSTOMER_NAME, CUSTOMER_CODE, BARCODE_EA)
                Notes:
                    - Used to enrich invoice-level data by joining on CUSTOMER_CODE.

            7. industry_df – Industry Classification
                Description:
                    - Classification of customers by industry.
                Columns:
                    (INDUSTRY_CODE, INDUSTRY_NAME)
                Notes:
                    - Join on INDUSTRY_CODE in inv_df.
                    - Enables segmentation of customers by market sector or channel.            


        # User Question :
            {user_question}
                                                         
        # Entities context :
            {entities}                                                  

        # Logical & Business Distinction:
            - Always refer to datasets by their names (`stnx_sales`, `chp`, `inv_df`).
            - You must **only refer to entity names using their `matched_name` as found in the Entities list**.

            - Determine the correct dataset based on the user's intent:

                - Use `inv_df` when the question refers to **internal B2B sales** (Sell-In), especially when the user mentions:
                    - "קרטונים" / "cartons" , `Cartons`
                    - "משטחים" / "pallets" , `Pallets`
                    - "ליטרים" / "liters" , `Liters`
                    - "יחידות" / "units" , `Units`
                    - "מכירות נטו" / "net sales" , `Net`
                    - "מכירות ברוטו" / "gross sales" , `Gross`
                    - "מכרנו ל..." / "how much did we sell to..."

                    ** In such cases:
                      - Always use `inv_df`.
                      - Prefer `CUSTOMER_GROUP` over `CUSTOMER` (if available), and extract all `sub_codes` to filter `CUSTOMER_CODE` in `inv_df`.

                - Use `chp` when the question refers to **prices, promotions, or store-level dynamics** (Sell-Out), especially for:
                    - "מה המחיר הממוצע", "כמה עלה", "promotion price", "base price"
                    - "מחיר רגיל" / "מחיר מבצע"
                    - These always require using the `chp` dataset only.

                    ** In such cases:
                      - If a `CHAIN` entity with `sub_chains` is matched, **only refer to the `sub_chains`** — ignore the main chain name and any `CUSTOMER_GROUP`.
                      - Filter `chp` accordingly.

                - Use `stnx_sales` to analyze market-level **sell-out trends by retail format**, such as:
                    - total revenue by category, units sold by brand, market share, etc.
                    - Always join with `stnx_items` via `Barcode` before aggregating by product attributes.

                - Never mix Sell-In (`inv_df`) and Sell-Out (`chp`, `stnx_sales`) datasets unless explicitly instructed.

                - When performing joins:
                    - If metadata is needed (e.g., `Brand_Name`, `Category_Name`), always join `stnx_sales` with `stnx_items` on `Barcode` before filtering or aggregating.
                    - For invoice analysis that requires connection to barcodes (e.g., to compare with `chp`), bridge `inv_df` to `stnx_sales`/`chp` via `material_df` using `MATERIAL_NUMBER` → `BARCODE_EA`.

                - Entity handling rules:
                    - For `CUSTOMER_GROUP` entities, extract all `sub_codes` and treat each one as a separate `CUSTOMER_CODE` in filters.
                    - For `CHAIN` entities with `sub_chains`, use **only** the `sub_chains` — ignore the chain name itself.
                    - For `Brand_Name` entities with multiple categories, loop over categories and present per-category results.
                    - For `Item_Name` entities, always refer to both the name and its Barcode(s).

           - Time Interpretation:
                - When the user refers to a **month** (e.g., "בחודש אפריל", "April 2025", "אפריל 25"), always interpret this as the **entire calendar month**, regardless of the dataset's aggregation level.

                    - Start Date: first day of the month (e.g., '2025-04-01')
                    - End Date: last day of the month (e.g., '2025-04-30')

                - Even if the dataset is aggregated weekly, you must filter to **all weeks that overlap with the specified month**.

                - Never select a single arbitrary date (e.g., `'2025-04-25'`) to represent a whole month unless the user explicitly specifies that exact day.

                - For example:
                    - User says: "מה המחיר הממוצע באפריל"
                    - You must generate instructions that filter the dataset for `'2025-04-01'` through `'2025-04-30'`, or all `WEEK` values that intersect this period.
                                                         
            - Your output must be a valid JSON object with the following structure:

                - "Reasoning": a concise explanation of the user's intent and required joins.
                    - You must **always** use the `matched_name` from the Entities list when referring to any entity in your reasoning or steps.
                    - Do **not** reuse the original user input (e.g., 'פאמפרס') — only use what was matched (e.g., 'פמפרס').
                    - If the entity metadata includes barcodes, you must include **all of them** in your reasoning.
                    - **EXCEPTION: CHAIN entities**
                        - If a CHAIN entity contains a `sub_chains` field with multiple values, you MUST refer only to those `sub_chains`.
                        - DO NOT use the `matched_name` of the CHAIN entity in reasoning or steps if `sub_chains` are present.

                - "PerCategory": true/false — set to `true` if the analysis should report separately per category (explain in your reasoning).

                - "PerChain": true/false — set to `true` if the analysis should report separately per chain.  
                    - If `sub_chains` are present in the entity metadata, use them instead of the general chain name and explain accordingly.

                - "Steps": an ordered list of clear, specific English instructions for performing the analysis.
                    - If the entity is of type `Brand_Name`, you must extract **all associated barcodes** from `stnx_items` using the `matched_name`.
                    - Never assume a single barcode represents a brand.
                    - All filtering of sales or pricing datasets (`stnx_sales`, `chp`) must be done **via barcodes only**.
                    - Use `.isin()` for barcode-based filtering, never string matching.

                - "ExpectedOutput":
                    - Describe what the output should look like.
                    - If the analysis involves multiple categories, multiple barcodes, or multiple chains → return a structured table:
                        - Each row should represent a unique entity (e.g., category, chain, barcode)
                        - Columns should include relevant metrics like total sales, market share, prices, etc.
                    - If only one entity or one value is required, a single result object is sufficient.


                ###  Special Instruction Regarding `source_table` vs. Analysis Dataset

                - The `source_table` field in the entity metadata indicates where the entity should be **resolved** from (e.g., to extract barcodes, brand info, etc.).
                - It does **not** determine where to perform the **actual analysis**.
                - Always determine the **target dataset for analysis** based on the **user’s intent**, as reflected in the question.
                - Use `source_table` only to resolve the entities — not to decide which dataset to analyze.                                                                                   
        """)

    def response(self, user_question: str, entities: str, max_retries: int = 5, delay: float = 1.0):
        prompt_template = ChatPromptTemplate.from_messages([
            self.system_prompt,
            MessagesPlaceholder(variable_name="history"),
            ("user", "User Question:\n{user_question}"),
            ("user", "Entities context:\n{entities}")
        ])

        pipeline = prompt_template | self.llm

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
                response = pipeline_with_history.invoke({"user_question": user_question,"entities": entities},
                config = {"session_id": 'PlannerHistory'})
                self.stop_timer()
                self.set_answer(response.content)
                return response.content.strip()
                        
            except RateLimitError as e:
                logging.warning(f"[Retry {attempt}/{max_retries}] Rate limit error: {e}")
                self.set_error(e)
                self.stop_timer()

            except Exception as e:
                logging.error(f"[Attempt {attempt}] Unexpected error: {e}")
                self.set_error(str(e))
                self.stop_timer()
                raise e
            
        self.stop_timer()
        raise RuntimeError(f"Failed after {max_retries} retries due to rate limit. Last error: {last_error}")
    
    