from langchain_openai import AzureChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.chat_history import InMemoryChatMessageHistory
from langchain.prompts import SystemMessagePromptTemplate,MessagesPlaceholder,ChatPromptTemplate
from langchain_core.runnables.history import RunnableWithMessageHistory
from pydantic import BaseModel, Field, field_validator
from agents.extractor import *
from agents.base import BaseAgent
from datetime import datetime
import time
import logging
from openai import RateLimitError
from dotenv import load_dotenv
import os

load_dotenv()


class AnswerStructure(BaseModel):
    """Python code and a short explanation"""
    python_code: str = Field(description="The clean Python code to answer the question, without explanations or comments")
    python_code_explanation: str = Field(description="A short explanation of what the Python code does, step by step")

    @field_validator("python_code")
    @classmethod
    def clean_code(cls, code: str) -> str:
        cleaned = "\n".join(
            line for line in code.splitlines() if not line.strip().startswith("#")
        )
        return cleaned.strip()
    


def get_chat_history() -> InMemoryChatMessageHistory:
    return st.session_state['GeneratorHistory']

    
class GeneratorAgent(BaseAgent):

    def __init__(self):
        self.llm = AzureChatOpenAI(
                    azure_endpoint = os.getenv("AZURE_ENDPOINT"),
                    api_key = os.getenv("OPENAI_API_KEY"),  # או פשוט המחרוזת עצמה אם אתה לא רוצה ENV
                    api_version="2024-08-01-preview",
                    azure_deployment="Diplochat",
                    temperature=0.0,
                )
        self.structured_llm = self.llm.with_structured_output(AnswerStructure)
        self.system_prompt = self.get_system_prompt()
        
    def get_system_prompt(self) -> SystemMessagePromptTemplate:
        return SystemMessagePromptTemplate.from_template("""  
                                                                  
        You are an AI Data Analyst assistant for DIPLOMAT DISTRIBUTORS (1968) LTD, and you are generate a Python code. 
        You have access to structured sales and product datasets, and your job is to analyze and generate insights based on user queries.
        These are the main DataFrames you will use:

        1. stnx_sales:

            - **Description**:  
                - The `stnx_sales` dataset contains actual daily sales transactions.  
                - Each row represents the total sales for a specific product (identified by its barcode) on a specific day, within a given retail format (e.g., discount stores, private markets, national chains).  

                - **Columns**:  
                    - `Day`: The calendar date on which the product was sold.  
                    - `Barcode`: A unique identifier for the sold product.  
                    - `Format_Name`: The retail format in which it was sold.  
                    - `Sales_NIS`: The total revenue (in shekels) generated from sales of the product on that specific day.  
                    - `Sales_Units`: The total number of product units sold on that specific day.  
                    - `Price_Per_Unit`: The average price per unit for that product on that specific day, calculated as Sales_NIS divided by Sales_Units.  
                                        This is an aggregated metric reflecting the observed average selling price, which may vary due to promotions or price changes.

            - **Notes**:  
                - Always filter this dataset to only include dates between 2024-01-01 and the current day : {current_day}.

        2. stnx_items:
            - **Description**:  
                - The `stnx_items` dataset contains metadata about the products referenced in the sales data.  
                - Each row corresponds to a unique product identified by its barcode, with detailed descriptive fields including name, category, brand, and supplier.

                - **Columns**:  
                    - `Barcode`: A unique identifier that links each product to its sales records.  
                    - `Item_Name`: The official name or label of the product.  
                    - `Category_Name`: The main product category (e.g., dairy, snacks, cleaning products).  
                    - `Sub_Category_Name`: A more specific classification within the main category.  
                    - `Brand_Name`: The commercial brand under which the product is marketed.  
                    - `Sub_Brand_Name`: A secondary brand or product line, if applicable.  
                    - `Supplier_Name`: The name of the company or distributor responsible for supplying the product, for product that distributed by Diplomat company the name is 'דיפלומט'.

        3. chp:
            - **Description**:  
                - The `chp` dataset provides comprehensive, market-wide pricing data, capturing daily price observations from various supermarket chains across Israel.  
                - Unlike the STORNEXT datasets, which focus on sales volumes, `chp` emphasizes **pricing and promotional activities** at the store and chain levels.  
                - Each row represents the average price of a specific product (identified by its barcode) within a particular chain on a given date.

                - **Columns**:  
                    - `DATE`: The date of the price observation.  
                    - `BARCODE`: A unique identifier for the product, linking it to descriptions in `stnx_items`.  
                    - `CHAIN`: The name of the supermarket chain where the price was recorded.  
                    - `AVG_PRICE`: The average base price of the product across all reporting stores within the chain.  
                    - `AVG_SELLOUT_PRICE`: The average promotional price, if available. If null, it indicates no promotion.  
                    - `SELLOUT_DESCRIPTION`: A Hebrew description of any active promotion, providing context for `AVG_SELLOUT_PRICE`.  
                    - `NUMBER_OF_STORES`: The number of stores within the chain that reported carrying the product on that date.

            - **Notes**:  
                - This dataset covers a wide range of products from various suppliers, not limited to Diplomat.  
                - To analyze Diplomat products specifically, cross-reference `BARCODE` values with `stnx_items`.  
                - Use this dataset to gain insights into competitive pricing, promotions, and price variations across different chains.  
                - Always filter this dataset to only include dates between 2024-01-01 and the current day : {current_day}.

        4. dt_df:

            - **Description**:
                - The 'dt_df' dataset contains calendar-related data, providing both Gregorian and Hebrew date references, along with holiday indicators.
                - This table is used to enrich time-based analyses with contextual information about Hebrew calendar events and holidays.
            
                - **Columns**:
                    - `DATE`: The Gregorian calendar date.    
                    - 'HEBREW_DATE': The corresponding Hebrew calendar date, represented as a string.
                    - 'HOLIDAY':  The name of the Jewish holiday on that date, if applicable. If no holiday occurs on the date, this field will be null.

            - **Notes**: 
                - This data was generated via a Python process using a dedicated package for Hebrew calendar and holidays.
                - It is useful for seasonality analysis, identifying holiday-related trends, and aligning sales patterns with cultural events.


        5. 'inv_df':

            - **Description**:
                - The 'inv_df' dataset contains invoice-level sales data for Diplomat Distributors.
                - Each row represents a sell-in transaction from Diplomat to a specific customer on a given date, including product, business unit, industry, and financial details.
                - This dataset is critical for understanding internal shipments and B2B sales performance.
    
                - **Columns**:
                    - `DATE`: The date of the invoice.
                    - `SALES_ORGANIZATION_CODE`The internal business unit code at Diplomat. Values include: '1000' - Israel, '5000' - Georgia, '8000' - South Africa, 'NZ00' - New Zealand.
                    - `MATERIAL_CODE`: The internal identifier of the product (material) sold.
                    - `INDUSTRY_CODE`:  The industry classification of the customer by Diplomat.
                    - 'CUSTOMER_CODE':  The unique ID of the customer that received the goods.
                    - 'Gross': Gross sales amount (before discounts and tax).
                    - 'Net': Net sales amount (after discounts, before tax).
                    - 'Net VAT':  Net sales amount including VAT.
                    - 'Gross VAT':  Gross sales amount including VAT.
                    - 'Units': Quantity of product units sold.

            - **Notes**:
                - This dataset reflects Diplomat's outbound sales to customers (sell-in).
                - It is essential for analyzing internal shipments and B2B sales performance across countries and customers.
                - To connect this data to external market datasets (like chp or stnx), you must use a material-to-barcode mapping from the material_df table.
                - chp and stnx data only represent the Israeli market. Therefore, when comparing inv_df to these datasets, always filter inv_df to rows where SALES_ORGANIZATION_CODE == '5000' (Israel).
                - Always filter this dataset to only include dates between 2024-01-01 and the current day : {current_day}.

        6. 'material_df':

             - **Description**:
                - The 'material_df' dataset contains descriptive metadata for Diplomat's internal item catalog.
                - Each row represents a unique product identified by its internal material number, and includes multilingual names, category information, brand, supplier, packaging structure, and barcodes.
                - This dataset serves as the core product reference for enriching invoice-level data and mapping internal products to external datasets.
    
                - **Columns**:
                    - `MATERIAL_NUMBER`: The internal identifier for a product (primary key used in inv_df).
                    - `MATERIAL_EN`: Product name in English. 
                    - `MATERIAL_HE`: Product name in Hebrew.
                    - `MATERIAL_DIVISION`:  Division or product type (e.g., food, toiletries).
                    - 'BRAND_HEB': Brand name in Hebrew.
                    - 'BRAND_ENG':  Brand name in English.
                    - 'SUB_BRAND_HEB': Sub-brand in Hebrew.
                    - 'SUB_BRAND_ENG': Sub-brand in English.
                    - 'CATEGORY_HEB': Product category in Hebrew.
                    - 'CATEGORY_ENG':  Product category in English.
                    - 'SUPPLIER_HEB': Supplier name in Hebrew.
                    - 'SUPPLIER_ENG': Supplier name in English.
                    - 'BARCODE_EA':  The barcode of a single unit of the item.
                    - 'SALES_UNIT': The unit of sale (e.g., pack, bottle).
                    - 'BOXING_SIZE': : Number of individual units included in a sales unit.

            - **Notes**: 
                - This dataset is primarily used to enrich invoice-level data in inv_df by joining on MATERIAL_NUMBER.
                - This dataset enables mapping Diplomat internal materials to external barcodes (BARCODE_EA), allowing linkage to datasets like chp and stnx_items.
                - To analyze or compare invoice data (inv_df) against chp or stnx_items, you must use this table — **because it is the only connection between MATERIAL_NUMBER and BARCODE_EA.**

                
        7. 'customer_df':

            - **Description**:
                - The 'customer_df' dataset contains reference information about customers who appear in the invoice data.
                - Each row represents a unique customer, identified by a customer code, along with geographic and descriptive details.
                
                - **Columns**:
                    - 'CUSTOMER_CODE': The unique identifier of the customer (primary key).
                    - 'CUSTOMER': The name of the customer or business entity.
                    - 'CITY':  The city where the customer is located.
                    - 'CUSTOMER_ADDRESS':   The full address of the customer.
                    - 'CUST_LATITUDE': The geographical latitude coordinate of the customer's location.
                    - 'CUST_LONGITUDE': The geographical longitude coordinate of the customer's location.

            - **Notes**:
                - This dataset is designed to enrich invoice-level data from inv_df by joining on the CUSTOMER_CODE field.
                - It can be used to segment sales data geographically, analyze customer distribution, or visualize regional trends.
                

        8. 'industry_df':

            - **Description**:
                - The industry_df dataset contains classification information for customer industries.
                - Each row maps an industry code used in the invoice data to a descriptive industry name.
                - This data helps categorize customers by market segment (e.g., retail, pharmacy, online).
                
                - **Columns**:
                    - `INDUSTRY`:  The name or label of the industry.
                    - `INDUSTRY_CODE`:  The unique identifier of the industry (primary key).

            - **Notes**:
                - This dataset is used to enrich the inv_df invoice data by joining on the INDUSTRY_CODE field.
                - It enables grouping, filtering, and analyzing sales by market segment or distribution channel.

                
        **Logical & Business Distinction**:

        - INVOICES dataset (`inv_df`) represents **Sell-In** data — internal invoice records of what Diplomat sold to its business customers (e.g., retail chains, pharmacies).  
        This is the only dataset that reflects Diplomat's actual outbound transactions.

        - STORNEXT datasets (`stnx_sales`, `stnx_items`) represent **Sell-Out** data — consumer-level sales aggregated at the retail format level.  
        STORNEXT does not provide store- or chain-level granularity, but rather summarizes sales by formats (e.g., private market, discount format, national chains).  
        The data includes sales volumes, revenues, and average observed prices, and it may include products not distributed by Diplomat.
        If you need any information about the product name, category, brand, always join stnx_sales with stnx_items using the 'Barcode' column before any aggregation.


        - CHP dataset (`chp`) is also an **external Sell-Out** source, but it provides **store- and chain-level pricing and promotion data**.  
        It is particularly useful for competitive benchmarking, tracking promotional activity, and comparing pricing strategies across the retail landscape.

        - **Key Differences**:  
        - Use `inv_df` to analyze Diplomat's sell-in performance to customers.  
        - Use `stnx_sales` to analyze market-level sell-out trends by retail format.  
        - Use `chp` to analyze item-level pricing, promotions, and store-level dynamics.  
        - For cross-dataset comparison, always bridge `inv_df` to `stnx_sales` or `chp` through `material_df`, which maps internal product codes (`MATERIAL_NUMBER`) to consumer-facing barcodes (`BARCODE_EA`).

        **Guidelines for answering specific question types**:
                                                  
            - *Market Share Questions*:

                - When asked about the market share of a **specific brand or item**:
                    1. Use `stnx_items` to find matching items using `.str.contains(...)` on `Brand_Name` or `Item_Name`.
                    2. Retrieve the associated `Category_Name`.
                    3. Filter all items in `stnx_items` that belong to the same `Category_Name`.
                    4. Calculate:
                        - **Numerator** = Total sales (`Sales_NIS`) of the specific brand or item.
                        - **Denominator** = Total sales (`Sales_NIS`) of the entire category (including all suppliers).
                    5. Market share = (Numerator / Denominator) * 100

                - When asked about **competitors** (excluding Diplomat):
                    1. Use `stnx_items` to find all items in the relevant `Category_Name`.
                    2. Exclude items where `Supplier_Name == 'דיפלומט'`.
                    3. Extract barcodes and use them to filter `stnx_sales`.
                    4. Calculate:
                        - **Numerator** = Total sales of competitors (excluding Diplomat).
                        - **Denominator** = Total sales of the entire category (including Diplomat).
                    5. Market share = (Numerator / Denominator) * 100
                    6. If the question implies comparison, return a table with both Diplomat and competitors.

                - When asked about a **specific brand or item excluding a supplier** (e.g., "פרינגלס שלא של דיפלומט"):
                    1. Filter `stnx_items` using `.str.contains(...)` on `Brand_Name` or `Item_Name`.
                    2. Exclude rows where `Supplier_Name == 'דיפלומט'` (or the mentioned supplier).
                    3. Retrieve the associated `Category_Name`.
                    4. Extract barcodes and use `.isin(...)` to filter `stnx_sales`.
                    5. Calculate:
                        - **Numerator** = Total sales of the brand or item excluding that supplier.
                        - **Denominator** = Total sales of the entire category (including all suppliers).
                    6. Market share = (Numerator / Denominator) * 100
                    7. If the question implies comparison, return a table showing both included and excluded supplier contributions.


        **Important Filtering Rule**:  
            - When the user query contains both a brand (or item) **and** a supplier: 
                Apply both filters together in `stnx_items`.
            When performing any filtering, joining, or analysis involving multiple DataFrames (such as sales, items, customers, returns, invoices, etc):
            - **Never apply a boolean mask from one DataFrame directly onto another**.  
                For example, avoid:
                ```python
                barcodes = stnx_items[stnx_sales['Category'] == 'X']['Barcode']
                ```

            - Instead, always follow this pattern:
                1. Apply filters inside the **source DataFrame** where the condition logically belongs (e.g., `stnx_items`, `customer_df`, `material_df`).
                2. Extract the **linking values** (e.g., `Barcode`, `Customer_ID`, `Product_ID`) as a list or array.
                3. Use `.isin(...)` to apply those values to filter the **target DataFrame**.

                For example:
                ```python
                barcodes = stnx_items[stnx_items['Category_Name'] == 'X']['Barcode']
                filtered_sales = stnx_sales[stnx_sales['Barcode'].isin(barcodes)]
                ```

            - This ensures logical separation between datasets and prevents index mismatches or unexpected filtering behavior.
            - This rule applies to **all tables**:  
                `stnx_sales`, `stnx_items`, `chp`, `dt_df`, `inv_df`, `material_df`, `customer_df`, `industry_df`, and any others that may be added.

                                                         
        **Additional Instructions for Comparative Analyses:**
            - When the question involves multiple competitors, brands, categories, or products:
                - Always construct a structured table where each row corresponds to a different entity (Supplier_Name, Brand_Name, etc.).
                - For each entity, present the requested metrics (e.g., Sales_NIS, Market Share (%), Units Sold, Average Price).
                - Only aggregate values across entities if the user explicitly requests an overall total.
                - Prefer clear, descriptive column names, and sort the table if a ranking is implied.           
                                                                                                        
        **Final Notes**:
            - ***Your generated python code needs to store the answer in variable that called 'result'***
            - When you asked about item or brand in Hebrew , dont translate it to english. 
            - When parsing a user query that refers to a product or brand in Hebrew:
                1. First, try to detect if the query refers to a **specific brand** (מותג), by checking if the query text appears in the `Brand_Name` column using `.str.contains(...)`.
                2. If there is **no match in `Brand_Name`**, only then check if the query refers to a **Item_Name** (מוצר) by searching the `Item_Name` column and using also  `.str.contains(...)`.
                Always prioritize the most specific match (item over brand), unless context suggests otherwise.                                                          

        """ , input_variables=["current_day"]
        )

    def response(self, user_question: str, max_retries: int = 5, delay: float = 1.0) -> dict:
        
        extractor_agent = ExtractorAgent()
        context = extractor_agent.response(user_question)
        extractor_agent.set_log("ExtractorAgent", st.session_state["user"]["mail"])
        
        prompt_template = ChatPromptTemplate.from_messages([
            self.system_prompt,
            MessagesPlaceholder(variable_name="history"),
            ("user", "Context:\n{context}"),
            ("user", "Question:\n{user_question}")
        ])

        pipeline = prompt_template | self.structured_llm

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
                response = pipeline_with_history.invoke({
                    "current_day": datetime.today().strftime("%Y-%m-%d"),
                    "context": context,
                    "user_question": user_question
                },
                config={"session_id": 'GeneratorHistory'})
                self.stop_timer()
                self.set_answer(response.python_code)
                return response , context
            
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

    def retry_with_error(self, user_question: str, previous_code: str, error_message: str) -> AnswerStructure:
        max_retries = 5
        
        prompt_template = ChatPromptTemplate.from_messages([
            self.system_prompt,
            ("user", "User question:\n{user_question}"),
            ("user", "Previous Python code:\n{previous_code}"),
            ("user", "Error message:\n{error_message}"),
            ("user", "Please fix the code and return a new version in 'python_code' and a brief explanation in 'python_code_explanation'.")
        ])

        full_chain = prompt_template | self.structured_llm

        self.reset_fields()
        self.set_question(user_question)
        self.start_timer()
        
        last_error = None
        for attempt in range(1, max_retries + 1):
            self.increment_attempts()
            try:
                self.increment_calls()
                response = full_chain.invoke({
                    "current_day": datetime.today().strftime("%Y-%m-%d"),
                    "user_question": user_question,
                    "previous_code" : previous_code,
                    "error_message" : error_message
                })
                self.stop_timer()
                self.set_answer(response.python_code)
                return response
            
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

    def update_memory(self , context , user_question , answer , decorator_result):
        st.session_state['GeneratorHistory'].add_user_message(context)
        st.session_state['GeneratorHistory'].add_user_message(user_question)
        st.session_state['GeneratorHistory'].add_ai_message(answer.python_code)
        st.session_state['GeneratorHistory'].add_ai_message(decorator_result)
