from langchain.agents import initialize_agent, AgentType
from langchain.prompts import SystemMessagePromptTemplate, MessagesPlaceholder, HumanMessagePromptTemplate
from langchain.prompts.chat import ChatPromptTemplate
from langchain.tools import tool
from langchain_openai import AzureChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
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


class DecoratorAgent(BaseAgent):

    def __init__(self):
        self.llm = AzureChatOpenAI(
                    azure_endpoint = os.getenv("AZURE_ENDPOINT"),
                    api_key = os.getenv("OPENAI_API_KEY"),  # או פשוט המחרוזת עצמה אם אתה לא רוצה ENV
                    api_version="2024-08-01-preview",
                    azure_deployment="Diplochat",
                    temperature=0.0,
                    streaming=True
                )
        self.system_prompt = self.get_system_prompt()
        
    def get_system_prompt(self) -> SystemMessagePromptTemplate:
        return SystemMessagePromptTemplate.from_template(
            """
            You are a Response Formatter Agent for Diplomat Distributors Ltd.

            Your role:
            - Produce a clear, business-friendly, and accurate final answer for the user.
            - You will receive:
                - The original user question.
                - The logical explanation of how the model generated the answer (for your understanding only).
                - The actual output/result to present.

            Very Important:
            - **Your final answer must be based on the user question.**
            - If the question involves competitor analysis, market share, or comparisons:
                - Write a professional opening sentence that explains what the table or result shows.
                - For large tables (more than 12 rows), mention that only the top competitors or top results are shown.
                - Do NOT over-explain the technical process — focus only on the final user-facing result.
            - If the question is about something else (like totals, insights, or single product analysis), adapt the tone accordingly.
            - Preserve the language of the user's question (Hebrew stays Hebrew, English stays English).
            - If Hebrew, ensure Right-To-Left (RTL) formatting.
            - Never invent additional data. Only summarize the received output professionally.

            - **When the output includes a table:**
                - Detect the table and format it cleanly in Markdown.
                - Automatically translate all column headers into the language of the user's question.
                - Remove any underscores ("_") and unnecessary symbols from headers (e.g., "%").
                - For Hebrew, ensure the column names are formal and properly aligned.
                - For English, format the headers clearly and professionally in Title Case.

            Always think:  
            > "How would I explain this answer clearly to a Diplomat business user who asked the question?"

            Remember:
            - The model explanation (process) is for your internal understanding only.
            - The user must never see the technical process.
            """
        )


    def decorate(self, user_question , result , max_retries: int = 2, delay: float = 1.0) -> dict:
        
        prompt_template = ChatPromptTemplate.from_messages([
            self.system_prompt,
            HumanMessagePromptTemplate.from_template("user_question: {user_question} \n"
                                                     "actual output: {result}"  )
        ])

        full_chain = prompt_template | self.llm
    
        self.reset_fields()
        self.set_question(user_question)
        self.start_timer()

        last_error = None
        for attempt in range(1, max_retries + 1):
            self.increment_attempts()
            try:
                self.increment_calls()
                response = full_chain.invoke({"user_question": user_question  , "result" :result})
                self.stop_timer()
                self.set_answer(response.content)
                return response.content
            
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
    