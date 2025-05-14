import time
from datetime import datetime
import streamlit as st
import pandas as pd

class BaseAgent:
    def __init__(self):
        self.reset_fields()

    def reset_fields(self):
        self.question = None
        self.answer = None
        self.start_time = None
        self.end_time = None
        self.num_llm_attempts = 0
        self.num_llm_calls = 0
        self.error = None
        self.num_exec = None
        self.exec_error = None
        self.rating = None

    def set_question(self, question: str):
        self.question = question

    def start_timer(self):
        self.start_time = time.time()

    def stop_timer(self):
        self.end_time = time.time()

    def increment_attempts(self):
        self.num_llm_attempts += 1

    def increment_calls(self):
        self.num_llm_calls += 1

    def set_num_exec(self, number_of_executions):
        self.num_exec = number_of_executions

    def set_answer(self, answer):
        self.answer = answer

    def set_error(self, error: Exception):
        self.error = str(error)

    def set_exec_error(self, error: Exception):
        self.exec_error = str(error)
        
    def set_rating(self, rating : int):
        self.rating = rating


    def get_duration(self) -> float:
        return round((self.end_time - self.start_time), 3) if self.start_time and self.end_time else 0.0

    def set_log(self, agent_type: str, user_email: str, was_retry: bool = False) -> dict:
        new_row =  {
            "user": user_email,
            "timestamp": datetime.now(),
            "agent": agent_type,
            "attempts": self.num_llm_attempts,
            "calls" : self.num_llm_calls,
            "error": self.error,
            "num_exec": self.num_exec,
            "exec_error" : self.exec_error,
            "retry" : was_retry,
            "duration": self.get_duration(),
            "question": self.question,
            "answer": self.answer,
            "rating" : self.rating
        }

        st.session_state["Logs"] = pd.concat([st.session_state["Logs"], pd.DataFrame([new_row])], ignore_index=True)

        