
from langchain.tools import Tool
from .core import search_entities_in_vdb_core
from pydantic import BaseModel
from typing import List

class SearchEntitiesInput(BaseModel):
    names: List[str]

def make_search_entities_tool(embedding_model, combined_entities):
    return Tool(
        name="search_entities_in_vdb",
        func=lambda input: search_entities_in_vdb_core(input, embedding_model, combined_entities),
        args_schema=SearchEntitiesInput,
        description="""
            Accepts a list of entity names in Hebrew only.
            Do NOT pass English or foreign names.
            The system assumes all names have been translated to Hebrew beforehand.
            
            Returns:
            For each input name, returns up to 5 best matching entities by cosine similarity.
        """
    )