from typing import List, Any
from pydantic import BaseModel, Field
from datetime import datetime

class Result(BaseModel):
    name: str
    url: str


class Pokeapi(BaseModel):
    updated: datetime = Field(default_factory=datetime.utcnow)
    count: int
    next: str
    previous: Any
    results: List[Result]
