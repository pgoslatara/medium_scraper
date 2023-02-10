from pydantic import BaseModel, Field
from datetime import datetime

class Pokemon(BaseModel):
    updated: datetime = Field(default_factory=datetime.utcnow)
    name: str
    url: str
