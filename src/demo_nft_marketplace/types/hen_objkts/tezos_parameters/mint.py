# generated by datamodel-codegen:
#   filename:  mint.json

from __future__ import annotations

from pydantic import BaseModel
from pydantic import Extra


class MintParameter(BaseModel):
    class Config:
        extra = Extra.forbid

    address: str
    amount: str
    token_id: str
    token_info: dict[str, str]