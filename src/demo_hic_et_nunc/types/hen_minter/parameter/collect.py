# generated by datamodel-codegen:
#   filename:  collect.json

from __future__ import annotations

from pydantic import BaseModel
from pydantic import Extra


class CollectParameter(BaseModel):
    class Config:
        extra = Extra.forbid

    objkt_amount: str
    swap_id: str