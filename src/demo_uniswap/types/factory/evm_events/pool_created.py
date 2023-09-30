# generated by datamodel-codegen:
#   filename:  PoolCreated.json

from __future__ import annotations

from pydantic import BaseModel
from pydantic import Extra


class PoolCreated(BaseModel):
    class Config:
        extra = Extra.forbid

    token0: str
    token1: str
    fee: int
    tickSpacing: int
    pool: str