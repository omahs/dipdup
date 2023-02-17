from decimal import Decimal

from demo_token.handlers.on_balance_update import on_balance_update
from demo_token.types.tzbtc.parameter.transfer import TransferParameter
from demo_token.types.tzbtc.storage import TzbtcStorage
from dipdup.context import HandlerContext
from dipdup.models.tezos_tzkt import TzktTransaction


async def on_transfer(
    ctx: HandlerContext,
    transfer: TzktTransaction[TransferParameter, TzbtcStorage],
) -> None:
    if transfer.parameter.from_ == transfer.parameter.to:
        # NOTE: Internal tzBTC transfer
        return

    amount = Decimal(transfer.parameter.value) / (10**8)
    await on_balance_update(
        address=transfer.parameter.from_,
        balance_update=-amount,
        timestamp=transfer.data.timestamp,
    )
    await on_balance_update(
        address=transfer.parameter.to,
        balance_update=amount,
        timestamp=transfer.data.timestamp,
    )