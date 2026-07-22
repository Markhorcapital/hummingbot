from decimal import Decimal
from enum import Enum
from typing import Literal, Optional

from pydantic import model_validator

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase


class TWAPMode(Enum):
    MAKER = "MAKER"
    TAKER = "TAKER"


class TWAPExecutorConfig(ExecutorConfigBase):
    type: Literal["twap_executor"] = "twap_executor"
    connector_name: str
    trading_pair: str
    side: TradeType
    leverage: int = 1
    total_amount_quote: Decimal
    total_duration: int
    order_interval: int
    mode: TWAPMode = TWAPMode.TAKER

    # MAKER mode specific parameters
    limit_order_buffer: Optional[Decimal] = None
    order_resubmission_time: Optional[int] = None

    # CoinGecko volume gate (optional). When both threshold and coin_id are set,
    # each child order is allowed only if exchange pair volume passes the rule.
    volume_threshold_usd: Optional[Decimal] = None
    coingecko_coin_id: Optional[str] = None
    # True: trade only when volume < threshold. False: trade only when volume >= threshold.
    trade_when_volume_below: bool = True
    # If CoinGecko call fails, skip the child order (safer default).
    skip_on_volume_api_error: bool = True

    @model_validator(mode="after")
    def validate_mode_config(self):
        """Validate mode-specific requirements"""
        if self.mode == TWAPMode.MAKER and self.limit_order_buffer is None:
            raise ValueError("limit_order_buffer is required for MAKER mode")
        return self

    @property
    def is_maker(self) -> bool:
        return self.mode == TWAPMode.MAKER

    @property
    def volume_check_enabled(self) -> bool:
        return self.volume_threshold_usd is not None and bool(self.coingecko_coin_id)

    @property
    def number_of_orders(self) -> int:
        return (self.total_duration // self.order_interval) + 1

    @property
    def order_amount_quote(self) -> Decimal:
        return self.total_amount_quote / self.number_of_orders

    @property
    def order_type(self) -> OrderType:
        return OrderType.LIMIT if self.is_maker else OrderType.MARKET
