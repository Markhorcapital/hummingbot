from decimal import Decimal
from enum import Enum
from typing import Literal, Optional

from pydantic import field_validator, model_validator

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase


class VolumeBotMode(Enum):
    """Execution modes for Volume Bot"""
    FIXED = "FIXED"                    # Fixed batch sizes
    VARIABLE = "VARIABLE"              # Variable batch sizes with randomization
    TIME_BASED = "TIME_BASED"          # Time-based execution rate
    VOLUME_TARGET = "VOLUME_TARGET"    # Target daily volume


class VolumeBotExecutorConfig(ExecutorConfigBase):
    """
    Configuration for Volume Bot Executor
    Reuses same structure as TWAPExecutorConfig for consistency
    """
    type: Literal["volume_bot_executor"] = "volume_bot_executor"
    
    # Required Parameters (same as TWAP)
    connector_name: str
    trading_pair: str
    leverage: int = 1
    
    # Volume Bot Specific Parameters
    mode: VolumeBotMode = VolumeBotMode.FIXED
    
    # Trade Amount Configuration
    trade_amount_quote: Decimal  # Amount per trade in quote currency
    
    # Batch Configuration (for FIXED and VARIABLE modes)
    buy_batch_size: int = 3
    sell_batch_size: int = 3
    
    # Timing Configuration (in seconds)
    interval_between_buys: int = 5      # Seconds between buy orders
    interval_between_sells: int = 5    # Seconds between sell orders
    interval_between_cycles: int = 10  # Seconds between buy→sell cycles
    
    # Variable Mode Configuration
    min_batch_size: Optional[int] = None
    max_batch_size: Optional[int] = None
    randomize: bool = False
    
    # Time-Based Mode Configuration
    buys_per_hour: Optional[int] = None
    maintain_balance: bool = True
    
    # Volume Target Mode Configuration
    daily_volume_target: Optional[Decimal] = None
    
    # Order Type Configuration
    order_type: OrderType = OrderType.MARKET  # Default to market orders
    
    # MAKER Mode Configuration (if using limit orders)
    limit_order_buffer: Optional[Decimal] = None
    
    @field_validator('buy_batch_size', 'sell_batch_size')
    @classmethod
    def validate_batch_size(cls, v):
        if v < 1:
            raise ValueError("Batch size must be at least 1")
        return v
    
    @field_validator('min_batch_size', 'max_batch_size')
    @classmethod
    def validate_variable_batch(cls, v, values):
        if v is not None and v < 1:
            raise ValueError("Batch size must be at least 1")
        if 'min_batch_size' in values.data and 'max_batch_size' in values.data:
            min_size = values.data.get('min_batch_size')
            max_size = values.data.get('max_batch_size')
            if min_size and max_size and min_size > max_size:
                raise ValueError("min_batch_size must be <= max_batch_size")
        return v
    
    @model_validator(mode="after")
    def validate_mode_config(self):
        """Validate mode-specific requirements"""
        if self.mode == VolumeBotMode.VARIABLE:
            if self.min_batch_size is None or self.max_batch_size is None:
                raise ValueError("min_batch_size and max_batch_size required for VARIABLE mode")
        
        if self.mode == VolumeBotMode.TIME_BASED:
            if self.buys_per_hour is None:
                raise ValueError("buys_per_hour required for TIME_BASED mode")
        
        if self.mode == VolumeBotMode.VOLUME_TARGET:
            if self.daily_volume_target is None:
                raise ValueError("daily_volume_target required for VOLUME_TARGET mode")
        
        if self.order_type == OrderType.LIMIT and self.limit_order_buffer is None:
            raise ValueError("limit_order_buffer required for LIMIT order type")
        
        return self
    
    @property
    def is_maker(self) -> bool:
        """Check if using maker orders (limit orders)"""
        return self.order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER)
    
    @property
    def is_balanced(self) -> bool:
        """Check if buy and sell batch sizes are balanced"""
        return self.buy_batch_size == self.sell_batch_size

