"""
Volume Bot Strategy Example
Demonstrates how to use VolumeBotExecutor in a Strategy V2 implementation

This example follows the same pattern as v2_twap_multiple_pairs.py
"""
import os
import time
from typing import Dict, List, Set

from pydantic import Field, field_validator

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import PositionMode, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.volume_bot_executor.data_types import (
    VolumeBotExecutorConfig,
    VolumeBotMode,
)
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class VolumeBotExampleConfig(StrategyV2ConfigBase):
    """Configuration for Volume Bot Example Strategy"""
    script_file_name: str = os.path.basename(__file__)
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}
    
    position_mode: PositionMode = Field(
        default="HEDGE",
        json_schema_extra={
            "prompt": "Enter the position mode (HEDGE/ONEWAY): ",
            "prompt_on_new": True
        }
    )
    
    volume_bot_configs: List[VolumeBotExecutorConfig] = Field(
        default="binance,BTC-USDT,100,3,3,5,5,10,FIXED",
        json_schema_extra={
            "prompt": "Enter the Volume Bot configurations (e.g. connector,trading_pair,trade_amount_quote,buy_batch_size,sell_batch_size,interval_between_buys,interval_between_sells,interval_between_cycles,mode): ",
            "prompt_on_new": True
        }
    )

    @field_validator("volume_bot_configs", mode="before")
    @classmethod
    def validate_volume_bot_configs(cls, v):
        """Parse string configuration into VolumeBotExecutorConfig objects"""
        if isinstance(v, str):
            volume_bot_configs = []
            for config in v.split(":"):
                parts = config.split(",")
                if len(parts) >= 9:
                    (
                        connector,
                        trading_pair,
                        trade_amount_quote,
                        buy_batch_size,
                        sell_batch_size,
                        interval_between_buys,
                        interval_between_sells,
                        interval_between_cycles,
                        mode,
                    ) = parts
                    
                    volume_bot_configs.append(
                        VolumeBotExecutorConfig(
                            timestamp=time.time(),
                            connector_name=connector,
                            trading_pair=trading_pair,
                            trade_amount_quote=trade_amount_quote,
                            buy_batch_size=int(buy_batch_size),
                            sell_batch_size=int(sell_batch_size),
                            interval_between_buys=int(interval_between_buys),
                            interval_between_sells=int(interval_between_sells),
                            interval_between_cycles=int(interval_between_cycles),
                            mode=VolumeBotMode[mode.upper()],
                        )
                    )
            return volume_bot_configs
        return v

    @field_validator('position_mode', mode="before")
    @classmethod
    def validate_position_mode(cls, v: str) -> PositionMode:
        if v.upper() in PositionMode.__members__:
            return PositionMode[v.upper()]
        raise ValueError(
            f"Invalid position mode: {v}. Valid options: {', '.join(PositionMode.__members__)}"
        )


class VolumeBotExample(StrategyV2Base):
    """Volume Bot Example Strategy"""
    volume_bots_created = False

    @classmethod
    def init_markets(cls, config: VolumeBotExampleConfig):
        """
        Initialize the markets that the strategy is going to use.
        Same pattern as TWAP example.
        """
        markets = {}
        for volume_bot_config in config.volume_bot_configs:
            if volume_bot_config.connector_name not in markets:
                markets[volume_bot_config.connector_name] = set()
            markets[volume_bot_config.connector_name].add(volume_bot_config.trading_pair)
        cls.markets = markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: VolumeBotExampleConfig):
        super().__init__(connectors, config)
        self.config = config

    def start(self, clock: Clock, timestamp: float) -> None:
        """Start the strategy"""
        self._last_timestamp = timestamp
        self.apply_initial_setting()

    def apply_initial_setting(self):
        """Apply initial settings (leverage, position mode)"""
        for connector in self.connectors.values():
            if self.is_perpetual(connector.name):
                connector.set_position_mode(self.config.position_mode)
        
        for config in self.config.volume_bot_configs:
            if self.is_perpetual(config.connector_name):
                self.connectors[config.connector_name].set_leverage(
                    config.trading_pair, config.leverage
                )

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        Determine executor actions - creates Volume Bot executors
        Same pattern as TWAP example
        """
        executor_actions = []
        if not self.volume_bots_created:
            self.volume_bots_created = True
            for config in self.config.volume_bot_configs:
                config.timestamp = self.current_timestamp
                executor_actions.append(CreateExecutorAction(executor_config=config))
        return executor_actions

    def on_tick(self):
        """Called on each tick"""
        super().on_tick()
        self.check_all_executors_completed()

    def check_all_executors_completed(self):
        """Check if all executors are completed"""
        all_executors = self.get_all_executors()
        if len(all_executors) > 0 and all([executor.is_done for executor in all_executors]):
            self.logger().info("All Volume Bot executors have been completed.")
            HummingbotApplication.main_application().stop()

