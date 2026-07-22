import os
import time
from decimal import Decimal
from typing import Dict, List, Optional, Set

from pydantic import Field, field_validator

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import PositionMode, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.twap_executor.data_types import TWAPExecutorConfig, TWAPMode
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class TWAPMultiplePairsConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}
    position_mode: PositionMode = Field(
        default="HEDGE",
        json_schema_extra={
            "prompt": "Enter the position mode (HEDGE/ONEWAY): ", "prompt_on_new": True})
    twap_configs: List[TWAPExecutorConfig] = Field(
        default="binance,WLD-USDT,BUY,1,100,60,15,TAKER",
        json_schema_extra={
            "prompt": "Enter the TWAP configurations (e.g. connector,trading_pair,side,leverage,total_amount_quote,total_duration,order_interval,mode:same_for_other_config): ",
            "prompt_on_new": True})
    # Applied to every TWAP executor unless already set on that executor config.
    volume_threshold_usd: Optional[Decimal] = Field(
        default=None,
        json_schema_extra={
            "prompt": "CoinGecko pair volume USD threshold (empty to disable): ",
            "prompt_on_new": True})
    coingecko_coin_id: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "CoinGecko coin id for volume checks (e.g. bitcoin): ",
            "prompt_on_new": True})
    trade_when_volume_below: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Trade only when volume is below threshold? (True/False): ",
            "prompt_on_new": True})
    skip_on_volume_api_error: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Skip order if CoinGecko volume API fails? (True/False): ",
            "prompt_on_new": True})

    @field_validator("twap_configs", mode="before")
    @classmethod
    def validate_twap_configs(cls, v):
        if isinstance(v, str):
            twap_configs = []
            for config in v.split(":"):
                connector, trading_pair, side, leverage, total_amount_quote, total_duration, order_interval, mode = config.split(",")
                twap_configs.append(
                    TWAPExecutorConfig(
                        timestamp=time.time(),
                        connector_name=connector,
                        trading_pair=trading_pair,
                        side=TradeType[side.upper()],
                        leverage=leverage,
                        total_amount_quote=total_amount_quote,
                        total_duration=total_duration,
                        order_interval=order_interval,
                        mode=TWAPMode[mode.upper()]))
            return twap_configs
        return v

    @field_validator('position_mode', mode="before")
    @classmethod
    def validate_position_mode(cls, v: str) -> PositionMode:
        if v.upper() in PositionMode.__members__:
            return PositionMode[v.upper()]
        raise ValueError(f"Invalid position mode: {v}. Valid options are: {', '.join(PositionMode.__members__)}")


class TWAPMultiplePairs(StrategyV2Base):
    twaps_created = False

    @classmethod
    def init_markets(cls, config: TWAPMultiplePairsConfig):
        """
        Initialize the markets that the strategy is going to use. This method is called when the strategy is created in
        the start command. Can be overridden to implement custom behavior.
        """
        markets = {}
        for twap_config in config.twap_configs:
            if twap_config.connector_name not in markets:
                markets[twap_config.connector_name] = set()
            markets[twap_config.connector_name].add(twap_config.trading_pair)
        cls.markets = markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: TWAPMultiplePairsConfig):
        super().__init__(connectors, config)
        self.config = config

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        self._last_timestamp = timestamp
        self.apply_initial_setting()

    def apply_initial_setting(self):
        for connector in self.connectors.values():
            if self.is_perpetual(connector.name):
                connector.set_position_mode(self.config.position_mode)
        for config in self.config.twap_configs:
            if self.is_perpetual(config.connector_name):
                self.connectors[config.connector_name].set_leverage(config.trading_pair, config.leverage)

    def _apply_volume_gate_settings(self, config: TWAPExecutorConfig) -> None:
        if config.volume_threshold_usd is None and self.config.volume_threshold_usd is not None:
            config.volume_threshold_usd = self.config.volume_threshold_usd
        if not config.coingecko_coin_id and self.config.coingecko_coin_id:
            config.coingecko_coin_id = self.config.coingecko_coin_id
        config.trade_when_volume_below = self.config.trade_when_volume_below
        config.skip_on_volume_api_error = self.config.skip_on_volume_api_error

    def determine_executor_actions(self) -> List[ExecutorAction]:
        executor_actions = []
        if not self.twaps_created:
            self.twaps_created = True
            for config in self.config.twap_configs:
                config.timestamp = self.current_timestamp
                self._apply_volume_gate_settings(config)
                executor_actions.append(CreateExecutorAction(executor_config=config))
        return executor_actions

    def on_tick(self):
        super().on_tick()
        self.check_all_executors_completed()

    def check_all_executors_completed(self):
        # Don't stop the application - let executors loop continuously
        # all_executors = self.get_all_executors()
        # if len(all_executors) > 0 and all([executor.is_done for executor in self.get_all_executors()]):
        #     self.logger().info("All TWAP executors have been completed.")
        #     HummingbotApplication.main_application().stop()
        pass
