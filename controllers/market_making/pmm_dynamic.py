from decimal import Decimal
from typing import List, Tuple

import pandas as pd
import pandas_ta as ta  # noqa: F401
from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.market_making_controller_base import (
    MarketMakingControllerBase,
    MarketMakingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig


class PMMDynamicControllerConfig(MarketMakingControllerConfigBase):
    controller_name: str = "pmm_dynamic"
    candles_config: List[CandlesConfig] = []
    buy_spreads: List[float] = Field(
        default="1,2,4",
        json_schema_extra={
            "prompt": "Enter a comma-separated list of buy spreads measured in units of volatility(e.g., '1, 2'): ",
            "prompt_on_new": True, "is_updatable": True}
    )
    sell_spreads: List[float] = Field(
        default="1,2,4",
        json_schema_extra={
            "prompt": "Enter a comma-separated list of sell spreads measured in units of volatility(e.g., '1, 2'): ",
            "prompt_on_new": True, "is_updatable": True}
    )
    candles_connector: str = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the connector for the candles data, leave empty to use the same exchange as the connector: ",
            "prompt_on_new": True})
    candles_trading_pair: str = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the trading pair for the candles data, leave empty to use the same trading pair as the connector: ",
            "prompt_on_new": True})
    interval: str = Field(
        default="3m",
        json_schema_extra={
            "prompt": "Enter the candle interval (e.g., 1m, 5m, 1h, 1d): ",
            "prompt_on_new": True})
    volatility_reference_pair: str = Field(
        default="ETH-USDT",
        json_schema_extra={
            "prompt": "Enter the trading pair to use for NATR/RSI volatility calculation (e.g., ETH-USDT): ",
            "prompt_on_new": True, "is_updatable": True})
    volatility_spread_increment_pct: float = Field(
        default=1.0,
        json_schema_extra={
            "prompt": "Enter the spread increment percentage to add when volatility is detected (e.g., 1.0 for 1%): ",
            "prompt_on_new": True, "is_updatable": True}
    )
    natr_length: int = Field(
        default=14,
        json_schema_extra={"prompt": "Enter the NATR length: ", "prompt_on_new": True})
    rsi_length: int = Field(
        default=14,
        json_schema_extra={"prompt": "Enter the RSI length: ", "prompt_on_new": True})
    rsi_buying_threshold: float = Field(
        default=60.0,
        json_schema_extra={
            "prompt": "Enter the RSI threshold for buying pressure (e.g., 60.0): ",
            "prompt_on_new": True, "is_updatable": True})
    rsi_selling_threshold: float = Field(
        default=40.0,
        json_schema_extra={
            "prompt": "Enter the RSI threshold for selling pressure (e.g., 40.0): ",
            "prompt_on_new": True, "is_updatable": True})
    rsi_asymmetric_multiplier: float = Field(
        default=1.5,
        json_schema_extra={
            "prompt": "Enter the asymmetric spread multiplier for RSI-based adjustments (e.g., 1.5 for 50% wider spreads): ",
            "prompt_on_new": True, "is_updatable": True})
    volatility_threshold: float = Field(
        default=1.0,
        json_schema_extra={
            "prompt": "Enter the volatility threshold as percentage (NATR percentage to trigger spread increase, e.g., 1.0 for 1%): ",
            "prompt_on_new": True, "is_updatable": True}
    )
    natr_upper_limit: float = Field(
        default=10.0,
        json_schema_extra={
            "prompt": "Enter the maximum NATR percentage limit (orders will stop if NATR exceeds this, e.g., 10.0 for 10%): ",
            "prompt_on_new": True, "is_updatable": True})

    @field_validator("candles_connector", mode="before")
    @classmethod
    def set_candles_connector(cls, v, validation_info: ValidationInfo):
        if v is None or v == "":
            return validation_info.data.get("connector_name")
        return v

    @field_validator("candles_trading_pair", mode="before")
    @classmethod
    def set_candles_trading_pair(cls, v, validation_info: ValidationInfo):
        if v is None or v == "":
            return validation_info.data.get("trading_pair")
        return v


class PMMDynamicController(MarketMakingControllerBase):
    """
    This is a dynamic version of the PMM controller. It uses NATR to detect volatility
    and RSI to determine buying vs selling pressure. When volatility is detected, it
    increases spreads asymmetrically based on RSI direction:
    - Buying pressure (RSI > threshold): Wider sell spreads
    - Selling pressure (RSI < threshold): Wider buy spreads
    - Neutral: Equal spread adjustment
    The spread increment amount is specified by volatility_spread_increment_pct parameter.
    It also uses the Triple Barrier Strategy to manage the risk.
    """

    def __init__(self, config: PMMDynamicControllerConfig, *args, **kwargs):
        self.config = config
        # Use max of natr_length and rsi_length for max_records calculation
        self.max_records = max(config.natr_length, config.rsi_length) + 5
        self._previous_natr_exceeded = False  # Track previous state for resume logging
        if len(self.config.candles_config) == 0:
            self.config.candles_config = [CandlesConfig(
                connector=config.candles_connector,
                trading_pair=config.volatility_reference_pair,  # Use volatility reference pair for indicators
                interval=config.interval,
                max_records=self.max_records
            )]
        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        # Only use volatility_reference_pair (e.g., ETH-USDT) for NATR/RSI calculation
        # No fallback - if volatility_reference_pair candles are not available, skip NATR/RSI
        volatility_pair = self.config.volatility_reference_pair

        # Get reference price for trading pair (always needed for order placement)
        from hummingbot.core.data_type.common import PriceType
        try:
            reference_price = self.market_data_provider.get_price_by_type(
                self.config.connector_name, self.config.trading_pair, PriceType.MidPrice
            )
        except Exception as e:
            self.logger().error(
                f"Error getting reference price for {self.config.trading_pair}: {str(e)}. "
                f"Setting default values."
            )
            self.processed_data = {
                "reference_price": Decimal("0"),
                "buy_spread_adjustment": Decimal("0"),
                "sell_spread_adjustment": Decimal("0"),
                "natr_exceeded_limit": False,
                "features": pd.DataFrame()
            }
            return

        # Try to fetch candles for volatility reference pair ONLY
        try:
            candles = self.market_data_provider.get_candles_df(
                connector_name=self.config.candles_connector,
                trading_pair=volatility_pair,
                interval=self.config.interval,
                max_records=self.max_records
            )

            # Validate candles DataFrame
            if candles is None or len(candles) == 0:
                self.logger().warning(
                    f"No candles data available for {volatility_pair}. "
                    f"Skipping NATR/RSI calculation. Using default spread adjustments (no volatility adjustment)."
                )
                # Use default values without NATR/RSI
                self.processed_data = {
                    "reference_price": Decimal(reference_price),
                    "buy_spread_adjustment": Decimal("0"),
                    "sell_spread_adjustment": Decimal("0"),
                    "natr_exceeded_limit": False,
                    "features": pd.DataFrame()
                }
                return

        except Exception as e:
            self.logger().error(
                f"Error fetching candles for {volatility_pair}: {str(e)}. "
                f"Skipping NATR/RSI calculation. Using default spread adjustments (no volatility adjustment)."
            )
            # Use default values without NATR/RSI
            self.processed_data = {
                "reference_price": Decimal(reference_price),
                "buy_spread_adjustment": Decimal("0"),
                "sell_spread_adjustment": Decimal("0"),
                "natr_exceeded_limit": False,
                "features": pd.DataFrame()
            }
            return

        # Check if required columns exist
        required_columns = ["high", "low", "close"]
        missing_columns = [col for col in required_columns if col not in candles.columns]
        if missing_columns:
            self.logger().warning(
                f"Missing required columns for NATR calculation: {missing_columns}. "
                f"Skipping NATR/RSI calculation. Using default spread adjustments (no volatility adjustment)."
            )
            # Use default values without NATR/RSI
            self.processed_data = {
                "reference_price": Decimal(reference_price),
                "buy_spread_adjustment": Decimal("0"),
                "sell_spread_adjustment": Decimal("0"),
                "natr_exceeded_limit": False,
                "features": pd.DataFrame()
            }
            return

        # Log candle count for debugging
        num_candles = len(candles)
        if num_candles < self.config.natr_length:
            self.logger().warning(
                f"Insufficient candles for NATR calculation: {num_candles} candles available, "
                f"but {self.config.natr_length} required. Using available candles."
            )

        # Calculate NATR using pandas_ta (original hummingbot implementation)
        natr_raw = ta.natr(
            candles["high"], candles["low"], candles["close"],
            length=self.config.natr_length
        )

        # Check if NATR calculation returned None or invalid result
        if natr_raw is None:
            self.logger().warning("NATR calculation returned None. Using default value.")
            current_natr = 0.01  # Default 1% if calculation fails
            natr = pd.Series([current_natr] * num_candles)
        elif len(natr_raw) == 0:
            self.logger().warning("NATR calculation returned empty result. Using default value.")
            current_natr = 0.01  # Default 1% if calculation fails
            natr = pd.Series([current_natr] * num_candles)
        else:
            # Divide by 100 to convert from 0-100 range to 0-1 range
            natr = natr_raw / 100

            # Get current NATR value
            current_natr = natr.iloc[-1]

        # Handle NaN or invalid NATR
        if pd.isna(current_natr) or current_natr <= 0:
            current_natr = 0.001  # Default 1% if invalid

        # Calculate NATR statistics for reference (not used for detection)
        # natr_mean = natr.mean()
        # natr_std = natr.std()

        # Check if volatility is detected (current NATR percentage is above threshold)
        volatility_detected = False
        natr_exceeded_limit = False  # Flag to track if NATR exceeds upper limit

        # Convert NATR to percentage (0-1 range to 0-100 range) and compare directly
        current_natr_percentage = current_natr * 100  # Convert to percentage

        # Print NATR value
        # self.logger().info(f"[NATR] Current NATR: {current_natr_percentage:.4f}%")

        if not pd.isna(current_natr_percentage):
            # Check if NATR exceeds upper limit (stop orders)
            if current_natr_percentage >= self.config.natr_upper_limit:
                natr_exceeded_limit = True
                self.logger().warning(
                    f"[NATR Upper Limit] NATR ({current_natr_percentage:.4f}%) exceeds upper limit "
                    f"({self.config.natr_upper_limit:.2f}%). New orders will be stopped. "
                    f"Monitoring continues - orders will resume when NATR drops below limit."
                )
            # If NATR percentage is above threshold (but below upper limit), volatility is detected
            elif current_natr_percentage >= self.config.volatility_threshold:
                volatility_detected = True

        # Check if orders should resume (NATR dropped below limit)
        if self._previous_natr_exceeded and not natr_exceeded_limit:
            self.logger().info(
                f"[NATR Upper Limit] NATR ({current_natr_percentage:.4f}%) has dropped below upper limit "
                f"({self.config.natr_upper_limit:.2f}%). Resuming new order placement."
            )

        # Update previous state
        self._previous_natr_exceeded = natr_exceeded_limit

        # Calculate RSI to determine buying vs selling pressure
        rsi = ta.rsi(candles["close"], length=self.config.rsi_length)

        # Check if RSI calculation returned None or invalid result
        if rsi is None:
            self.logger().warning("RSI calculation returned None. Using default value.")
            current_rsi = 50.0  # Default neutral RSI
        elif len(rsi) == 0:
            self.logger().warning("RSI calculation returned empty result. Using default value.")
            current_rsi = 50.0  # Default neutral RSI
        else:
            current_rsi = rsi.iloc[-1]

            # Handle NaN or invalid RSI
            if pd.isna(current_rsi) or current_rsi <= 0 or current_rsi >= 100:
                current_rsi = 50.0  # Default neutral RSI

        # Print RSI value
        # self.logger().info(f"[RSI] Current RSI: {current_rsi:.2f}")

        # Determine pressure direction based on RSI
        buying_pressure = current_rsi > self.config.rsi_buying_threshold
        selling_pressure = current_rsi < self.config.rsi_selling_threshold

        # Calculate spread adjustments based on volatility and direction
        base_spread_adjustment = Decimal(str(self.config.volatility_spread_increment_pct)) / Decimal("100") if volatility_detected else Decimal("0")

        # Apply asymmetric spread adjustments based on RSI direction
        asymmetric_multiplier = Decimal(str(self.config.rsi_asymmetric_multiplier))

        if volatility_detected:
            if buying_pressure:
                # High volatility + Buying pressure = Protect sell side more
                buy_spread_adjustment = Decimal("0")
                sell_spread_adjustment = base_spread_adjustment * asymmetric_multiplier
                pressure_direction = "Buying"
            elif selling_pressure:
                # High volatility + Selling pressure = Protect buy side more
                buy_spread_adjustment = base_spread_adjustment * asymmetric_multiplier
                sell_spread_adjustment = Decimal("0")
                pressure_direction = "Selling"
            else:
                # High volatility + Neutral = Equal adjustment
                buy_spread_adjustment = base_spread_adjustment
                sell_spread_adjustment = base_spread_adjustment
                pressure_direction = "Neutral"
        else:
            # No volatility = No adjustment
            buy_spread_adjustment = Decimal("0")
            sell_spread_adjustment = Decimal("0")
            pressure_direction = "None"

        # Reference price should be from the trading pair (ALI-USDT), not from volatility reference pair (ETH-USDT)
        # Get the current price for the actual trading pair
        from hummingbot.core.data_type.common import PriceType
        try:
            reference_price = self.market_data_provider.get_price_by_type(
                self.config.connector_name, self.config.trading_pair, PriceType.MidPrice
            )
        except Exception as e:
            self.logger().warning(
                f"Error getting reference price for {self.config.trading_pair}: {str(e)}. "
                f"Falling back to last candle close price from trading pair candles."
            )
            # Fallback: try to get candles for the trading pair
            try:
                trading_pair_candles = self.market_data_provider.get_candles_df(
                    connector_name=self.config.candles_connector,
                    trading_pair=self.config.trading_pair,
                    interval=self.config.interval,
                    max_records=1
                )
                if trading_pair_candles is not None and len(trading_pair_candles) > 0:
                    reference_price = trading_pair_candles["close"].iloc[-1]
                else:
                    # Last resort: use ETH price (wrong but better than crashing)
                    reference_price = candles["close"].iloc[-1]
                    self.logger().error(
                        "Using volatility reference pair price as fallback. "
                        "This may cause incorrect order prices!"
                    )
            except Exception as e2:
                self.logger().error(
                    f"Critical: Cannot get reference price: {str(e2)}. Using ETH price as fallback."
                )
                reference_price = candles["close"].iloc[-1]
        # self.logger().info(f"[Reference Price] Reference price: {reference_price}")
        # Log only when volatility is detected or when status changes (reduces log spam)
        if volatility_detected or natr_exceeded_limit or (self._previous_natr_exceeded != natr_exceeded_limit):
            self.logger().info(
                f"[PMM Dynamic] NATR: {current_natr_percentage:.4f}% | "
                f"RSI: {current_rsi:.1f} | "
                f"Volatility: {'Detected' if volatility_detected else 'Normal'} | "
                f"Direction: {pressure_direction} | "
                f"Buy Adj: {float(buy_spread_adjustment) * 100:.2f}% | "
                f"Sell Adj: {float(sell_spread_adjustment) * 100:.2f}% | "
                f"Orders: {'STOPPED' if natr_exceeded_limit else 'ACTIVE'}"
            )

        candles["buy_spread_adjustment"] = float(buy_spread_adjustment)
        candles["sell_spread_adjustment"] = float(sell_spread_adjustment)
        candles["reference_price"] = reference_price
        candles["volatility_detected"] = volatility_detected
        candles["current_rsi"] = current_rsi
        candles["pressure_direction"] = pressure_direction

        self.processed_data = {
            "reference_price": Decimal(reference_price),
            "buy_spread_adjustment": buy_spread_adjustment,
            "sell_spread_adjustment": sell_spread_adjustment,
            "natr_exceeded_limit": natr_exceeded_limit,  # Flag to prevent order placement
            "features": candles
        }

    def get_price_and_amount(self, level_id: str) -> Tuple[Decimal, Decimal]:
        """
        Get the spread and amount in quote for a given level id.
        Override to add spread_adjustment when volatility is detected.
        Uses separate buy and sell spread adjustments based on RSI direction.
        """
        from hummingbot.core.data_type.common import TradeType

        level = self.get_level_from_level_id(level_id)
        trade_type = self.get_trade_type_from_level_id(level_id)
        spreads, amounts_quote = self.config.get_spreads_and_amounts_in_quote(trade_type)
        reference_price = Decimal(self.processed_data["reference_price"])

        # Get base spread from config (as percentage, e.g., 0.5 = 0.5%)
        base_spread_pct = Decimal(spreads[int(level)]) / Decimal("100")

        # Get appropriate spread adjustment based on trade type
        if trade_type == TradeType.BUY:
            spread_adjustment = Decimal(self.processed_data["buy_spread_adjustment"])
        else:  # TradeType.SELL
            spread_adjustment = Decimal(self.processed_data["sell_spread_adjustment"])

        spread_in_pct = base_spread_pct + spread_adjustment

        # Calculate order price
        side_multiplier = Decimal("-1") if trade_type == TradeType.BUY else Decimal("1")
        order_price = reference_price * (1 + side_multiplier * spread_in_pct)

        return order_price, Decimal(amounts_quote[int(level)]) / order_price

    def get_executor_config(self, level_id: str, price: Decimal, amount: Decimal):
        trade_type = self.get_trade_type_from_level_id(level_id)
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            level_id=level_id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_price=price,
            amount=amount,
            triple_barrier_config=self.config.triple_barrier_config,
            leverage=self.config.leverage,
            side=trade_type,
        )

    def get_levels_to_execute(self) -> List[str]:
        """
        Override to prevent order placement when NATR exceeds upper limit.
        Orders will automatically resume when NATR drops below the limit.
        """
        # Check if NATR exceeded the upper limit
        if self.processed_data.get("natr_exceeded_limit", False):
            # Return empty list to prevent order creation
            # Note: This method is called continuously, so orders will resume
            # automatically when natr_exceeded_limit becomes False
            return []

        # Call parent method for normal behavior when NATR is within limits
        return super().get_levels_to_execute()
