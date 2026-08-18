from typing import Optional, TypeVar

from pydantic import BaseModel

from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase

ExecutorConfigType = TypeVar("ExecutorConfigType", bound=ExecutorConfigBase)


class ExecutorAction(BaseModel):
    """
    Base class for bot actions.
    """
    controller_id: Optional[str] = "main"


class CreateExecutorAction(ExecutorAction):
    """
    Action to create an executor.
    """
    executor_config: ExecutorConfigType


class StopExecutorAction(ExecutorAction):
    """
    Action to stop an executor.
    """
    executor_id: str
    keep_position: Optional[bool] = False
    # When True, executor should shut down without issuing per-order cancels
    # (e.g. exchange orders already cleared by bulk cancel on refresh).
    skip_order_cancel: bool = False


class StoreExecutorAction(ExecutorAction):
    """
    Action to store an executor.
    """
    executor_id: str
