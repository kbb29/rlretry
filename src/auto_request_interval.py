from datetime import datetime, timedelta
import random
import time
from typing import Callable, Optional, Tuple
from enum import Enum
import pandas as pd


def default_success_func(e: Exception) -> bool:
    return False


def auto_request_interval(
    alpha: float = 0.05,
    minimum: timedelta = timedelta(seconds=0),
    maximum: Optional[timedelta] = None,
    success_func: Callable[[Exception], bool] = default_success_func,
):
    """
    A decorator that adjusts the query interval to find the optimum rate to repeat a function.
    seeks to minimize the average interval between successful queries (ie queries which do not raise an exception)
    """

    def decorator_no_args(func: Callable):
        average_success_interval = timedelta(seconds=0)
        last_success_time = None
        last_request_time = None

        def wrapper(*args, **kwargs):
            last_success_time = (
                datetime.utcnow() if last_success_time is None else last_success_time
            )
            last_request_time = datetime.utcnow() if last_request_time is None else last_request_time

            # wait until it is time to query
            sleep_duration = average_success_interval - (datetime.utcnow() - last_request_time)
            time.sleep(sleep_duration.total_seconds())

            is_success = False
            try:
                func(*args, **kwargs)
                is_success = True
            except Exception as e:
                is_success = success_func(e)

            if is_success:
                now = datetime.utcnow()
                success_interval = now - last_success_time
                # update the average success interval using alpha as a weighting factor
                average_success_interval += (
                    success_interval - average_success_interval
                ) * alpha
                last_success_time = datetime.utcnow()

        return wrapper

    return decorator_no_args
