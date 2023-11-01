# This will be returned every time for certain parameter values (like an HTTP 404 or 500)
# agent should abort immediately
from collections import deque
from datetime import datetime
import datetime as dt
import itertools
import pathlib
import random
import string
import time
from typing import Callable, Optional, Tuple
import pandas as pd
from rlretry import rlretry, update_average

TIME_SPEEDUP = 30


def timedelta(seconds=0):
    return dt.timedelta(seconds=seconds / TIME_SPEEDUP)


def total_seconds(td: dt.timedelta):
    return td.total_seconds() * TIME_SPEEDUP


def mock_state_function(e: RuntimeError) -> str:
    cls = e.__class__
    if cls in [RandomFailure, TooBusyFailure, ClusteredFailure, RepeatableFailure]:
        return cls.__name__
    raise e


class RepeatableFailure(RuntimeError):
    pass


# This will be returned with low probablity for any request (like an HTTP 504)
# agent should retry immediately
class RandomFailure(RuntimeError):
    THRESHOLD = 0.02


RUN_START = datetime.utcnow()


# this will be returned with high probability for any request during a fixed period (eg. for 1 minute in every 5).
# like a 502 or some short-term outage
# agent should retry after a suitable time period
class ClusteredFailure(RuntimeError):
    @classmethod
    def is_outage(cls) -> bool:
        run_seconds = total_seconds(datetime.utcnow() - RUN_START)
        # this will be for 30s every 5 minutes, ie. 10% of the time
        # optimium retry period should be 15-30 seconds
        return (run_seconds % 300) < 30


# this will be returned when we have given it too many requests in a particular time window
# like a 429
# agent should retry after a suitable time period
# I think 15s is probably optimal if we are sending 100 requests in 45s.
# TODO, when we get lots of these, regularly we should try to figure out
# the optimal period for making requests so that we don't get 429s in the first place
class TooBusyFailure(RuntimeError):
    N = 100
    LAST_N_REQUEST_TIMES = deque(maxlen=N)
    WINDOW = timedelta(seconds=60)

    @classmethod
    def is_too_busy(cls) -> bool:
        cls.LAST_N_REQUEST_TIMES.append(datetime.utcnow())
        if len(cls.LAST_N_REQUEST_TIMES) >= cls.N:
            n_requests_ago = cls.LAST_N_REQUEST_TIMES.popleft()
            return datetime.utcnow() - n_requests_ago < cls.WINDOW
        return False

def create_weights_loader_function(
    weights_dir: pathlib.Path,
) -> Callable[[], Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]]:
    def load_weights_from_file() -> (
        Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]
    ):
        df_pickle_path = weights_dir / "df.pickle"
        counts_df_pickle_path = weights_dir / "counts_df.pickle"
        if df_pickle_path.exists() and counts_df_pickle_path.exists():
            return (
                pd.read_pickle(weights_dir / "df.pickle"),
                pd.read_pickle(weights_dir / "counts_df.pickle")
            )
        else:
            return None, None
    return load_weights_from_file


def create_weights_dumper_function(
    weights_dir: pathlib.Path, distributed: bool = False
) -> Callable[[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame], None]:
    # if this is running single threaded and there is only one process dumping weights
    # to the file, then we can just save them directly
    weights_dir.mkdir(exist_ok=True)

    def dump_weights_to_file(
        df: pd.DataFrame,
        counts_df: pd.DataFrame,
        _previous_df: pd.DataFrame,
        _previous_counts_df: pd.DataFrame,
    ):
        df.to_pickle(weights_dir / "df.pickle")
        counts_df.to_pickle(weights_dir / "counts_df.pickle")

    if not distributed:
        return dump_weights_to_file

    weights_loader_function = create_weights_loader_function(weights_dir)
    def dump_weights_to_file_distributed(
        df: pd.DataFrame,
        counts_df: pd.DataFrame,
        previous_df: pd.DataFrame,
        previous_counts_df: pd.DataFrame,
    ):
        latest_saved_df, latest_saved_counts_df = weights_loader_function()
        new_df, new_counts_df = update_average(
            previous_df,
            previous_counts_df,
            df,
            counts_df,
            latest_saved_df,
            latest_saved_counts_df,
        )

    return dump_weights_to_file_distributed

WEIGHTS_PATH = pathlib.Path('/tmp/rlretry')

@rlretry(
    state_func=mock_state_function,
    timeout=timedelta(seconds=300),
    weight_loader=create_weights_loader_function(WEIGHTS_PATH),
    weight_dumper=create_weights_dumper_function(WEIGHTS_PATH),
)
def retryable_function(parameter: str) -> bool:
    """
    this function is a dummy for a function that eg. sends a query to a server

    In a real scenario, this function would sent a requests.Request to a server and
    if the request failed it would raise one of

    ConnectionError
    ProxyError
    HTTPError

    Instead of querying a server, we will just raise suitable exceptions
    """

    if random.random() < RandomFailure.THRESHOLD:
        print(f"retryable_function({parameter}) raising RandomFailure")
        raise RandomFailure()
    if ClusteredFailure.is_outage():
        print(f"retryable_function({parameter}) raising ClusteredFailure")
        raise ClusteredFailure()
    if TooBusyFailure.is_too_busy():
        print(f"retryable_function({parameter}) raising TooBusyFailure")
        raise TooBusyFailure()
    if parameter[0] == "k":
        print(f"retryable_function({parameter}) raising RepeatableFailure")
        raise RepeatableFailure()

    return True


if __name__ == "__main__":
    for i in itertools.count():
        try:
            print("calling retryable_function")
            rv = retryable_function(
                "".join(random.choices(string.ascii_lowercase, k=6))
            )
            print(f"returned {rv}")
        except RuntimeError as e:
            print(e.__class__, e)

        time.sleep(0.45 / TIME_SPEEDUP)
