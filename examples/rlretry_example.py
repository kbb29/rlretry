from datetime import timedelta
import itertools
import pathlib
import random
import string
import time
from typing import Callable, Optional, Tuple
import pandas as pd

from rlretry import rlretry, update_average
from mock_server import ClusteredFailure, RandomFailure, RepeatableFailure, TooBusyFailure, mock_server_function, TIME_SPEEDUP

WEIGHTS_PATH = pathlib.Path("/tmp/rlretry")


def mock_state_function(e: RuntimeError) -> str:
    cls = e.__class__
    if cls in [RandomFailure, TooBusyFailure, ClusteredFailure, RepeatableFailure]:
        return cls.__name__
    raise e


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
                pd.read_pickle(weights_dir / "counts_df.pickle"),
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


@rlretry(
    state_func=mock_state_function,
    timeout=timedelta(seconds=300),
    weight_loader=create_weights_loader_function(WEIGHTS_PATH),
    weight_dumper=create_weights_dumper_function(WEIGHTS_PATH),
)
def retryable_function(parameter: str) -> bool:
    return mock_server_function(parameter)


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
