import itertools
import random
import string
import time
from mock_server import mock_server_function, TIME_SPEEDUP
from rlretry import auto_request_interval
from datetime import timedelta


@auto_request_interval(timedelta(seconds=30 / TIME_SPEEDUP), time_increment=timedelta(seconds=1.0/TIME_SPEEDUP))
def repeat_me(parameter: str):
    return mock_server_function(parameter)


if __name__ == "__main__":
    for i in itertools.count():
        try:
            print("calling repeatable_function")
            rv = repeat_me(
                "".join(random.choices(string.ascii_lowercase, k=6))
            )
            print(f"{time.time()} returned {rv}")
        except RuntimeError as e:
            print(e.__class__, e)

