# This will be returned every time for certain parameter values (like an HTTP 404 or 500)
# agent should abort immediately
from collections import deque
from datetime import datetime
import datetime as dt
import random

TIME_SPEEDUP = 20


def timedelta(seconds=0):
    return dt.timedelta(seconds=seconds / TIME_SPEEDUP)


def total_seconds(td: dt.timedelta):
    return td.total_seconds() * TIME_SPEEDUP


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

def mock_server_function(parameter: str) -> bool:
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