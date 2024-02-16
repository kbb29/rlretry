from __future__ import annotations
from datetime import datetime, timedelta
import math
import random
import time
from typing import Callable, Tuple, Union
from enum import Enum
import pandas as pd
import logging

log = logging.Logger(__name__)


def update_average(
    q0: pd.DataFrame,
    n0: pd.DataFrame,
    q01: pd.DataFrame,
    n01: pd.DataFrame,
    q02: pd.DataFrame,
    n02: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    calculate the total average from two different estimates of the average"""
    denom = n01 + n02 - n0
    return q01 * n01 / denom + q02 * n02 / denom - q0 * n0 / denom, denom


def update_recency_weighted_average(
    q0: pd.DataFrame,
    n0: pd.DataFrame,
    q01: pd.DataFrame,
    n01: pd.DataFrame,
    q02: pd.DataFrame,
    n02: pd.DataFrame,
    alpha: float,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # calculate the average reward between q02 and q0 and then replay this average reward onto q01
    k = n02 - n0

    counts = n01 + n02 - n0
    new_reward = (1 - alpha) ** k * q01 + (q02 - q0 * (1 - alpha) ** k)

    return new_reward, counts


class Action(Enum):
    ABRT = 0
    RETRY0 = 1
    RETRY0_1 = 2
    RETRY0_2 = 3
    RETRY0_5 = 4

    def sleeptime(self) -> float:
        if self == Action.RETRY0_1:
            return 0.1
        elif self == Action.RETRY0_2:
            return 0.2
        elif self == Action.RETRY0_5:
            return 0.5
        return 0


class RLRetryError(RuntimeError):
    def __init__(self, msg: str = ""):
        super().__init__(msg)


class RLRetryTimeout(RLRetryError):
    pass


class RLRetryMaxRetries(RLRetryError):
    pass


class RLRetryAbort(RLRetryError):
    pass


class RLRetryNoException(RuntimeError):
    pass


def default_state_func(e: Exception) -> str:
    return e.__class__.__name__


def default_alpha_func(n: int) -> float:
    """
    n is the count of attempts made so far for this state/action pair
    """
    if n > 5:
        return 0.05
    return 0.5 - n * 0.45 / 6


class StateActionMap:
    def __init__(
        self,
        df: pd.DataFrame,
        counts_df: pd.DataFrame,
        initial_value: float = 0.0,
        alpha: Union[float, None, Callable[[int], float]] = 0.0,
    ):
        self._df = StateActionMap.default_df() if df is None or df.empty else df
        self._counts_df = (
            StateActionMap.default_counts_df()
            if counts_df is None or counts_df.empty
            else counts_df
        )
        self._last_saved_df = self._df.copy(deep=True)
        self._last_saved_counts_df = self._counts_df.copy(deep=True)
        self._initial_value = initial_value

        if callable(alpha):
            self._alpha = alpha
        elif isinstance(alpha, float):
            self._alpha = lambda _: alpha
        else:
            self._alpha = None

    @staticmethod
    def default_df() -> pd.DataFrame:
        return pd.DataFrame(columns=list(Action), dtype=pd.Float32Dtype())

    @staticmethod
    def default_counts_df() -> pd.DataFrame:
        return pd.DataFrame(columns=list(Action))

    @staticmethod
    def random_action() -> Action:
        # don't ever choose ABRT as a random action
        # the reward doesn't change so we don't need to explore it
        return random.choice(list(Action)[1:])

    def randomish_action(self, state: str) -> Action:
        # don't ever choose ABRT as a random action
        # the reward doesn't change so we don't need to explore it

        # choose an action at random, but prefer those which have been tried the least

        # if we have no counts yet, just choose randomly
        if self._counts_df.empty:
            return self.__class__.random_action()

        # to make the weights take the inverse of the count (we want a low count to mean a high probability of being chosen)
        # take the log of the counts too so that small numbers have more impact. ie.
        #   if an action has been chosen once it should be significantly more likely than an option chosen 10 times
        #   but an option chosen 100 times should be about the same as an option chosen 1000 times
        #   bascially, in the long run, the weights will even out at roughly even
        #   but at the start, infrequently chosen options will be boosted
        possible_actions = list(Action)[1:]
        weights = [
            1 / math.log(2 + self._counts_df[action][state])
            for action in possible_actions
        ]
        return random.choices(possible_actions, weights=weights)[0]


    def best_action(self, state) -> Action:
        if state not in self._df.index:
            return random.choice(list(Action))

        actions = self._df.loc[state]

        idx_of_best_action = actions.argmax()

        best_action = actions.index[idx_of_best_action]

        return best_action

    def create_state(self, state: str):
        # always set ABRT to have a reward of 1, regardless of other settings.
        self._df.loc[state] = [1.0] + [
            float(self._initial_value) for _ in list(Action)[1:]
        ]
        self._counts_df.loc[state] = [0 for _ in list(Action)]

    def update_average_reward(self, state: str, action: Action, new_reward: float):
        if state not in self._df.index:
            self.create_state(state)
        count = self._counts_df[action][state]
        current_average = self._df[action][state]
        value_delta = new_reward - current_average
        # if alpha has been specified, use that as a recency weighting
        # otherwise use average reward
        if self._alpha is not None:
            value_delta *= self._alpha(count)
        else:
            value_delta /= count + 1

        self._df[action][state] += value_delta
        self._counts_df[action][state] += 1

    def update_last_saved(self):
        self._last_saved_df = self._df.copy(deep=True)
        self._last_saved_counts_df = self._counts_df.copy(deep=True)


def _default_weight_loader() -> Tuple[pd.DataFrame, pd.DataFrame]:
    return (StateActionMap.default_df(), StateActionMap.default_counts_df())


def _default_weight_dumper(
    _df: pd.DataFrame,
    _counts_df: pd.DataFrame,
    _previous_df: pd.DataFrame,
    _previous_counts_df: pd.DataFrame,
):
    pass


class RLAgent:

    def __init__(
        self,
        epsilon: float,
        weight_loader: Callable[
            [], Tuple[pd.DataFrame, pd.DataFrame]
        ] = _default_weight_loader,
        weight_dumper: Callable[
            [pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame], None
        ] = _default_weight_dumper,
        initial_value: float = 0.0,
        alpha: Union[float, None, Callable[[int], float]] = 0.0,
        dump_interval: int = 100,
    ):
        self._state_action_map = StateActionMap(
            *weight_loader(), initial_value, alpha=alpha
        )
        self._eps = epsilon
        self._age = 0
        self._weight_loader = weight_loader
        self._weight_dumper = weight_dumper
        self._dump_interval = dump_interval

    def dump_weights(self):
        self._weight_dumper(
            self._state_action_map._df,
            self._state_action_map._counts_df,
            self._state_action_map._last_saved_df,
            self._state_action_map._last_saved_counts_df,
        )
        self._state_action_map.update_last_saved()

    def choose_action(self, state: str) -> Action:
        self._age += 1
        if self._age % self._dump_interval == 0:
            self.dump_weights()

        if random.random() < self._eps:
            log.debug("agent choosing random action")
            return self._state_action_map.randomish_action(state)
        return self._state_action_map.best_action(state)

    def apply_reward(self, state, action, reward):
        self._state_action_map.update_average_reward(state, action, reward)


class RLEnvironment:

    def __init__(
        self,
        func,
        max_wait: timedelta,
        state_func: Callable[[Exception], str] = default_state_func,
    ):
        self._previous_action_start_time = datetime.utcnow()
        self._func = func
        self._state_func = state_func
        self.func_retval = None
        self._max_wait = max_wait
        self.last_exception = RLRetryNoException("something has gone wrong")

    def run_func(self) -> str:
        try:
            self.func_retval = self._func()
            return "success"
        except Exception as e:
            self.last_exception = e
            return self._state_func(e)

    def next_state_to_reward(self, next_state: str, duration: timedelta) -> float:
        if next_state == "success":
            return 2.5 - duration / self._max_wait
        return 1 - duration / self._max_wait

    def execute_action(self, action: Action) -> Tuple[str, float]:
        log.debug(f"RLEnvironment execute_action({action})")
        previous_action_start_time = datetime.utcnow()

        next_state = self.run_func() if action != Action.ABRT else "abort"

        time.sleep(self._max_wait.total_seconds() * action.sleeptime())

        reward = self.next_state_to_reward(
            next_state, datetime.utcnow() - previous_action_start_time
        )

        return next_state, reward


def rlretry(
    max_retries: int = 5,
    timeout: timedelta = timedelta(seconds=300),
    state_func: Callable = default_state_func,
    epsilon: float = 0.1,
    weight_loader: Callable[
        [], Tuple[pd.DataFrame, pd.DataFrame]
    ] = _default_weight_loader,
    weight_dumper: Callable[
        [pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame], None
    ] = _default_weight_dumper,
    dump_interval: int = 100,
    optimistic_initial_values: bool = True,
    alpha: Union[float, None, Callable[[int], float]] = default_alpha_func,
    raise_primary_exception=False,
):
    """
    A decorator that performs retries on a function if it throws an exceptions.

    It uses reinforcement learning to determine the best course of action to take on any given exception
    It will either abort, retry immediately or retry after one of 4 time periods (which are scaled relative to timeout)


    :param max_retries: the maximum number of retries to perform
    :param timeout: the maximum time to allow for all retries
    :param state_func: a function that accepts an exception and returns a string which is the name of a state (in RL parlance).  The default_state_func uses the name of the exception class as the state.
    :param epsilon: determines how much exploration to do.  Set to zero to always use the best option (but slows down learning).  Set to 1 to always use a random option.  Ideally set it somewhere in between
    :param weight_loader loads the weights (ie. the probabilities that certain actions will be chosen)
    :param weight_dumper a function that saves the weights somewhere (so that you don't start from the beginning next time it runs)
    """
    initial_value = 1 if optimistic_initial_values else 0.0

    def raise_rlexception(e: RLRetryError, original_exception: Exception):
        raise e from original_exception

    def raise_original_exception(_e: RLRetryError, original_exception: Exception):
        raise original_exception

    raise_exception = (
        raise_original_exception if raise_primary_exception else raise_rlexception
    )

    def decorator_no_args(
        func: Callable, return_agent: bool = False
    ) -> Union[Callable, Tuple[Callable, RLAgent]]:
        agent = RLAgent(
            epsilon, weight_loader, weight_dumper, initial_value, alpha, dump_interval
        )

        def wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            environment = RLEnvironment(
                lambda: func(*args, **kwargs),
                timeout,
                state_func=state_func,
            )
            current_state = environment.run_func()
            # if it works first time, then we don't have to do any RL stuff
            if current_state == "success":
                return environment.func_retval

            for _ in range(max_retries):
                if datetime.utcnow() - start_time > timeout:
                    raise_exception(RLRetryTimeout(), environment.last_exception)
                action = agent.choose_action(current_state)
                previous_state = current_state
                current_state, reward = environment.execute_action(action)
                agent.apply_reward(previous_state, action, reward)
                if current_state == "success":
                    return environment.func_retval
                elif current_state == "abort":
                    raise_exception(
                        RLRetryAbort(
                            f"encountered a state in which RLRetry thinks it is not worth continuing {previous_state}",
                        ),
                        environment.last_exception,
                    )

            raise_exception(RLRetryMaxRetries(), environment.last_exception)

        if return_agent:
            return wrapper, agent
        return wrapper

    return decorator_no_args
