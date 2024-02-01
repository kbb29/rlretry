from datetime import datetime, timedelta
import random
import time
from typing import Callable, Tuple
from enum import Enum
import pandas as pd


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


class RLRetryError(RuntimeError):
    def __init__(self, e: RuntimeError, msg: str = ""):
        super().__init__(msg)
        self.original_exception = e


class RLRetryTimeout(RLRetryError):
    pass


class RLRetryMaxRetries(RLRetryError):
    pass


class RLRetryAbort(RLRetryError):
    pass


class RLRetryNoException(RuntimeError):
    pass


def default_state_func(e: RuntimeError) -> str:
    cls = e.__class__
    return cls.__name__


class StateActionMap:
    def __init__(
        self,
        df: pd.DataFrame,
        counts_df: pd.DataFrame,
        initial_value: float = 0.0,
        alpha: float = 0.0,
    ):
        self._df = StateActionMap.default_df() if df.empty else df
        self._counts_df = StateActionMap.default_counts_df() if counts_df.empty else counts_df
        self._last_saved_df = self._df.copy(deep=True)
        self._last_saved_counts_df = self._counts_df.copy(deep=True)
        self._initial_value = initial_value
        self._alpha = alpha

    @staticmethod
    def default_df() -> pd.DataFrame:
        return pd.DataFrame(columns=list(Action), dtype=pd.Float32Dtype())

    @staticmethod
    def default_counts_df() -> pd.DataFrame:
        return pd.DataFrame(columns=list(Action))

    @staticmethod
    def random_action() -> Action:
        return random.choice(list(Action))

    def best_action(self, state) -> Action:
        if state not in self._df.index:
            return random.choice(list(Action))

        actions = self._df.loc[state]

        idx_of_best_action = actions.argmax()

        best_action = actions.index[idx_of_best_action]

        return best_action

    def load_weights(self, df: pd.DataFrame, counts_df: pd.DataFrame):
        self._df = df
        self._counts_df = counts_df

    def create_state(self, state: str):
        self._df.loc[state] = [self._initial_value for _ in list(Action)]
        self._counts_df.loc[state] = [0 for _ in list(Action)]

    def update_average_reward(self, state: str, action: Action, new_reward: float):
        if state not in self._df.index:
            self.create_state(state)
        count = self._counts_df.loc[state][action]
        current_average = self._df.loc[state][action]
        value_delta = new_reward - current_average
        # if alpha has been specified, use that as a recency weighting
        # otherwise use average reward
        if self._alpha > 0.0:
            value_delta *= self._alpha
        else:
            value_delta /= count + 1

        self._df.loc[state][action] += value_delta
        self._counts_df.loc[state][action] += 1

    def update_last_saved(self):
        self._last_saved_df = self._df.copy(deep=True)
        self._last_saved_counts_df = self._counts_df.copy(deep=True)


class RLAgent:
    @staticmethod
    def _default_weight_loader() -> Tuple[pd.DataFrame, pd.DataFrame]:
        return (StateActionMap.default_df(), StateActionMap.default_counts_df())

    @staticmethod
    def _default_weight_dumper(
        _df: pd.DataFrame,
        _counts_df: pd.DataFrame,
        _previous_df: pd.DataFrame,
        _previous_counts_df: pd.DataFrame,
    ):
        pass

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
        alpha: float = 0.0,
    ):
        self._state_action_map = StateActionMap(
            *weight_loader(), initial_value, alpha=alpha
        )
        self._eps = epsilon
        self._age = 0
        self._weight_loader = weight_loader
        self._weight_dumper = weight_dumper

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
        if self._age % 100 == 0:
            self.dump_weights()

        if random.random() < self._eps:
            print("agent choosing random action")
            return self._state_action_map.random_action()
        return self._state_action_map.best_action(state)

    def apply_reward(self, state, action, reward):
        self._state_action_map.update_average_reward(state, action, reward)


class RLEnvironment:
    @staticmethod
    def success_reward(max_wait: timedelta, max_retries: int) -> float:
        return 2 * (max_wait.total_seconds() + max_retries) + 1

    def __init__(
        self,
        func,
        max_wait: timedelta,
        success_reward: float,
        state_func=default_state_func,
    ):
        self._previous_action_start_time = datetime.utcnow()
        self._func = func
        self._state_func = state_func
        self.func_retval = None
        self._max_wait = max_wait
        self._success_reward = success_reward
        self.last_exception = RLRetryNoException("something has gone wrong")

    def run_func(self) -> str:
        try:
            self.func_retval = self._func()
            return "success"
        except RuntimeError as e:
            self.last_exception = e
            return self._state_func(e)

    def execute_action(self, state: str, action: Action):
        print(f"RLEnvironment execute_action({state}, {action})")
        previous_action_start_time = datetime.utcnow()
        reward = -1
        if action == Action.ABRT:
            # need to give -1 reward to the state/action
            next_state = "abort"
        else:
            if action == Action.RETRY0_1:
                time.sleep(self._max_wait.total_seconds() * 0.1)
            elif action == Action.RETRY0_2:
                time.sleep(self._max_wait.total_seconds() * 0.2)
            elif action == Action.RETRY0_5:
                time.sleep(self._max_wait.total_seconds() * 0.5)

            next_state = self.run_func()
            if next_state == "success":
                reward += self._success_reward

        reward -= (datetime.utcnow() - previous_action_start_time).total_seconds()

        return next_state, reward


def rlretry(
    max_retries: int = 5,
    timeout: timedelta = timedelta(seconds=300),
    state_func: Callable = default_state_func,
    epsilon: float = 0.1,
    weight_loader: Callable[
        [], Tuple[pd.DataFrame, pd.DataFrame]
    ] = RLAgent._default_weight_loader,
    weight_dumper: Callable[
        [pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame], None
    ] = RLAgent._default_weight_dumper,
    optimistic_initial_values: bool = True,
    alpha=0.0,
    raise_primary_exception=True,
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
    success_reward = RLEnvironment.success_reward(timeout, max_retries)
    initial_value = success_reward if optimistic_initial_values else 0.0


    def raise_rlexception(e: RLRetryError):
        raise e

    def raise_original_exception(e: RLRetryError):
        raise e.original_exception

    raise_exception = raise_original_exception if raise_primary_exception else raise_rlexception

    def decorator_no_args(func: Callable):
        agent = RLAgent(epsilon, weight_loader, weight_dumper, initial_value, alpha)

        def wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            environment = RLEnvironment(
                lambda: func(*args, **kwargs),
                timeout,
                success_reward,
                state_func=state_func,
            )
            current_state = environment.run_func()
            # if it works first time, then we don't have to do any RL stuff
            if current_state == "success":
                return environment.func_retval

            for _ in range(max_retries):
                if datetime.utcnow() - start_time > timeout:
                    raise_exception(RLRetryTimeout(environment.last_exception))
                action = agent.choose_action(current_state)
                previous_state = current_state
                current_state, reward = environment.execute_action(
                    current_state, action
                )
                agent.apply_reward(previous_state, action, reward)
                if current_state == "success":
                    return environment.func_retval
                elif current_state == "abort":
                    raise_exception(
                        RLRetryAbort(
                            environment.last_exception,
                            f"encountered a state in which RLRetry thinks it is not worth continuing {previous_state}",
                        )
                    )

            raise_exception(RLRetryMaxRetries(environment.last_exception))

        return wrapper

    return decorator_no_args
