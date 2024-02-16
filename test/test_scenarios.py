from datetime import timedelta
from typing import Tuple
import pytest
import pytest_mock
from src.rlretry.rlretry import (
    RLAgent,
    RLEnvironment,
    Action,
    rlretry,
    RLRetryError,
    default_alpha_func,
)
import random

ACTION_DURATIONS = {
    Action.ABRT: timedelta(seconds=0.0),
    Action.RETRY0: timedelta(seconds=1.0),
    Action.RETRY0_1: timedelta(seconds=1.1),
    Action.RETRY0_2: timedelta(seconds=1.2),
    Action.RETRY0_5: timedelta(seconds=1.5),
}


@pytest.mark.parametrize("retries", [2, 3, 4])
@pytest.mark.parametrize("iterations", [10, 20, 50])  # [10, 20, 50, 100])
@pytest.mark.parametrize("timeout_int", [1, 2, 5, 10, 20])  # [1, 2, 5, 10, 20])
@pytest.mark.parametrize("fail_duration_int", [0.02, 0.1, 1])  # [0.02, 0.1, 1])
def test_scenario_1(mocker, retries, timeout_int, iterations, fail_duration_int):
    """
    this scenario tests that if we are faced with a state that always results in failure
    then ABRT ends up with the highest average reward
    """

    random.seed(0)

    def dummy_func():
        raise RuntimeError("badness ocurred")

    def mock_execute(enviro: RLEnvironment, action: Action) -> Tuple[str, float]:
        if action == Action.ABRT:
            # need to give -1 reward to the state/action
            next_state = "abort"
            duration = timedelta(seconds=0.01)
        else:
            next_state = enviro.run_func()
            duration = (
                timedelta(seconds=fail_duration_int)
                + enviro._max_wait * action.sleeptime()
            )

        reward = enviro.next_state_to_reward(next_state, duration)

        # print(f"mock_execute({next_state}, {action}) -> {next_state, reward}")

        return next_state, reward

    mocker.patch("src.rlretry.rlretry.RLEnvironment.execute_action", mock_execute)

    timeout = timedelta(seconds=timeout_int)

    wrapped_func, agent = rlretry(
        timeout=timeout,
        max_retries=retries,
        raise_primary_exception=False,
        alpha=default_alpha_func,
        optimistic_initial_values=True,
    )(dummy_func, return_agent=True)

    assert agent._state_action_map._counts_df.empty
    assert agent._state_action_map._df.empty

    for _ in range(iterations):
        try:
            wrapped_func()
        except RLRetryError:
            pass

    # print()
    # print(agent._state_action_map._df)
    # print(agent._state_action_map._counts_df)
    assert agent._state_action_map.best_action("RuntimeError") == Action.ABRT


@pytest.mark.parametrize("retries", [2, 3, 4])
@pytest.mark.parametrize("timeout_int", [1, 2, 5, 10, 20])
@pytest.mark.parametrize("iterations", [10, 20, 50, 100])
def test_scenario_2(mocker, retries, timeout_int, iterations):
    """
    test to make sure that aborting doesn't result in a higher average reward than
    waiting for ages and then getting success.
    """

    random.seed(0)

    def dummy_func():
        raise RuntimeError("badness ocurred")

    def mock_execute(enviro: RLEnvironment, action: Action) -> Tuple[str, float]:
        if action == Action.ABRT:
            # need to give -1 reward to the state/action
            next_state = "abort"
            duration = timedelta(seconds=0.01)
        else:
            next_state = enviro.run_func()
            if action == Action.RETRY0_5:
                next_state = "success"
            duration = timedelta(seconds=action.sleeptime() + 0.01)

        reward = enviro.next_state_to_reward(next_state, duration)

        # print(f"mock_execute({next_state}, {action}) -> {next_state, reward}")

        return next_state, reward

    mocker.patch("src.rlretry.rlretry.RLEnvironment.execute_action", mock_execute)

    timeout = timedelta(seconds=timeout_int)

    wrapped_func, agent = rlretry(
        timeout=timeout,
        max_retries=retries,
        raise_primary_exception=False,
        alpha=default_alpha_func,
        optimistic_initial_values=True,
    )(dummy_func, return_agent=True)

    assert agent._state_action_map._counts_df.empty
    assert agent._state_action_map._df.empty

    for _ in range(iterations):
        try:
            wrapped_func()
        except RLRetryError:
            pass

    # print()
    # print(agent._state_action_map._df)
    # print(agent._state_action_map._counts_df)
    assert agent._state_action_map.best_action("RuntimeError") == Action.RETRY0_5
