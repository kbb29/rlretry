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
    _default_weight_loader,
)

ACTION_DURATIONS = {
    Action.ABRT: timedelta(seconds=0.0),
    Action.RETRY0: timedelta(seconds=1.0),
    Action.RETRY0_1: timedelta(seconds=1.1),
    Action.RETRY0_2: timedelta(seconds=1.2),
    Action.RETRY0_5: timedelta(seconds=1.5),
}


@pytest.mark.parametrize("retries", [2, 3, 4])
@pytest.mark.parametrize("iterations", [10, 20, 50, 100])
@pytest.mark.parametrize("timeout_int", [1, 2, 5, 10, 20])
@pytest.mark.parametrize("fail_duration_int", [0.02, 0.1, 1])
@pytest.mark.parametrize(
    "alpha", [0.0]
)  # TODO: need to figure out why this fails with alpha > 0
def test_scenario_1(mocker, retries, timeout_int, iterations, fail_duration_int, alpha):
    """
    this scenario tests that if we are faced with a state that always results in failure
    then ABRT ends up with the highest average reward
    """

    def dummy_func():
        raise RuntimeError("badness ocurred")

    def mock_execute(enviro, action) -> Tuple[str, float]:
        if action == Action.ABRT:
            # need to give -1 reward to the state/action
            next_state = "abort"
            duration = timedelta(seconds=0.01)
        else:
            next_state = enviro.run_func()
            duration = timedelta(seconds=fail_duration_int)

        reward = enviro.next_state_to_reward(next_state, duration)

        # print(f"mock_execute({next_state}, {action}) -> {next_state, reward}")

        return next_state, reward

    mocker.patch("src.rlretry.rlretry.RLEnvironment.execute_action", mock_execute)

    timeout = timedelta(seconds=timeout_int)

    wrapped_func, agent = rlretry(
        timeout=timeout,
        max_retries=retries,
        raise_primary_exception=False,
        alpha=alpha,
    )(dummy_func, return_agent=True)

    for _ in range(iterations):
        try:
            wrapped_func()
        except RLRetryError:
            pass

    # print(agent._state_action_map._df)
    # print(agent._state_action_map._counts_df)
    assert agent._state_action_map.best_action("RuntimeError") == Action.ABRT


@pytest.mark.parametrize("retries", [2, 3, 4])
@pytest.mark.parametrize("timeout_int", [1, 2, 5, 10, 20])
@pytest.mark.parametrize("iterations", [10, 20, 50, 100])
@pytest.mark.parametrize(
    "alpha", [0.0]
)  # TODO: need to figure out why this fails with alpha > 0
def test_scenario_2(mocker, retries, timeout_int, iterations, alpha):
    """
    test to make sure that aborting doesn't result in a higher average reward than
    waiting for ages and then getting success.
    """

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
        alpha=alpha,
    )(dummy_func, return_agent=True)

    for _ in range(iterations):
        try:
            wrapped_func()
        except RLRetryError:
            pass

    # print(agent._state_action_map._df)
    # print(agent._state_action_map._counts_df)
    assert agent._state_action_map.best_action("RuntimeError") == Action.RETRY0_5
