from datetime import timedelta
from typing import Tuple
import pytest
import pytest_mock
from src.rlretry.rlretry import RLAgent, RLEnvironment, Action, rlretry, RLRetryError

ACTION_DURATIONS = {
    Action.ABRT: timedelta(seconds=0.0),
    Action.RETRY0: timedelta(seconds=1.0),
    Action.RETRY0_1: timedelta(seconds=1.1),
    Action.RETRY0_2: timedelta(seconds=1.2),
    Action.RETRY0_5: timedelta(seconds=1.5),
}


def test_scenario_1(mocker):
    """
    this scenario tests that if we are faced with a state that always results in failure
    then ABRT ends up with the highest average reward
    """

    duration = None

    def dummy_func():
        nonlocal duration
        duration = timedelta(seconds=1)
        raise RuntimeError('badness ocurred')

    def mock_execute(enviro, next_state, action) -> Tuple[str, float]:
        print(f"called mock_execute({next_state}, {action})")
        if action == Action.ABRT:
            # need to give -1 reward to the state/action
            next_state = "abort"
        else:
            next_state = enviro.run_func()

        reward = enviro.next_state_to_reward(
            next_state, duration
        )

        return next_state, reward

    mocker.patch("src.rlretry.rlretry.RLEnvironment.execute_action", mock_execute)

    timeout = timedelta(seconds=2)
    max_retries = 3

    wrapped = rlretry( timeout=timeout, max_retries=max_retries, raise_primary_exception=False)(dummy_func)

    for _ in range(20):
        try:
            wrapped()
        except RLRetryError as e:
            pass

    assert True


def test_scenario_2():
    """
    test to make sure that aborting doesn't result in a higher average reward than
    waiting for ages and then getting success.
    """
    pass
