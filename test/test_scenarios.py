from datetime import timedelta
import pytest
from src.rlretry.rlretry import RLAgent, RLEnvironment, Action

ACTION_DURATIONS = {
    Action.ABRT: timedelta(seconds=0.0),
    Action.RETRY0: timedelta(seconds=1.0),
    Action.RETRY0_1: timedelta(seconds=1.1),
    Action.RETRY0_2: timedelta(seconds=1.2),
    Action.RETRY0_5: timedelta(seconds=1.5),
}


def test_scenario_1():
    def next_state(current_state: str, action: Action) -> str:
        return "PermaFail"

    timeout = timedelta(seconds=2)
    max_retries = 3
    success_reward = RLEnvironment.success_reward(timeout, max_retries)
    agent = RLAgent(0.0, initial_value=success_reward, alpha=0.1)
    environment = RLEnvironment(
        lambda: _,
        timeout,
        success_reward=success_reward,
        state_func=next_state,
    )

    current_state = "PermaFail"
    for _ in range(20):
        action = agent.choose_action(current_state)
        duration = ACTION_DURATIONS[action]
        previous_state = current_state
        current_state = next_state(current_state, action)
        reward = environment.next_state_to_reward(current_state, duration)
        agent.apply_reward(previous_state, action, reward)

    print("\n")
    print(success_reward)
    print(agent._state_action_map._initial_value)
    print(agent._state_action_map._counts_df)
    print(agent._state_action_map._df)
