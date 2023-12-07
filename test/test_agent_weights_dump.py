from typing import Tuple
import pytest
import pandas as pd

from rlretry.rlretry import RLAgent, update_average, update_recency_weighted_average

q_saved = pd.DataFrame([[0]])
n_saved = pd.DataFrame([[0]])

def dummy_weight_dumper(q_curr: pd.DataFrame, n_curr: pd.DataFrame, q_prev: pd.DataFrame, n_prev: pd.DataFrame):
    global q_saved, n_saved
    if q_saved is None or n_saved is None:
        q_saved = q_curr
        n_saved = n_curr
        return

    q02 = q_saved
    n02 = n_saved

    q_saved, n_saved = update_average(q_prev, n_prev, q02, n02, q_curr, n_curr)

def dummy_weight_dumper_weighted(q_curr: pd.DataFrame, n_curr: pd.DataFrame, q_prev: pd.DataFrame, n_prev: pd.DataFrame):
    global q_saved, n_saved
    if q_saved is None or n_saved is None:
        q_saved = q_curr
        n_saved = n_curr
        return

    q02 = q_saved
    n02 = n_saved

    q_saved, n_saved = update_recency_weighted_average(q_prev, n_prev, q02, n02, q_curr, n_curr, 0.9)

def dummy_weight_loader() -> Tuple[pd.DataFrame, pd.DataFrame]:
    return q_saved, n_saved

@pytest.fixture
def agent1():
    return RLAgent(0.1, weight_dumper=dummy_weight_dumper, weight_loader=dummy_weight_loader)

@pytest.fixture
def agent2():
    return RLAgent(0.1, weight_dumper=dummy_weight_dumper, weight_loader=dummy_weight_loader)

@pytest.fixture
def agent_weighted():
    return RLAgent(0.1, weight_dumper=dummy_weight_dumper_weighted, weight_loader=dummy_weight_loader)

def test_multi_dump(agent1: RLAgent, agent2: RLAgent):

    global q_saved, n_saved
    data = list(range(21))

    batch0 = data[:7]
    batch1 = data[7:14]
    batch2 = data[14:]
    n0 = len(batch0)
    n1 = len(batch1)
    n2 = len(batch2)

    # agent1 records an average
    q0df = pd.DataFrame([[sum(batch0) / n0]])
    n0df = pd.DataFrame([[n0]])
    sam = agent1._state_action_map
    sam._df = q0df
    sam._counts_df = n0df

    agent1.dump_weights()

    assert n_saved.shape == (1,1)
    assert q_saved.shape == (1,1)
    assert n_saved[0][0] == n0
    assert q_saved[0][0] == sum(batch0) / n0

    # now a agent2 counts more averages
    sam2 = agent2._state_action_map
    sam2._df = pd.DataFrame([[sum(batch1) / n1]])
    sam2._counts_df = pd.DataFrame([[n1]])

    agent2.dump_weights()

    assert n_saved.shape == (1,1)
    assert q_saved.shape == (1,1)
    assert n_saved[0][0] == n0 + n1
    assert q_saved[0][0] == sum(batch0+batch1) / (n0 + n1)

    #now agent1 counts different averages on top of the batch from agent2
    sam._df = pd.DataFrame([[sum(batch0+batch2) / (n0 + n2)]])
    sam._counts_df = pd.DataFrame([[n0+n2]])

    agent1.dump_weights()

    assert n_saved.shape == (1,1)
    assert q_saved.shape == (1,1)
    assert n_saved[0][0] == n0 + n1 + n2
    assert q_saved[0][0] == sum(batch0 + batch1 + batch2) / (n0 + n1 + n2)
