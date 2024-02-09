import pytest
import pandas as pd

from src.rlretry.rlretry import update_average, update_recency_weighted_average

@pytest.mark.parametrize(
        "batch0,batch1,batch2",
        [
            (list(range(9)), list(range(6, 18)), list(range(23, 36, 2))),
            ([], list(range(6, 18)), list(range(23, 36, 2))),
            (list(range(9)), [], list(range(23, 36, 2))),
            (list(range(9)), list(range(6, 18)), []),
            (list(range(9)), [], []),
            ([], list(range(6, 18)), []),
            (list(range(9)), [], []),
        ]
)
def test_3way_average(batch0, batch1, batch2):

    ntotal = len(batch0) + len(batch1) + len(batch2)

    n0 = len(batch0)
    q0 = sum(batch0) / n0 if n0 > 0 else 0
    n01 = len(batch0) + len(batch1)
    n02 = len(batch0) + len(batch2)
    q01 = sum(batch0 + batch1) / n01 if n01 > 0 else 0
    q02 = sum(batch0 + batch2) / n02 if n02 > 0 else 0

    actual_average, actual_count = update_average(q0, n0, q01, n01, q02, n02)

    assert actual_count == ntotal
    assert actual_average == pytest.approx(sum(batch0 + batch1 + batch2) / ntotal)

    q0_df = pd.DataFrame(((q0,q0),(q0, q0)))
    n0_df = pd.DataFrame(((n0,n0), (n0, n0)))
    q01_df = pd.DataFrame(((q01,q01), (q01,q01)))
    n01_df = pd.DataFrame(((n01,n01), (n01,n01)))
    q02_df = pd.DataFrame(((q02,q02),(q02,q02)))
    n02_df = pd.DataFrame(((n02,n02),(n02,n02)))

    actual_average, actual_count = update_average(q0_df, n0_df, q01_df, n01_df, q02_df, n02_df)

    assert actual_average.shape == (2,2)
    assert actual_count.shape == (2,2)

    num_elems = actual_average.size

    assert list(actual_count.values.flatten()) == [ntotal] * num_elems
    assert list(actual_average.values.flatten()) == [pytest.approx(sum(batch0 + batch1 + batch2) / ntotal)] * num_elems


@pytest.mark.parametrize(
        "batch0,batch1,batch2",
        [
            (list(range(9)), list(range(6, 18)), list(range(23, 36, 2))),
            ([], list(range(6, 18)), list(range(23, 36, 2))),
            (list(range(9)), [], list(range(23, 36, 2))),
            (list(range(9)), list(range(6, 18)), []),
            (list(range(9)), [], []),
            ([], list(range(6, 18)), []),
            (list(range(9)), [], []),
        ]
)

def calculate_weighted_average_iter(start_q, r_values, alpha):
    q = start_q
    for r in r_values:
        q += (r - q) * alpha
    return q

def calculate_weighted_average(start_q, r_values, alpha):
    k = len(r_values)
    alpha_series = [r*(1-alpha)**(k-i-1) for i,r in enumerate(r_values)]
    alpha_sum = sum(alpha_series)
    return start_q*(1-alpha)**k + alpha * alpha_sum

def test_calculate_weighted_average():
    # test extreme alpha values
    assert calculate_weighted_average_iter(0.0, [5,5,5,5], 0) == 0
    assert calculate_weighted_average(0, [5,5,5,5], 0) == 0
    assert calculate_weighted_average_iter(0.0, [5,5,5,7], 1) == 7
    assert calculate_weighted_average(0, [5,5,5,7], 1) == 7


    assert calculate_weighted_average_iter(4.0, [4], 0.1) == 4.0
    assert calculate_weighted_average(4.0, [4], 0.1) == 4.0

    assert calculate_weighted_average_iter(4.0, [4,4], 0.1) == 4.0
    assert calculate_weighted_average(4.0, [4,4], 0.1) == 4.0

    assert calculate_weighted_average_iter(4.0, [4,4,4, 4], 0.1) == 4.0
    assert calculate_weighted_average(4.0, [4,4,4,4], 0.1) == 4.0


    assert calculate_weighted_average_iter(5.0, [5,5,5,5], 0.1) == 5.0
    assert calculate_weighted_average(5.0, [5,5,5,5], 0.1) == 5.0

    assert calculate_weighted_average(4.0, [4]*9, 0.1) == pytest.approx(4.0)
    assert calculate_weighted_average_iter(4.0, [4]*9, 0.1) == pytest.approx(4.0)

    assert calculate_weighted_average_iter(0.0, [4]*9, 0.9) == pytest.approx(4.0)
    assert calculate_weighted_average(0.0, [4]*9, 0.9) == pytest.approx(4.0)

@pytest.mark.parametrize('alpha', [(0.1),(0.5),(0.9)])
@pytest.mark.parametrize(
        "batch0,batch1,batch2",
        [
            (list(range(9)), list(range(6, 18)), list(range(23, 36, 2))),
            ([], list(range(6, 18)), list(range(23, 36, 2))),
            (list(range(9)), [], list(range(23, 36, 2))),
            (list(range(9)), list(range(6, 18)), []),
            (list(range(9)), [], []),
            ([], list(range(6, 18)), []),
            (list(range(9)), [], []),
        ]
)
def test_3way_average_weighted(batch0, batch1, batch2, alpha):
    ntotal = len(batch0) + len(batch1) + len(batch2)

    n0 = len(batch0)
    q0 = calculate_weighted_average_iter(0.0, batch0, alpha)
    n01 = len(batch0) + len(batch1)
    n02 = len(batch0) + len(batch2)
    q01 = calculate_weighted_average(q0, batch1, alpha)
    q02 = calculate_weighted_average(q0, batch2, alpha)

    # print(f'test_3way_average_weighted {q0} {n0} | {q01} {n01} | {q02} {n02}')

    actual_average, actual_count = update_recency_weighted_average(q0, n0, q01, n01, q02, n02, alpha)

    assert actual_count == ntotal
    assert actual_average == pytest.approx(calculate_weighted_average(0, batch0 + batch1 + batch2, alpha))

    q0_df = pd.DataFrame(((q0,q0),(q0, q0)))
    n0_df = pd.DataFrame(((n0,n0), (n0, n0)))
    q01_df = pd.DataFrame(((q01,q01), (q01,q01)))
    n01_df = pd.DataFrame(((n01,n01), (n01,n01)))
    q02_df = pd.DataFrame(((q02,q02),(q02,q02)))
    n02_df = pd.DataFrame(((n02,n02),(n02,n02)))

    actual_average, actual_count = update_recency_weighted_average(q0_df, n0_df, q01_df, n01_df, q02_df, n02_df, alpha)

    assert actual_average.shape == (2,2)
    assert actual_count.shape == (2,2)

    num_elems = actual_average.size

    assert list(actual_count.values.flatten()) == [ntotal] * num_elems
    assert list(actual_average.values.flatten()) == [pytest.approx(calculate_weighted_average(0, batch0 + batch1 + batch2, alpha))] * num_elems

