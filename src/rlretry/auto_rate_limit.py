from collections import defaultdict
from datetime import datetime, timedelta
import random
import time
from typing import Callable, Dict, Optional


def default_success_func(e: Exception) -> bool:
    return False


def get_optimum_interval(average_rewards: Dict[timedelta, float]) -> timedelta:
    sorted_intervals = sorted(average_rewards.items(), key=lambda x: x[1])
    return sorted_intervals[-1][0]


def get_random_interval(
    time_increment: timedelta, minimum: timedelta, maximum: timedelta
) -> timedelta:
    num_time_increments = random.randint(
        int(minimum / time_increment), int(maximum / time_increment)
    )
    return time_increment * num_time_increments


def get_epsilon_greedy_interval(
    average_rewards: Dict[timedelta, float],
    time_increment: timedelta,
    minimum: timedelta,
    maximum: timedelta,
    epsilon: float,
) -> timedelta:
    if random.random() < epsilon:
        return get_random_interval(time_increment, minimum, maximum)
    return get_optimum_interval(average_rewards)


def auto_request_interval(
    maximum: timedelta,
    minimum: timedelta = timedelta(seconds=0),
    time_increment: timedelta = timedelta(seconds=1),
    success_func: Callable[[Exception], bool] = default_success_func,
    alpha: float = 0.05,
    epsilon: float = 0.05,
):
    """
    A decorator that adjusts the query interval to find the optimum rate to repeat a function.
    seeks to minimize the average interval between successful queries (ie queries which do not raise an exception)
    """

    if minimum % time_increment != timedelta(seconds=0):
        raise ValueError("minimum must be a multiple of time_increment")

    if maximum % time_increment != timedelta(seconds=0):
        raise ValueError("maximum must be a multiple of time_increment")

    if maximum <= minimum:
        raise ValueError("maximum must be greater than minimum")

    def cropping_func(value: timedelta) -> timedelta:
        if value < minimum:
            return minimum
        if maximum is not None and value > maximum:
            return maximum
        return value

    def decorator_no_args(func: Callable):
        average_rewards = defaultdict(float)
        current_interval = minimum
        last_success_time = None
        last_request_time = None
        subsequent_failed_requests = 0
        n = 0

        def wrapper(*args, **kwargs):
            nonlocal last_request_time, last_success_time, average_rewards, alpha, subsequent_failed_requests, time_increment, current_interval, epsilon, n

            last_success_time = (
                datetime.utcnow() if last_success_time is None else last_success_time
            )
            last_request_time = (
                datetime.utcnow() if last_request_time is None else last_request_time
            )

            # wait until it is time to query
            sleep_duration = cropping_func(current_interval) - (
                datetime.utcnow() - last_request_time
            )

            print(f'waiting for {sleep_duration}')

            time.sleep(max(0, sleep_duration.total_seconds()))

            if n % 5 == 0:
                print('average rewards')
                for interval in sorted(average_rewards.keys()):
                    print (f'    {interval}: {average_rewards[interval]}')
            n += 1

            last_request_time = datetime.utcnow()

            is_success = False
            try:
                func(*args, **kwargs)
                is_success = True
            except Exception as e:
                is_success = success_func(e)

            now = datetime.utcnow()
            # update rewards
            if is_success:
                success_interval_seconds = (now - last_success_time).total_seconds()
                reward = -success_interval_seconds
                average_reward_delta = (
                    reward - average_rewards[current_interval]
                ) * alpha
                average_rewards[current_interval] += average_reward_delta
                last_success_time = datetime.utcnow()
                subsequent_failed_requests = 0
            else:
                fail_interval_seconds = (now - last_request_time).total_seconds()
                # this is just an adjustment to the reward so that we punish multiple failures
                average_rewards[current_interval] -= fail_interval_seconds * alpha
                subsequent_failed_requests += 1

            # now choose a new interval if required
            if is_success or subsequent_failed_requests:
                new_interval = get_epsilon_greedy_interval(
                    average_rewards, time_increment, minimum, maximum, epsilon
                )
                # force a change of interval if we had 10 successive failures
                if not is_success and new_interval == current_interval:
                    new_interval = current_interval + time_increment
                    if new_interval > maximum:
                        new_interval = maximum

                current_interval = new_interval
                print(f'updated current_interval to {current_interval}')

        return wrapper

    return decorator_no_args
