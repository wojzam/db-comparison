import time

import matplotlib.pyplot as plt
import numpy as np

DEFAULT_ITERATIONS = 5
TEST_ITERATIONS = 10
DEFAULT_MAX_ROWS = 1000


def varied_limits_results(db, func, func_before=lambda: None, func_after=lambda: None,
                          max_rows=DEFAULT_MAX_ROWS, iterations=DEFAULT_ITERATIONS):
    mean_times, std_times = [], []
    limits = generate_limits_list(max_rows)
    initial_limit = db.limit
    for limit in limits:
        db.limit = limit
        mean, std = timed_with_hooks(func, func_after, func_before, iterations)
        mean_times.append(mean)
        std_times.append(std)
    db.limit = initial_limit
    return limits, mean_times, std_times


def timed_with_hooks(func, func_after, func_before, iterations):
    times = []
    for _ in range(iterations):
        func_before()
        start_time = time.time()
        func()
        times.append(time.time() - start_time)
        func_after()
    return np.mean(times), np.std(times)


def result_timed(func, iterations=DEFAULT_ITERATIONS):
    result = None
    times = []
    for _ in range(iterations):
        start_time = time.time()
        result = func()
        times.append(time.time() - start_time)

    return result, np.mean(times), np.std(times)


def show_time_comparison_plot(title: str, results: dict):
    for label, (limit, mean, std) in results.items():
        plt.errorbar(limit, mean, yerr=std, label=label, fmt='-o')

    plt.xlabel('Limit')
    plt.ylabel('Execution Time (s)')
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.show()


def generate_limits_list(max_limit):
    limits = [int(x) for x in np.linspace(max_limit / 10, max_limit, 10, dtype=int)]
    if 100 < np.min(limits):
        limits = [100] + limits
    return limits
