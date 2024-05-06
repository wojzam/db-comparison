import time

import matplotlib.pyplot as plt
import numpy as np

DEFAULT_ITERATIONS = 5
TEST_ITERATIONS = 10
LIMITS = [100] + [x for x in range(1000, 10001, 1000)]


def varied_limits_results(db, query):
    mean_times = []
    std_times = []
    initial_limit = db.limit
    for limit in LIMITS:
        db.limit = limit
        mean, std = result_timed(getattr(db, query), TEST_ITERATIONS)[1:]
        mean_times.append(mean)
        std_times.append(std)
    db.limit = initial_limit
    return LIMITS, mean_times, std_times


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
