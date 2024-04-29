import time

import matplotlib.pyplot as plt
from numpy import mean, std, arange

DEFAULT_ITERATIONS = 5


def result_timed(func, iterations=DEFAULT_ITERATIONS):
    result = None
    times = []
    for _ in range(iterations):
        start_time = time.time()
        result = func()
        times.append(time.time() - start_time)

    return result, mean(times), std(times)


def show_time_comparison_plot(title, results: dict):
    x = arange(len(results))
    avg_times, std_devs = zip(*results.values())

    plt.errorbar(x, avg_times, yerr=std_devs, fmt='o')
    plt.xticks(x, results.keys())
    plt.xlabel('Database')
    plt.ylabel('Execution Time (s)')
    plt.title(title)
    plt.grid(True)
    plt.show()
