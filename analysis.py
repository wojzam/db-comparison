import time

DEFAULT_ITERATIONS = 20


def result_timed(func, iterations=DEFAULT_ITERATIONS):
    result = None
    start_time = time.time()
    for _ in range(iterations):
        result = func()
    avg_time = (time.time() - start_time) / iterations
    return result, avg_time
