import time


def timer(logger):
    def timer_decorator(method):
        def timer_func(*args, **kw):
            logger.info(f"Running query: {args[1]}")
            start_time = time.time()
            result = method(*args, **kw)
            duration = (time.time() - start_time)
            logger.info(f"Query executed in {duration:0.2f} seconds")
            return result

        return timer_func

    return timer_decorator
