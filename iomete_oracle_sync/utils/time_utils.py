import logging
import time

logger = logging.getLogger(__name__)


def timer(message: str):
    def timer_decorator(method):
        def timer_func(*args, **kw):
            logger.info(f"{message} started")
            start_time = time.time()
            result = method(*args, **kw)
            duration = (time.time() - start_time)
            logger.info(f"{message} completed in {duration:0.2f} seconds")
            return result

        return timer_func

    return timer_decorator


def table_timer(message: str):
    def timer_decorator(method):
        def timer_func(*args, **kw):
            logger.info(f"{message} started")
            start_time = time.time()
            total_rows = method(*args, **kw)
            duration = (time.time() - start_time)
            logger.info(f"{message} completed in {duration:0.2f} seconds. Total rows: {total_rows}")
            return total_rows

        return timer_func

    return timer_decorator
