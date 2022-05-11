import structlog, time

logger = structlog.get_logger("Query Scheduler")


def timeit(method):
    def timed(*args, **kw):
        start_time = time.time()
        result = method(*args, **kw)
        duration = (time.time() - start_time)
        logger.info(f'Completed in {duration} seconds')
        return result

    return timed
