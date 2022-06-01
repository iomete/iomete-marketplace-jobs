import logging

import time

logger = logging.getLogger(__name__)


def timeit(method):
    def timed(*args, **kw):
        logger.info(f"Running query: {args[1]}")
        start_time = time.time()
        result = method(*args, **kw)
        duration = (time.time() - start_time)
        logger.info(f"Query executed in {duration:0.2f} seconds")
        return result

    return timed
