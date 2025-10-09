import logging
import time

from data_compaction_job.constants import CompactionOperation, ConfigProperty
from data_compaction_job.table_parser import get_table_config_override

logger = logging.getLogger(__name__)


def is_operation_enabled(config, database, table_name, operation: CompactionOperation):
    """Check if an operation is enabled for a given table"""
    # Check for table-specific override
    table_override = get_table_config_override(config.table_overrides,
                                               database,
                                               table_name,
                                               operation.value,
                                               ConfigProperty.ENABLED.value)
    if table_override is not None:
        return table_override

    # Fall back to global config
    if operation == CompactionOperation.REWRITE_MANIFESTS:
        return config.rewrite_manifests.enabled
    elif operation == CompactionOperation.REWRITE_DATA_FILES:
        return config.rewrite_data_files.enabled
    elif operation == CompactionOperation.EXPIRE_SNAPSHOT:
        return config.expire_snapshot.enabled
    elif operation == CompactionOperation.REMOVE_ORPHAN_FILES:
        return config.remove_orphan_files.enabled

    return False


def operation_enabled(operation: CompactionOperation):
    """Decorator to check if an operation is enabled before executing the method"""

    def decorator(method):
        def wrapper(self, catalog, database, table_name, *args, **kwargs):
            if is_operation_enabled(self.config, database, table_name, operation):
                return method(self, catalog, database, table_name, *args, **kwargs)
            return None

        return wrapper

    return decorator


def timer(message: str):
    """Decorator to time method execution"""

    def timer_decorator(method):
        def timer_func(*args, **kw):
            logger.debug(f"{message} started")
            start_time = time.time()
            result = method(*args, **kw)
            duration = (time.time() - start_time)
            logger.info(f"{message} completed in {duration:0.2f} seconds")
            return result

        return timer_func

    return timer_decorator
