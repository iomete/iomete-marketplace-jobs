from enum import Enum


class CompactionOperation(str, Enum):
    REWRITE_MANIFESTS = "rewrite_manifests"
    REWRITE_DATA_FILES = "rewrite_data_files"
    EXPIRE_SNAPSHOT = "expire_snapshot"
    REMOVE_ORPHAN_FILES = "remove_orphan_files"


class ConfigProperty(str, Enum):
    ENABLED = "enabled"
    RETAIN_LAST = "retain_last"
    OLDER_THAN_DAYS = "older_than_days"
    STRATEGY = "strategy"
    SORT_ORDER = "sort_order"
    OPTIONS = "options"
    WHERE = "where"
    USE_CACHING = "use_caching"


class DefaultConfigValues:
    OPERATION_ENABLED = True
    GC_ENABLED = False
    RETAIN_LAST = 1
    OLDER_THAN_DAYS = 1
    MAX_FILES_PER_RECORD = 100
    STATS_BATCH_SIZE = 100
    PARALLELISM = 4
