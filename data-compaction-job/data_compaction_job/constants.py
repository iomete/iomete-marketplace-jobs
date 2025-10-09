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


class OperationDefaults:
    """Default values for compaction operations"""
    ENABLED = True


class ExpireSnapshotDefaults:
    """Default values for expire snapshot operation"""
    RETAIN_LAST = 1


class RemoveOrphanFilesDefaults:
    """Default values for remove orphan files operation"""
    OLDER_THAN_DAYS = 1


class StatsDefaults:
    """Default values for stats/metrics collection"""
    BATCH_SIZE = 100
    MAX_FILES_PER_RECORD = 100


class JobDefaults:
    """Default values for job execution"""
    PARALLELISM = 4
    GC_ENABLED = False
