from enum import Enum


class CompactionOperation(str, Enum):
    REWRITE_MANIFESTS = "rewrite_manifests"
    REWRITE_DATA_FILES = "rewrite_data_files"
    EXPIRE_SNAPSHOT = "expire_snapshot"
    REMOVE_ORPHAN_FILES = "remove_orphan_files"
