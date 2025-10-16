import logging
from collections import defaultdict
from typing import List, Dict, Optional, Any

from config import TableMetadata
from constants import CompactionOperation, ConfigProperty

logger = logging.getLogger(__name__)


def parse_table_list(table_list: List[str], databases: Optional[List[str]] = None) -> Dict[str, List[str]]:
    mapping = defaultdict(list)
    for table in table_list:
        table_split = table.split('.')
        if len(table_split) == 2:
            # Table name provided with database prefix (database.table)
            mapping[table_split[0]].append(table_split[1])
        elif len(table_split) == 1:
            # Table name provided without database prefix
            if databases:
                for database in databases:
                    mapping[database].append(table_split[0])
            else:
                logger.warning(f"Table '{table}' provided without database prefix and no databases available. "
                               f"Please provide table in format <database>.<table> or specify databases in config.")
        else:
            logger.warning(
                f"Invalid table format: {table}. Please provide table in format <database>.<table> or <table>")
    return mapping


def get_table_config_override(
        table_overrides: Optional[Dict],
        database: str,
        table: str,
        operation: str,
        config_name: str
) -> Optional[Any]:
    if not table_overrides:
        return None

    # Try with full "database.table" format first, then fallback to just table name
    for table_key in [f"{database}.{table}", table]:
        if (table_key in table_overrides
                and operation in table_overrides.get(table_key)
                and config_name in table_overrides[table_key][operation]):
            return table_overrides[table_key][operation][config_name]

    return None


def get_config_overrides(
    table_overrides: Optional[dict[CompactionOperation, dict]],
    operation: CompactionOperation,
    config_name: ConfigProperty
) -> Optional[Any]:
    return table_overrides.get(operation, {}).get(config_name, None) if table_overrides else None


def get_table_metadata(
    catalog: str,
    database: str,
    table: str,
    table_overrides: Optional[Dict[str, Dict]] = None
) -> TableMetadata:
    return TableMetadata(
        catalog=catalog,
        database=database,
        table=table,
        table_overrides=
        table_overrides.get(f"{database}.{table}", None) or table_overrides.get(table, None)
        if table_overrides else None
    )
