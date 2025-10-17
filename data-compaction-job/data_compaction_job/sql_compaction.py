import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import cache

import requests

from config import TableMetadata
from data_compaction_job.config import ApplicationConfig
from data_compaction_job.constants import CompactionOperation, ConfigProperty
from data_compaction_job.decorators import operation_enabled, timer
from data_compaction_job.table_parser import parse_table_list
from operations.expire_snapshots import get_expire_snapshots_query
from stats_emitter import emit_stats, init_emitter, close_emitter
from table_parser import get_table_metadata, get_config_overrides

logger = logging.getLogger(__name__)

from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession


class SqlCompaction:
    def __init__(self, spark: SparkSession, config: ApplicationConfig):
        self.spark = spark
        self.config = config
        self._databases = None

    def run_compaction(self):
        with ThreadPoolExecutor(max_workers=self.config.parallelism) as executor:
            futures = []
            catalog = self.__get_catalog()
            logger.info(f"Starting table optimisation for catalog: {catalog}")

            self._databases = self.__get_databases(catalog)
            logger.info(f"Databases in catalog '{catalog}' considered for optimisation : {self._databases}")

            db_table_mapping = defaultdict(list)
            for database in self._databases:
                logger.info(f"Introspecting database: {database}")
                tables = self.__get_tables(catalog, database)
                logger.info(f"Tables in database '{database}' considered for optimisation : {tables}")
                if tables:
                    db_table_mapping[database] = tables

            init_emitter(self.spark,
                         batch_size=self.config.stats_batch_size,
                         max_files_per_record=self.config.remove_orphan_files.max_files_per_record)
            for database in self._databases:
                for table in db_table_mapping[database]:
                    table_metadata = get_table_metadata(
                        catalog=catalog,
                        database=database,
                        table=table,
                        table_overrides=self.config.table_overrides
                    )

                    futures.append(executor.submit(self.__process_table_if_iceberg, table_metadata))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing table, error={e}")

            close_emitter(self.spark)

    def __process_table_if_iceberg(self, table_metadata: TableMetadata):
        catalog, db_name, table = table_metadata.catalog, table_metadata.database, table_metadata.table

        try:
            table_meta = self.spark.sql(f"describe extended {catalog}.{db_name}.{table}").collect()

            # Skip, if not an `iceberg` table
            if not any(row.col_name == "Provider" and row.data_type == "iceberg" for row in table_meta):
                return

            message = f"[{db_name}.{table}] table compaction"
            timer(message)(self.__process_table)(table_metadata)

        except Exception as e:
            logger.error(f"[{db_name}.{table}] Error processing table, error={e}")

    def __process_table(self, table_metadata: TableMetadata):
        catalog, database, table_name = table_metadata.catalog, table_metadata.database, table_metadata.table

        if self.config.gc_handling.enabled:
            gc_enabled = self.__check_gc_enabled(catalog, database, table_name)

            if gc_enabled is False:  # GC is disabled for this table
                logger.info(f"[{database}.{table_name}] G.C. is disabled. Temporarily enabling it for compaction.")
                self.__set_gc_enabled(catalog, database, table_name, True)

                try:
                    # Run all the compaction operations
                    self.__run_compaction_operations(table_metadata)
                finally:
                    # Disable GC again after compaction operations
                    logger.info(f"[{database}.{table_name}] Disabling G.C. after compaction.")
                    self.__set_gc_enabled(catalog, database, table_name, False)
            else:
                # GC is already enabled or None (not set), proceed with normal flow
                self.__run_compaction_operations(table_metadata)
        else:
            # GC handling is not enabled, proceed with normal flow
            self.__run_compaction_operations(table_metadata)

    def __run_compaction_operations(self, table_metadata: TableMetadata):
        """Run enabled compaction operations for a table"""
        self.__rewrite_manifest(table_metadata)
        self.__rewrite_data_files(table_metadata)
        self.__expire_snapshots(table_metadata)
        self.__remove_orphan_files(table_metadata)

    def __check_gc_enabled(self, catalog, database, table_name):
        try:
            # Get the table properties
            result = self.spark.sql(f"SHOW TBLPROPERTIES {catalog}.{database}.{table_name}").collect()

            # Look for the G.C. enabled property (might be named differently depending on implementation)
            for row in result:
                if row.key.lower() == "gc.enabled":
                    return row.value.lower() == "true"

            # Property not found
            return True
        except Exception as e:
            logger.warning(f"[{database}.{table_name}] Failed to check G.C. status: {e}")
            return True

    def __set_gc_enabled(self, catalog, database, table_name, enabled):
        try:
            # Convert boolean to string value
            value = str(enabled).lower()
            # Set the property
            self.spark.sql(
                f"ALTER TABLE {catalog}.{database}.{table_name} SET TBLPROPERTIES ('gc.enabled' = '{value}')").collect()
            logger.info(f"[{database}.{table_name}] Set G.C. enabled to {value}")
        except Exception as e:
            logger.error(f"[{database}.{table_name}] Failed to set G.C. enabled to {enabled}: {e}")
            raise e

    @operation_enabled(CompactionOperation.EXPIRE_SNAPSHOT)
    @emit_stats("EXPIRE_SNAPSHOTS")
    def __expire_snapshots(self, table_metadata: TableMetadata):
        query = get_expire_snapshots_query(self.config.expire_snapshot, table_metadata)
        result = self.spark.sql(query).collect()
        return result, query

    @operation_enabled(CompactionOperation.REMOVE_ORPHAN_FILES)
    @emit_stats("REMOVE_ORPHAN_FILES")
    def __remove_orphan_files(self, table_metadata: TableMetadata):
        catalog, database, table_name = table_metadata.catalog, table_metadata.database, table_metadata.table

        days = int(get_config_overrides(table_metadata.table_overrides,
                                             CompactionOperation.REMOVE_ORPHAN_FILES,
                                             ConfigProperty.OLDER_THAN_DAYS)
                   or self.config.remove_orphan_files.older_than_days)
        timestamp = datetime.now(timezone.utc) - timedelta(days=days)
        options = f"table => '`{catalog}`.`{database}`.`{table_name}`', older_than => TIMESTAMP '{timestamp}'"
        query = f"CALL {catalog}.system.remove_orphan_files({options})"
        result = self.spark.sql(query).collect()
        return result, query

    @operation_enabled(CompactionOperation.REWRITE_MANIFESTS)
    @emit_stats("REWRITE_MANIFESTS")
    def __rewrite_manifest(self, table_metadata: TableMetadata):
        catalog, database, table_name = table_metadata.catalog, table_metadata.database, table_metadata.table

        options = f"table => '`{catalog}`.`{database}`.`{table_name}`'"
        use_caching = (get_config_overrides(table_metadata.table_overrides,
                                            CompactionOperation.REWRITE_MANIFESTS,
                                            ConfigProperty.USE_CACHING)
                       or self.config.rewrite_manifests.use_caching)
        if use_caching:
            use_caching = str(use_caching).lower()
            options += f", use_caching => {use_caching}"
        query = f"CALL {catalog}.system.rewrite_manifests({options})"
        result = self.spark.sql(query).collect()
        return result, query

    @operation_enabled(CompactionOperation.REWRITE_DATA_FILES)
    @emit_stats("REWRITE_DATA_FILES")
    def __rewrite_data_files(self, table_metadata: TableMetadata):
        catalog, database, table_name = table_metadata.catalog, table_metadata.database, table_metadata.table

        strategy = (get_config_overrides(table_metadata.table_overrides,
                                        CompactionOperation.REWRITE_DATA_FILES,
                                        ConfigProperty.STRATEGY)
                    or self.config.rewrite_data_files.strategy)
        sort_order = (get_config_overrides(table_metadata.table_overrides,
                                          CompactionOperation.REWRITE_DATA_FILES,
                                          ConfigProperty.SORT_ORDER)
                      or self.config.rewrite_data_files.sort_order)
        rewrite_options = (get_config_overrides(table_metadata.table_overrides,
                                               CompactionOperation.REWRITE_DATA_FILES,
                                               ConfigProperty.OPTIONS)
                           or self.config.rewrite_data_files.options)
        where = (get_config_overrides(table_metadata.table_overrides,
                                     CompactionOperation.REWRITE_DATA_FILES,
                                     ConfigProperty.WHERE)
                 or self.config.rewrite_data_files.where)

        options = f"table => '`{catalog}`.`{database}`.`{table_name}`'"

        if strategy and strategy == "sort":
            options += f", strategy => {strategy}, sort_order => {sort_order}"
        if rewrite_options:
            option_map = ', '.join(', '.join((f"'{k}'", f"'{v}'")) for (k, v) in rewrite_options.items())
            options += f", options => map({option_map})"
        if where:
            options += f", where => {where}"

        query = f"CALL {catalog}.system.rewrite_data_files({options})"
        result = self.spark.sql(query).collect()
        return result, query

    def __get_catalog(self):
        catalog = self.config.catalog
        sql_client = SqlClient()
        available_catalogs = sql_client.catalogs()
        if catalog not in available_catalogs:
            logger.error(f"Catalog not found: {catalog}. Available catalogs: {available_catalogs}.")
            raise Exception(f"Catalog not found for optimisation: {catalog}")
        return catalog

    def __get_databases(self, catalog):
        available_databases = [database.namespace for database
                               in self.spark.sql(f"show databases from {catalog}").collect()]
        if self.config.include_exclude.databases:
            return [database for database in self.config.include_exclude.databases if database in available_databases]
        else:
            return available_databases

    def __get_tables(self, catalog, database):
        available_tables = [table.tableName for table
                            in self.spark.sql(f"show tables from {catalog}.{database}").collect()]
        if database in self.__get_table_includes():
            tables = [table for table
                      in self.__get_table_includes()[database]
                      if table in available_tables]
        elif database in self.__get_table_excludes():
            tables = [table for table
                      in available_tables
                      if table not in self.__get_table_excludes()[database]]
        else:
            tables = available_tables
        return tables

    @cache
    def __get_table_excludes(self):
        return parse_table_list(self.config.include_exclude.table_exclude, self._databases)

    @cache
    def __get_table_includes(self):
        return parse_table_list(self.config.include_exclude.table_include, self._databases)


class SqlClient:
    def __init__(self):
        release_namespace = os.getenv("RELEASE_NAMESPACE", "iomete-system")
        cluster_domain = os.getenv("CLUSTER_DOMAIN", "cluster.local")
        self.base_url = os.getenv("SQL_API_ENDPOINT", f"http://iom-core.{release_namespace}.svc.{cluster_domain}")

    def catalogs(self):
        response = requests.get(f"{self.base_url}/api/internal/sql/schema/catalogs")
        if response.status_code == 200:
            return set(response.json())
        else:
            response.raise_for_status()
