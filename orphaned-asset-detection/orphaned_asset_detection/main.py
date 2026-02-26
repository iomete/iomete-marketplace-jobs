"""Spark-based orchestration for orphaned asset detection."""

import logging
from functools import reduce
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from orphaned_asset_detection.config import ApplicationConfig, DatabaseConfig

logger = logging.getLogger(__name__)

RESULT_SCHEMA = T.StructType(
    [
        T.StructField("asset_type", T.StringType(), False),
        T.StructField("asset_id", T.StringType(), False),
        T.StructField("asset_name", T.StringType(), True),
        T.StructField("domain_id", T.StringType(), True),
        T.StructField("owner_type", T.StringType(), False),
        T.StructField("owner_id", T.StringType(), False),
        T.StructField("archive_date", T.TimestampType(), False),
    ]
)

DOMAIN_DF_SCHEMA = T.StructType(
    [
        T.StructField("asset_id", T.StringType(), True),
        T.StructField("asset_name", T.StringType(), True),
        T.StructField("domain_id", T.StringType(), True),
        T.StructField("owners", T.StringType(), True),
    ]
)

BUNDLE_DF_SCHEMA = T.StructType(
    [
        T.StructField("asset_id", T.StringType(), True),
        T.StructField("asset_name", T.StringType(), True),
        T.StructField("domain_id", T.StringType(), True),
        T.StructField("owner_id", T.StringType(), True),
        T.StructField("owner_type", T.StringType(), True),
    ]
)

MAX_IDS_PER_QUERY = 500


def _configure_logging(debug_mode: bool):
    level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")
    if debug_mode:
        logger.debug("Logger initialized in debug mode")


def _jdbc_base_options(db_config: DatabaseConfig) -> dict:
    return {
        "url": f"jdbc:postgresql://{db_config.host}:{db_config.port}/{db_config.name}",
        "user": db_config.user,
        "password": db_config.password,
        "sslmode": db_config.ssl_mode,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }


def _read_query(spark: SparkSession, db_config: DatabaseConfig, query: str) -> DataFrame:
    options = _jdbc_base_options(db_config)
    options["dbtable"] = f"({query}) AS source"
    return spark.read.format("jdbc").options(**options).load()


def _read_table(spark: SparkSession, db_config: DatabaseConfig, table: str) -> DataFrame:
    options = _jdbc_base_options(db_config)
    options["dbtable"] = table
    return spark.read.format("jdbc").options(**options).load()


def _write_table(df: DataFrame, db_config: DatabaseConfig, table: str):
    options = _jdbc_base_options(db_config)
    options["dbtable"] = table
    df.write.format("jdbc").options(**options).mode("append").save()


def _load_deleted_users(
    spark: SparkSession,
    global_db: DatabaseConfig,
    last_archive_date: Optional[str] = None,
) -> DataFrame:
    base_query = """
        SELECT username AS owner_id, updated_at AS archive_date
        FROM iam_user
        WHERE is_deleted = true
    """
    if last_archive_date:
        query = base_query + f" AND updated_at > '{last_archive_date}'"
    else:
        query = base_query
    return _read_query(spark, global_db, query).select(
        F.col("owner_id").cast(T.StringType()).alias("owner_id"),
        F.col("archive_date"),
    )


def _load_deleted_groups(
    spark: SparkSession,
    global_db: DatabaseConfig,
    last_archive_date: Optional[str] = None,
) -> DataFrame:
    base_query = """
        SELECT name AS owner_id, updated_at AS archive_date
        FROM iam_group
        WHERE is_deleted = true
    """
    if last_archive_date:
        query = base_query + f" AND updated_at > '{last_archive_date}'"
    else:
        query = base_query
    return _read_query(spark, global_db, query).select(
        F.col("owner_id").cast(T.StringType()).alias("owner_id"),
        F.col("archive_date"),
    )


def _get_last_archive_date(spark: SparkSession, global_db: DatabaseConfig) -> Optional[str]:
    df = _read_query(
        spark,
        global_db,
        "SELECT MAX(archive_date) AS max_date FROM orphaned_asset",
    )
    row = df.collect()[0]
    max_date = row["max_date"]
    if max_date is None:
        return None
    return max_date.isoformat()


def _chunk_ids(values: Sequence[str], size: int = MAX_IDS_PER_QUERY) -> Iterable[List[str]]:
    normalized = [value for value in values if value]
    for idx in range(0, len(normalized), size):
        yield normalized[idx : idx + size]


def _escape_literal(value: str) -> str:
    return value.replace("'", "''")


def _array_literal(values: Sequence[str]) -> str:
    escaped = ", ".join(f"'{_escape_literal(value)}'" for value in values)
    return f"ARRAY[{escaped}]::text[]"


def _empty_dataframe(spark: SparkSession, schema: T.StructType) -> DataFrame:
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)


def _format_result(df: DataFrame, asset_type: str, owner_type: str) -> DataFrame:
    return df.select(
        F.lit(asset_type).alias("asset_type"),
        F.col("asset_id"),
        F.col("asset_name"),
        F.col("domain_id"),
        F.lit(owner_type).alias("owner_type"),
        F.col("owner_id"),
        F.col("archive_date"),
    )


def _explode_owner_list(domains_df: DataFrame) -> DataFrame:
    owners = (
        domains_df.select(
            F.col("asset_id"),
            F.col("asset_name"),
            F.col("domain_id"),
            F.from_json(F.col("owners"), T.ArrayType(T.StringType())).alias("owner_list"),
        )
        .select(
            F.col("asset_id"),
            F.col("asset_name"),
            F.col("domain_id"),
            F.explode_outer("owner_list").alias("raw_owner_id"),
        )
        .select(
            "asset_id",
            "asset_name",
            "domain_id",
            F.trim(F.col("raw_owner_id")).alias("owner_id"),
        )
    )
    return owners.where(F.col("owner_id").isNotNull() & (F.col("owner_id") != ""))


def detect_orphaned_domains(
    domains_df: DataFrame,
    deleted_users_df: DataFrame,
) -> DataFrame:
    owners = _explode_owner_list(domains_df).cache()
    user_matches = owners.join(F.broadcast(deleted_users_df), "owner_id", "inner")
    result = _format_result(user_matches, "DOMAIN", "USER")
    owners.unpersist(False)
    return result


def detect_orphaned_bundles(
    bundles_df: DataFrame,
    deleted_users_df: DataFrame,
    deleted_groups_df: DataFrame,
) -> DataFrame:
    spark = bundles_df.sparkSession
    normalized = bundles_df.select(
        F.col("asset_id"),
        F.col("asset_name"),
        F.col("domain_id"),
        F.trim(F.col("owner_id")).alias("owner_id"),
        F.upper(F.col("owner_type")).alias("owner_type"),
    )

    user_matches = normalized.where(F.col("owner_type") == F.lit("USER")).join(
        F.broadcast(deleted_users_df), "owner_id", "inner"
    )
    group_matches = normalized.where(F.col("owner_type") == F.lit("GROUP")).join(
        F.broadcast(deleted_groups_df), "owner_id", "inner"
    )

    return _union_all(
        [
            _format_result(user_matches, "BUNDLE", "USER"),
            _format_result(group_matches, "BUNDLE", "GROUP"),
        ],
        spark,
    )


def detect_orphaned_policies(
    policies_df: DataFrame,
    deleted_users_df: DataFrame,
    deleted_groups_df: DataFrame,
) -> DataFrame:
    spark = policies_df.sparkSession
    normalized = policies_df.select(
        F.col("asset_id"),
        F.col("asset_name"),
        F.col("domain_id"),
        F.trim(F.col("identity")).alias("owner_id"),
        F.upper(F.col("identity_type")).alias("identity_type"),
    )

    user_matches = normalized.where(F.col("identity_type") == F.lit("USER")).join(
        F.broadcast(deleted_users_df), "owner_id", "inner"
    )
    group_matches = normalized.where(F.col("identity_type") == F.lit("GROUP")).join(
        F.broadcast(deleted_groups_df), "owner_id", "inner"
    )

    return _union_all(
        [
            _format_result(user_matches, "DATA_ACCESS_POLICY", "USER"),
            _format_result(group_matches, "DATA_ACCESS_POLICY", "GROUP"),
        ],
        spark,
    )


def _union_all(dfs: Iterable[DataFrame], spark: SparkSession) -> DataFrame:
    dfs = [df for df in dfs if df is not None]
    if not dfs:
        return spark.createDataFrame([], RESULT_SCHEMA)
    return reduce(lambda acc, df: acc.unionByName(df, allowMissingColumns=False), dfs[1:], dfs[0])


def _exclude_existing(candidates_df: DataFrame, existing_df: DataFrame) -> DataFrame:
    return candidates_df.alias("c").join(
        existing_df.alias("e"),
        on=[
            F.col("c.asset_type") == F.col("e.asset_type"),
            F.col("c.asset_id") == F.col("e.asset_id"),
            F.col("c.owner_type") == F.col("e.owner_type"),
            F.col("c.owner_id") == F.col("e.owner_id"),
        ],
        how="left_anti",
    )


def _append_new_orphans(new_records_df: DataFrame, global_db: DatabaseConfig) -> int:
    count = new_records_df.count()
    if count == 0:
        return 0

    payload = new_records_df.withColumn("id", F.expr("uuid()")).select(
        "id",
        "asset_type",
        "asset_id",
        "asset_name",
        "domain_id",
        "owner_type",
        "owner_id",
        "archive_date",
    )
    _write_table(payload, global_db, "orphaned_asset")
    return count


def _load_domains_for_deleted_users(
    spark: SparkSession,
    global_db: DatabaseConfig,
) -> DataFrame:
    domains_raw = _read_query(
        spark,
        global_db,
        """
        SELECT id, name, owners, is_deleted
        FROM domain
        """,
    )
    filtered = domains_raw.where(F.col("is_deleted") == F.lit(False))
    return filtered.select(
        F.col("id").cast(T.StringType()).alias("asset_id"),
        F.col("name").alias("asset_name"),
        F.col("id").cast(T.StringType()).alias("domain_id"),
        F.col("owners"),
    ).dropDuplicates(["asset_id"])


def _load_bundles_for_deleted_users(
    spark: SparkSession,
    global_db: DatabaseConfig,
    deleted_users: Dict[str, Any],
) -> DataFrame:
    frames = []
    user_ids = list(deleted_users.keys())
    if not user_ids:
        return _empty_dataframe(spark, BUNDLE_DF_SCHEMA)

    for chunk in _chunk_ids(user_ids):
        array_sql = _array_literal(chunk)
        query = f"""
            SELECT id, name, domain, owner_id, owner_type
            FROM bundle
            WHERE is_archived = false
              AND owner_type = 'USER'
              AND owner_id = ANY({array_sql})
        """
        frames.append(_read_query(spark, global_db, query))

    if not frames:
        return _empty_dataframe(spark, BUNDLE_DF_SCHEMA)

    bundle_rows = _union_all(frames, spark)
    return bundle_rows.select(
        F.col("id").cast(T.StringType()).alias("asset_id"),
        F.col("name").alias("asset_name"),
        F.col("domain").cast(T.StringType()).alias("domain_id"),
        F.col("owner_id").cast(T.StringType()).alias("owner_id"),
        F.upper(F.col("owner_type")).alias("owner_type"),
    )


def _load_bundles_for_deleted_groups(
    spark: SparkSession,
    global_db: DatabaseConfig,
    deleted_groups: Dict[str, Any],
) -> DataFrame:
    frames = []
    group_ids = list(deleted_groups.keys())
    if not group_ids:
        return _empty_dataframe(spark, BUNDLE_DF_SCHEMA)

    for chunk in _chunk_ids(group_ids):
        array_sql = _array_literal(chunk)
        query = f"""
            SELECT id, name, domain, owner_id, owner_type
            FROM bundle
            WHERE is_archived = false
              AND owner_type = 'GROUP'
              AND owner_id = ANY({array_sql})
        """
        frames.append(_read_query(spark, global_db, query))

    if not frames:
        return _empty_dataframe(spark, BUNDLE_DF_SCHEMA)

    bundle_rows = _union_all(frames, spark)
    return bundle_rows.select(
        F.col("id").cast(T.StringType()).alias("asset_id"),
        F.col("name").alias("asset_name"),
        F.col("domain").cast(T.StringType()).alias("domain_id"),
        F.col("owner_id").cast(T.StringType()).alias("owner_id"),
        F.upper(F.col("owner_type")).alias("owner_type"),
    )


def _load_policy_actions_for_deleted_identities(
    spark: SparkSession,
    global_db: DatabaseConfig,
    deleted_entities: Dict[str, Any],
    identity_type: str,
) -> DataFrame:
    frames = []
    identity_ids = list(deleted_entities.keys())
    if identity_ids:
        schema = T.StructType(
            [
                T.StructField("policy_id", T.StringType(), True),
                T.StructField("identity", T.StringType(), True),
                T.StructField("identity_type", T.StringType(), True),
            ]
        )
    else:
        return _empty_dataframe(
            spark,
            T.StructType(
                [
                    T.StructField("policy_id", T.StringType(), True),
                    T.StructField("identity", T.StringType(), True),
                    T.StructField("identity_type", T.StringType(), True),
                ]
            ),
        )

    for chunk in _chunk_ids(identity_ids):
        array_sql = _array_literal(chunk)
        query = f"""
            SELECT policy_id, identity, identity_type
            FROM data_security_policy_action
            WHERE is_deleted = false
              AND identity_type = '{identity_type}'
              AND identity = ANY({array_sql})
        """
        frames.append(_read_query(spark, global_db, query))

    if not frames:
        return _empty_dataframe(spark, schema)

    return _union_all(frames, spark).select(
        F.col("policy_id").cast(T.StringType()).alias("policy_id"),
        F.col("identity").cast(T.StringType()).alias("identity"),
        F.col("identity_type").cast(T.StringType()).alias("identity_type"),
    )


def _load_policies_by_ids(
    spark: SparkSession,
    global_db: DatabaseConfig,
    policy_ids: Sequence[str],
) -> DataFrame:
    if not policy_ids:
        return _empty_dataframe(
            spark,
            T.StructType(
                [
                    T.StructField("asset_id", T.StringType(), True),
                    T.StructField("asset_name", T.StringType(), True),
                ]
            ),
        )

    frames = []
    for chunk in _chunk_ids(policy_ids):
        query = f"""
            SELECT id, name
            FROM data_security_policy_v2
            WHERE is_deleted = false
              AND id IN ({', '.join(f"'{_escape_literal(pid)}'" for pid in chunk)})
        """
        frames.append(_read_query(spark, global_db, query))

    if not frames:
        return _empty_dataframe(
            spark,
            T.StructType(
                [
                    T.StructField("asset_id", T.StringType(), True),
                    T.StructField("asset_name", T.StringType(), True),
                ]
            ),
        )

    return _union_all(frames, spark).select(
        F.col("id").cast(T.StringType()).alias("asset_id"),
        F.col("name").alias("asset_name"),
    ).dropDuplicates(["asset_id"])




def start_job(spark: SparkSession, config: ApplicationConfig):
    _configure_logging(config.debug_mode)
    logger.info("Starting Orphaned Asset Detection job")

    global_db = config.global_db

    last_archive_date = _get_last_archive_date(spark, global_db)
    deleted_users_df = _load_deleted_users(spark, global_db, last_archive_date).cache()
    deleted_groups_df = _load_deleted_groups(spark, global_db, last_archive_date).cache()

    deleted_users = {row.owner_id: row.archive_date for row in deleted_users_df.collect()}
    deleted_groups = {row.owner_id: row.archive_date for row in deleted_groups_df.collect()}

    domains_df = _load_domains_for_deleted_users(spark, global_db)
    bundles_df = _union_all(
        [
            _load_bundles_for_deleted_users(spark, global_db, deleted_users),
            _load_bundles_for_deleted_groups(spark, global_db, deleted_groups),
        ],
        spark,
    )
    policy_actions_df = _union_all(
        [
            _load_policy_actions_for_deleted_identities(spark, global_db, deleted_users, "USER"),
            _load_policy_actions_for_deleted_identities(spark, global_db, deleted_groups, "GROUP"),
        ],
        spark,
    ).cache()
    policy_details_df = _load_policies_by_ids(
        spark,
        global_db,
        [row.policy_id for row in policy_actions_df.select("policy_id").distinct().collect()],
    )
    policies_df = (
        policy_actions_df.join(
            policy_details_df,
            policy_actions_df.policy_id == policy_details_df.asset_id,
            how="left",
        )
        .select(
            policy_actions_df.policy_id.alias("asset_id"),
            policy_details_df.asset_name.alias("asset_name"),
            F.lit(None).cast(T.StringType()).alias("domain_id"),
            policy_actions_df.identity.alias("identity"),
            policy_actions_df.identity_type.alias("identity_type"),
        )
    )
    policy_actions_df.unpersist(False)
    candidates_df = _union_all(
        [
            detect_orphaned_domains(domains_df, deleted_users_df),
            detect_orphaned_bundles(bundles_df, deleted_users_df, deleted_groups_df),
            detect_orphaned_policies(policies_df, deleted_users_df, deleted_groups_df),
        ],
        spark,
    ).cache()

    total_candidates = candidates_df.count()
    logger.info("Detected %s orphaned asset candidates", total_candidates)

    existing_df = _read_table(
        spark,
        global_db,
        "orphaned_asset",
    ).select("asset_type", "asset_id", "owner_type", "owner_id")

    new_records_df = _exclude_existing(candidates_df, existing_df).cache()
    new_count = new_records_df.count()
    logger.info("%s orphaned assets are new", new_count)

    inserted = _append_new_orphans(new_records_df, global_db)
    logger.info("Inserted %s orphaned asset rows", inserted)

    deleted_users_df.unpersist(False)
    deleted_groups_df.unpersist(False)
    candidates_df.unpersist(False)
    new_records_df.unpersist(False)
