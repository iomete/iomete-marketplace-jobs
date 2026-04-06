# Functional Test Cases - iomete-catalog-sync

## 1. Exclusion Rules Enforcement (`RulesHelper`)

### 1.1 Catalog Exclusion by Name
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 1.1.1 | Exclude catalog matching exact name | Catalog name in exclusion list | `ExcludedItemException` thrown |
| 1.1.2 | Allow catalog not in exclusion list | Catalog name not in list | No exception |
| 1.1.3 | Exclude with case-sensitive name match | Name differs only in case | No exception (case-sensitive) |
| 1.1.4 | Empty exclusion name list | Any catalog name | No exception |
| 1.1.5 | Multiple names in exclusion list | Catalog matching second entry | `ExcludedItemException` thrown |

### 1.2 Catalog Exclusion by Properties
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 1.2.1 | Exclude catalog with matching spark property | Property matches filter | `ExcludedItemException` thrown |
| 1.2.2 | Allow catalog with non-matching property value | Property key exists, value differs | No exception |
| 1.2.3 | Allow catalog missing the filtered property key | Property key absent | No exception |
| 1.2.4 | Exclude when multiple filter properties all match | All filter properties present | `ExcludedItemException` thrown |
| 1.2.5 | Allow when only partial filter properties match | One of two filter properties matches | No exception |

### 1.3 Schema Exclusion by Properties
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 1.3.1 | Exclude schema with matching property | Property matches schema filter | `ExcludedItemException` thrown |
| 1.3.2 | Allow schema with no matching properties | No property match | No exception |
| 1.3.3 | Exclude schema via default rule | `iomete.governance.index=false` | `ExcludedItemException` thrown |
| 1.3.4 | Allow schema when default rule property has different value | `iomete.governance.index=true` | No exception |

### 1.4 Table Exclusion by Properties
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 1.4.1 | Exclude table with matching property | Property matches table filter | `ExcludedItemException` thrown |
| 1.4.2 | Allow table with no matching properties | No property match | No exception |
| 1.4.3 | Exclude table via default rule | `iomete.governance.index=false` | `ExcludedItemException` thrown |
| 1.4.4 | Table with `hidden=true` excluded when configured | `hidden=true` in filter | `ExcludedItemException` thrown |

### 1.5 Default Rule Behavior
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 1.5.1 | Default rule merges with catalog-specific rules | Both rules defined | Both enforced |
| 1.5.2 | Default rule merges with schema-specific rules | Both rules defined | Both enforced |
| 1.5.3 | Default rule merges with table-specific rules | Both rules defined | Both enforced |
| 1.5.4 | Default rule alone (no specific rules) | Only default configured | Default enforced |
| 1.5.5 | No default rule and no specific rules | Empty config | Nothing excluded |

### 1.6 `ignoreExcluded` Utility
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 1.6.1 | Returns value when no exclusion | Block returns value | Value returned |
| 1.6.2 | Returns null when ExcludedItemException thrown | Block throws ExcludedItemException | `null` returned |
| 1.6.3 | Propagates non-exclusion exceptions | Block throws RuntimeException | Exception propagated |

### 1.7 `matchesAnyExclusion`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 1.7.1 | Empty property map against non-empty rules | `emptyMap()` vs rules | `false` |
| 1.7.2 | Non-empty properties against empty rules | Properties vs `emptyMap()` | `false` |
| 1.7.3 | Matching key but null value in properties | Key exists, value is null | `false` |
| 1.7.4 | Matching key and value | Exact match | `true` |

---

## 2. Configuration Loading (`Config`)

### 2.1 ApplicationConfigFactory
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 2.1.1 | Load valid JSON config | Well-formed `/etc/configs/application.json` | `ApplicationConfig` with parsed rules |
| 2.1.2 | File not found fallback | Config file missing | Default `ApplicationConfig` returned |
| 2.1.3 | Malformed JSON fallback | Invalid JSON content | Default `ApplicationConfig` returned |
| 2.1.4 | Partial config with missing fields | JSON with only `catalogs` rules | Defaults for missing sections |
| 2.1.5 | Unknown JSON fields ignored | Extra fields in JSON | Parsed without error (`@JsonIgnoreProperties`) |

---

## 3. Spark Metadata Reader (`SparkMetadataReader`)

### 3.1 `getSchemas`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 3.1.1 | Return schemas for valid catalog | Spark returns databases | List of schema names |
| 3.1.2 | Return empty list on Spark SQL failure | Spark throws exception | Empty list, warning logged |
| 3.1.3 | Handle catalog with no schemas | Spark returns empty result | Empty list |

### 3.2 `getSchemaProperties`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 3.2.1 | Parse properties from DESC DATABASE EXTENDED | Valid output with properties row | Map of key-value pairs |
| 3.2.2 | Handle schema with no properties | No properties row in output | Empty map |
| 3.2.3 | Handle malformed properties string | Unparseable tuple format | Empty map, no crash |
| 3.2.4 | Return empty map on Spark SQL failure | Spark throws exception | Empty map |
| 3.2.5 | Parse nested brackets in property values | Properties with brackets in value | Correct key-value extraction |

### 3.3 `getTables`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 3.3.1 | Combine tables and views, deduplicate | Overlapping table and view names | Unique list by name |
| 3.3.2 | Tables query fails, views succeed | Tables throws, views returns data | Views-only result |
| 3.3.3 | Views query fails, tables succeed | Views throws, tables returns data | Tables-only result |
| 3.3.4 | Both queries fail | Both throw exceptions | Empty list |
| 3.3.5 | Skip views for unsupported catalog types | Catalog type = `jdbc` | Only tables fetched, views skipped |
| 3.3.6 | Fetch views for supported catalog types | Catalog type = `iceberg` | Both tables and views fetched |
| 3.3.7 | Fetch views for `glue` catalog type | Catalog type = `glue` | Views query executed |
| 3.3.8 | Fetch views for `rest` catalog type | Catalog type = `rest` | Views query executed |
| 3.3.9 | Temporary table flag preserved | Spark returns isTemp=true | `ShowTablesRow.isTemp = true` |

### 3.4 `describeTable`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 3.4.1 | Parse columns section correctly | Standard DESCRIBE output | Correct column names, types, descriptions |
| 3.4.2 | Parse partition columns | Output has partition section | Columns marked with `isPartitionKey=true` |
| 3.4.3 | Parse table metadata section | Output has table info | Metadata map with Type, Provider, Owner, etc. |
| 3.4.4 | Parse view info section | View DESCRIBE output | ViewText captured in metadata |
| 3.4.5 | Handle column sort order | Multiple columns | `sortOrder` assigned sequentially (0, 1, 2...) |
| 3.4.6 | Skip empty/separator rows | Rows with blank col_name | Separators not included in columns |
| 3.4.7 | Handle metadata columns section (Iceberg) | Iceberg-specific metadata columns | Metadata columns section parsed |

### 3.5 `processTableColumns`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 3.5.1 | Detect section transitions via `# col_name` markers | Rows starting with `#` | Correct section switching |
| 3.5.2 | Handle table with no partitions | No partition section | All columns have `isPartitionKey=false` |
| 3.5.3 | Handle table with only columns, no metadata | Minimal DESCRIBE output | Columns parsed, empty metadata map |

---

## 4. Table Metadata Extractor (`TableMetadataExtractor`)

### 4.1 `scrapeTable`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 4.1.1 | Extract metadata for MANAGED Iceberg table | Provider=iceberg, Type=MANAGED | Correct `TableMetadata` with stats |
| 4.1.2 | Extract metadata for EXTERNAL table | Type=EXTERNAL | `tableType=EXTERNAL` |
| 4.1.3 | Extract metadata for VIEW | isView=true | `tableType=VIEW`, `isView=true` |
| 4.1.4 | Table excluded by rules during scrape | Table properties match exclusion | `ExcludedItemException` thrown |
| 4.1.5 | Table statistics populated for Iceberg | Iceberg extractor returns stats | `numFiles`, `sizeInBytes`, `totalRecords` set |
| 4.1.6 | Table statistics null for unknown provider | GenericTableExtractor used | Statistics fields null |
| 4.1.7 | Column statistics populated for Hive table | DatasourceV1LikeTableExtractor | `ColumnStat` list on each column |
| 4.1.8 | PII tags assigned to columns | PII detection returns tags | `column.tags` contains PII/PCI tags |
| 4.1.9 | PII detection skipped when extractor doesn't support tags | GenericTableExtractor (no SupportColumnTags) | No PII detection call |
| 4.1.10 | Temporary table handling | `isTemp=true` | `isTemporary=true` in metadata |
| 4.1.11 | Sync time and spark app ID populated | Any table | `syncTime` and `sparkApplicationId` set |

### 4.2 `parseIcebergPropertiesSafe`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 4.2.1 | Parse valid Iceberg properties string | `[key1=value1,key2=value2]` | Map with entries |
| 4.2.2 | Handle null input | `null` | Empty map |
| 4.2.3 | Handle empty string | `""` | Empty map |
| 4.2.4 | Handle malformed input | `not-a-valid-format` | Empty map, no crash |
| 4.2.5 | Handle values containing `=` | `[key=val=ue]` | Correct split on first `=` |

### 4.3 Creation Time Parsing
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 4.3.1 | Parse valid datetime from metadata | `Created Time` present | `createdAt` epoch millis |
| 4.3.2 | Handle missing Created Time | Key absent | `createdAt = null` |
| 4.3.3 | Handle unparseable datetime format | Invalid format string | `createdAt = null`, no crash |

---

## 5. Table Extractor Factory (`TableExtractorFactory`)

### 5.1 Extractor Selection
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 5.1.1 | View returns ViewExtractor | `isView=true`, any provider | `ViewExtractor` instance |
| 5.1.2 | Iceberg provider returns IcebergTableExtractor | `provider=iceberg` | `IcebergTableExtractor` instance |
| 5.1.3 | Parquet provider returns DatasourceV1LikeTableExtractor | `provider=parquet` | `DatasourceV1LikeTableExtractor` |
| 5.1.4 | ORC provider returns DatasourceV1LikeTableExtractor | `provider=orc` | `DatasourceV1LikeTableExtractor` |
| 5.1.5 | Hive provider returns DatasourceV1LikeTableExtractor | `provider=hive` | `DatasourceV1LikeTableExtractor` |
| 5.1.6 | Unknown provider returns GenericTableExtractor | `provider=csv` | `GenericTableExtractor` |
| 5.1.7 | Null/empty provider returns GenericTableExtractor | `provider=null` | `GenericTableExtractor` |
| 5.1.8 | View flag takes precedence over provider | `isView=true`, `provider=iceberg` | `ViewExtractor` (not Iceberg) |

---

## 6. Iceberg Table Extractor (`IcebergTableExtractor`)

### 6.1 `extractTableStatistics`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 6.1.1 | Extract stats from snapshots table | Snapshots table has data | `lastModified`, `sizeInBytes`, `totalRecords` populated |
| 6.1.2 | Extract file counts from all_data_files | Data files table has data | `numFiles`, `totalTableNumFiles` populated |
| 6.1.3 | Handle table with no snapshots | Empty snapshots table | `null` returned |
| 6.1.4 | Handle Spark query failure on snapshots | Query throws exception | `null` returned gracefully |
| 6.1.5 | Handle Spark query failure on data files | Data files query fails | Partial stats returned |

### 6.2 Table Type
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 6.2.1 | Always returns MANAGED | Any Iceberg table | `getTableType == "MANAGED"` |

---

## 7. DatasourceV1Like Table Extractor (`DatasourceV1LikeTableExtractor`)

### 7.1 `extractTableStatistics`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 7.1.1 | Extract stats from CatalogTable | Stats available | `sizeInBytes`, `totalRecords` populated |
| 7.1.2 | Handle missing statistics | No stats on CatalogTable | `null` returned |
| 7.1.3 | Handle table not found in Spark catalog | Table missing | Exception handled gracefully |

### 7.2 `extractColumnStatistics`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 7.2.1 | Extract column stats for numeric column | Column with stats | `distinctCount`, `min`, `max`, `nullCount` present |
| 7.2.2 | Extract column stats for string column | String column with stats | `avgLen`, `maxLen` present |
| 7.2.3 | Handle column with no statistics | No stats collected | Empty `ColumnStat` list |
| 7.2.4 | Handle multiple columns | 3 columns requested | Map with 3 entries |
| 7.2.5 | Handle Scala Option.None values | Statistics return None | `null` values handled, no crash |

### 7.3 Table Type
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 7.3.1 | Return type from Spark catalog | MANAGED table | Correct type string |
| 7.3.2 | Return type for EXTERNAL table | EXTERNAL table | Correct type string |

---

## 8. View Extractor (`ViewExtractor`)

| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 8.1 | Table type is VIEW | Any view | `getTableType == "VIEW"` |
| 8.2 | Implements SupportColumnTags | N/A | PII detection eligible |
| 8.3 | Does not implement SupportTableStatistics | N/A | No statistics extracted |

---

## 9. Generic Table Extractor (`GenericTableExtractor`)

| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 9.1 | Table type is UNKNOWN | Any unknown provider | `getTableType == "UNKNOWN"` |
| 9.2 | No statistics support | N/A | No `SupportTableStatistics` interface |
| 9.3 | No column tags support | N/A | No `SupportColumnTags` interface |

---

## 10. PII Detection Service (`PIIDetectionService`)

### 10.1 Feature Toggle
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 10.1.1 | PII detection disabled by default | No env var set | Empty map returned, no Presidio call |
| 10.1.2 | PII detection enabled via env var | `PII_DETECTION_ENABLED=true` | Presidio called |
| 10.1.3 | PII detection enabled via system property | `piiDetectionEnabled=true` | Presidio called |

### 10.2 Data Sampling
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 10.2.1 | Sample 5 rows from table | Table has 100 rows | TABLESAMPLE query used, max 5 rows |
| 10.2.2 | Handle table with fewer than 5 rows | Table has 2 rows | 2 rows sampled |
| 10.2.3 | Handle empty table | Table has 0 rows | Empty map returned |
| 10.2.4 | Handle sampling query failure | Spark query fails | Empty map returned, error logged |

### 10.3 PII Entity Detection
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 10.3.1 | Detect PERSON entity in column | Column contains names | Tags include `DETECTED_PERSON`, `PII` |
| 10.3.2 | Detect EMAIL_ADDRESS entity | Column contains emails | Tags include `DETECTED_EMAIL_ADDRESS`, `PII` |
| 10.3.3 | Detect CREDIT_CARD entity (PCI) | Column contains card numbers | Tags include `DETECTED_CREDIT_CARD`, `PCI` |
| 10.3.4 | Detect IBAN_CODE entity (PCI) | Column contains IBANs | Tags include `DETECTED_IBAN_CODE`, `PCI` |
| 10.3.5 | No entity detected | Column contains random numbers | No tags added |
| 10.3.6 | Multiple columns with different entities | Name + email columns | Each column tagged independently |
| 10.3.7 | Presidio returns multiple entities, highest score wins | Multiple detections | Top-scored entity used |
| 10.3.8 | Handle Presidio API failure | REST call throws | Empty tags, error logged |
| 10.3.9 | Handle null column values in sample | Column has nulls | Nulls skipped, no crash |

---

## 11. Metadata Scraper Orchestration (`MetadataScraper`)

### 11.1 Catalog Processing
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 11.1.1 | Process single catalog end-to-end | 1 catalog, 1 schema, 1 table | `indexCatalog`, `indexSchema`, `indexTable` each called once |
| 11.1.2 | Process multiple catalogs | 3 catalogs | All 3 processed, 3 `indexCatalog` calls |
| 11.1.3 | Skip excluded catalog | Catalog in exclusion list | Catalog skipped, no indexing |
| 11.1.4 | Handle CoreClient returning empty catalog list | No catalogs | No processing, no errors |
| 11.1.5 | Handle CoreClient failure | REST call throws | Error logged, process exits gracefully |

### 11.2 Schema Processing
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 11.2.1 | Process schema with multiple tables | Schema has 5 tables | All 5 tables indexed |
| 11.2.2 | Skip excluded schema | Schema matches exclusion rule | Schema skipped, null returned |
| 11.2.3 | Handle schema with no tables | Empty schema | `SchemaMetadata` with `totalTableCount=0` |
| 11.2.4 | Schema properties fetched and checked | Schema has governance property | Exclusion rules applied to schema properties |

### 11.3 Table Processing
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 11.3.1 | Process table successfully | Valid table | `indexTable` called with correct metadata |
| 11.3.2 | Skip excluded table | Table matches exclusion rule | Table skipped, counted in processing |
| 11.3.3 | Handle table processing failure | `scrapeTable` throws exception | Failure counted, other tables continue |
| 11.3.4 | Parallel table processing | Multiple tables | Tables processed via parallel stream |
| 11.3.5 | Failed table count tracked in SchemaMetadata | 2 of 5 tables fail | `failedTableCount=2` |

### 11.4 Metadata Aggregation
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 11.4.1 | CatalogMetadata aggregates schema counts | 3 schemas, varying tables | Correct `totalSchemaCount`, `totalTableCount` |
| 11.4.2 | SchemaMetadata aggregates table sizes | Tables with varying sizes | Sum of `sizeInBytes` |
| 11.4.3 | SchemaMetadata counts views separately | Mix of tables and views | `totalViewCount` correct |
| 11.4.4 | CatalogMetadata aggregates total files | Tables with file counts | Sum of all `numFiles` |
| 11.4.5 | Handle null statistics in aggregation | Some tables have null sizes | Null-safe summation |

---

## 12. Spark Session Provider (`SparkSessionProvider`)

### 12.1 Session Management
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 12.1.1 | Create default Spark session with Hive support | First call | Session created with `enableHiveSupport()` |
| 12.1.2 | Reuse cached session for same catalog | Same catalog requested twice | Same `SparkSession` instance returned |
| 12.1.3 | Create new session for different catalog | Two different catalogs | Two distinct sessions |
| 12.1.4 | Apply Iceberg extensions to session | Catalog with Iceberg config | Iceberg and Nessie extensions configured |
| 12.1.5 | Apply catalog-specific Spark properties | Catalog with `sparkProperties` | Properties set on SparkSession config |
| 12.1.6 | Reuse default session when catalog already configured | Catalog already in Spark conf | Default session returned, no new session |

---

## 13. DTO Builder Functions (`Dto`)

### 13.1 `CatalogMetadata.build`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 13.1.1 | Build from multiple schemas | 3 SchemaMetadata objects | Aggregated counts and sizes |
| 13.1.2 | Build from empty schema list | No schemas | Zero counts, zero sizes |
| 13.1.3 | Catalog type and location propagated | CatalogDetails with type/location | Correct values in CatalogMetadata |

### 13.2 `SchemaMetadata.build`
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 13.2.1 | Build from multiple tables | 5 TableMetadata objects | Correct table/view counts, sum of sizes |
| 13.2.2 | Build from empty table list | No tables | Zero counts |
| 13.2.3 | Count views vs tables separately | Mix of views and tables | `totalViewCount` and `totalTableCount` correct |
| 13.2.4 | Failed table count passed through | `failuresSize=3` | `failedTableCount=3` |
| 13.2.5 | Null sizes handled in aggregation | Tables with null `sizeInBytes` | No NPE, null-safe sum |

---

## 14. Utility Functions (`Utils`)

### 14.1 Scala Option Interop
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 14.1.1 | Convert Some(value) to value | Scala `Some("test")` | `"test"` |
| 14.1.2 | Convert None to null | Scala `None` | `null` |

### 14.2 Row Extension Functions
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 14.2.1 | `getTimestamp` with valid timestamp | Row with timestamp field | Epoch millis as Long |
| 14.2.2 | `getTimestamp` with missing field | Field not in Row | `null` |
| 14.2.3 | `getLong` with valid value | Row with long field | Long value |
| 14.2.4 | `getLong` with null value | Null field in Row | `null` |
| 14.2.5 | `get` with valid field name | Field exists | Value returned |
| 14.2.6 | `get` with missing field name | Field absent | `null` or exception handled |

---

## 15. REST Client Interactions (`Clients`)

### 15.1 CatalogClient
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 15.1.1 | Index table returns success | Valid TableMetadata | HTTP 200 response |
| 15.1.2 | Index schema returns success | Valid SchemaMetadata | HTTP 200 response |
| 15.1.3 | Index catalog returns success | Valid CatalogMetadata | HTTP 200 response |
| 15.1.4 | Handle server error on indexTable | Service returns 500 | Error handled, logged |
| 15.1.5 | Handle connection timeout | Service unreachable | Timeout exception handled |

### 15.2 CoreClient
| # | Test Case | Input | Expected |
|---|-----------|-------|----------|
| 15.2.1 | Fetch catalogs returns list | Service returns catalog list | Deserialized `List<CatalogDetails>` |
| 15.2.2 | Handle empty catalog response | Service returns `[]` | Empty list |
| 15.2.3 | Deserialize CatalogDetails with all fields | Full JSON response | All fields populated including `sparkProperties` |
| 15.2.4 | Deserialize CatalogDetails with optional null fields | Missing `location` and `storageEndpoint` | Defaults to `null` |
| 15.2.5 | Handle unknown JSON fields | Extra fields in response | Ignored (`@JsonIgnoreProperties`) |

---

## 16. End-to-End Scenarios

| # | Test Case | Description | Expected |
|---|-----------|-------------|----------|
| 16.1 | Full sync with mixed catalog types | Iceberg + JDBC + Hive catalogs | Each catalog uses correct extractor, all metadata indexed |
| 16.2 | Sync with all exclusion rules active | Catalogs, schemas, and tables excluded | Only non-excluded items indexed |
| 16.3 | Sync with PII detection enabled | PII_DETECTION_ENABLED=true | Columns tagged with PII/PCI entities |
| 16.4 | Sync with PII detection disabled | PII_DETECTION_ENABLED=false | No PII tags, Presidio never called |
| 16.5 | Sync with empty data lake | No catalogs returned by CoreClient | Graceful completion, nothing indexed |
| 16.6 | Partial failure resilience | Some tables fail to scrape | Failed tables counted, others indexed successfully |
| 16.7 | Large catalog with many schemas/tables | 50 schemas, 1000 tables | Parallel processing, all indexed |
| 16.8 | Catalog with views and tables | Mixed views and regular tables | Views use ViewExtractor, tables use provider-specific extractor |
| 16.9 | Iceberg table with snapshots | Iceberg table with history | Statistics include snapshot-based file/size counts |
| 16.10 | Table with partition columns | Partitioned Hive table | Columns marked with `isPartitionKey=true` |

---

## 17. Error Handling & Edge Cases

| # | Test Case | Description | Expected |
|---|-----------|-------------|----------|
| 17.1 | Spark session creation failure | Spark unavailable | Error logged, process fails gracefully |
| 17.2 | Presidio service unavailable | REST call fails | PII detection returns empty, sync continues |
| 17.3 | Catalog service unavailable | Index call fails | Error logged, sync continues with other items |
| 17.4 | Core service unavailable | Catalog fetch fails | Process stops with error |
| 17.5 | Malformed DESCRIBE output | Unexpected Spark SQL output format | Parsing handles gracefully, no crash |
| 17.6 | Table with zero columns | DESCRIBE returns no columns | Empty column list in TableMetadata |
| 17.7 | Very long column names or values | Extremely long strings | No truncation errors |
| 17.8 | Concurrent modification of Spark catalog | Catalog changes during sync | No crash, stale data acceptable |
| 17.9 | Null provider in table metadata | Provider key missing from DESCRIBE | GenericTableExtractor used |
| 17.10 | Special characters in catalog/schema/table names | Backtick-escaped names | Spark SQL handles quoting correctly |