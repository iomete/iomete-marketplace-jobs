# Useful SQL Tools
This repo contains various SQL queries that can enhance your database management and querying experience.

## Usage Statistics of Tables

### Spark Audit Logs Analysis

We first create external table in spark_catalog for the audit files:

```sql
CREATE TABLE iomete_spark_audit_external_table (
    repositoryType INT,
    repositoryName STRING,
    user STRING,
    eventTime STRING,
    accessType STRING,
    resourcePath STRING,
    resourceType STRING,
    action STRING,
    accessResult INT,
    agentId STRING,
    policyId BIGINT,
    resultReason STRING,
    aclEnforcer STRING,
    sessionId STRING,
    clientType STRING,
    clientIP STRING,
    requestData STRING,
    agentHostname STRING,
    logType STRING,
    eventId STRING,
    seqNum BIGINT,
    eventCount BIGINT,
    eventDurationMS BIGINT,
    additionalInfo STRING,
    clusterName STRING,
    zoneName STRING
  )
USING ORC
PARTITIONED BY (day INT)
LOCATION 's3://lakehouse/ranger/audit';
```

From this table, one can query

- run time (using eventTime column)
- run date (using day column)
- user (using user column)
- job/query id (using eventId)


### Daily Job Counts

For Daily counts - create a view for a particular day of interest (day has to be hardcoded, such as  '20251125' below, for example). This is the fast solution as it filter ORC files for a single day:

```sql
CREATE OR REPLACE VIEW iomete_spark_audit_access_summary AS
with audit as (
	SELECT
		concat_ws(
	        '.',
	        split(resourcePath, '/')[0],
	        split(resourcePath, '/')[1],
	        split(resourcePath, '/')[2]
	    ) as table,
		CASE
			WHEN substr(agentHostname, 1, 2) = 'cc' THEN 'compute-query'
			WHEN substr(agentHostname, 1, 2) = 'jc' THEN 'jupyter-query'
			ELSE 'job'
		END AS query_type
	FROM
		spark_catalog.iomete_system_db.iomete_spark_audit_external_table 
	WHERE
		day = '20251125' and resourceType = 'table'
	ORDER BY
		eventTime),
stats as (
	select
		table,
		query_type,
		count(*) as stats_count
	from
		audit
	group by
		table,
		query_type),
stats_pivoted as (
	SELECT
		table,
		SUM(CASE WHEN query_type = 'job' THEN stats_count ELSE 0 END) AS job_count,
		SUM(CASE WHEN query_type = 'compute-query' THEN stats_count ELSE 0 END) AS compute_query_count,
		SUM(CASE WHEN query_type = 'jupyter-query' THEN stats_count ELSE 0 END) AS jupyter_query_count
	FROM
		stats
	GROUP BY
		table)
select *
from stats_pivoted
order by 
  job_count desc, 
  compute_query_count desc, 
  jupyter_query_count desc;
```

This will create table stats for that day like below:

![](images/image1.png)

One can query multiple days. This query can last for some time depending on how much data accumulated on each day, and how many days is being queried:

```sql
-- in case some ORC files are corrupted in s3 bucket
SET spark.sql.files.ignoreCorruptFiles = true;
-- query
with audit as (
	SELECT
		concat_ws(
	        '.',
	        split(resourcePath, '/')[0],
	        split(resourcePath, '/')[1],
	        split(resourcePath, '/')[2]
	    ) as table,
		CASE
			WHEN substr(agentHostname, 1, 2) = 'cc' THEN 'compute-query'
			WHEN substr(agentHostname, 1, 2) = 'jc' THEN 'jupyter-query'
			ELSE 'job'
		END AS query_type,
		day
	FROM
		orc.`s3://lakehouse/ranger/audit`
	WHERE
		day>='20251125' and resourceType = 'table'
	ORDER BY
		eventTime)
select
	table,
	query_type,
	day,
	count(*) as stats_count
from
	audit
group by
	table,
	query_type,
	day; 
```

This will create table stats for multiple days like below:

![](images/image2.png)