-- in case some ORC files are corrupted in s3 bucket
SET spark.sql.files.ignoreCorruptFiles = true;

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