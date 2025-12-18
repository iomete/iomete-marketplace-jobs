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