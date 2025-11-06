# HttpRequest UDF Usage

`com.iomete.spark.udf.HttpRequest` performs an HTTP `GET` request and returns the response body as a string. Install the UDF by following the steps in `INSTALLATION.md`, then invoke it from Spark SQL or the DataFrame API as shown below.

---

## SQL examples

The function accepts:

- `url` *(STRING)* – the target endpoint.
- `params` *(MAP<STRING,STRING>, optional)* – query parameters appended to the URL.
- `headers` *(MAP<STRING,STRING>, optional)* – HTTP headers attached to the request.

### Example 1 – full URL only

```sql
SELECT http_request('https://api.open-meteo.com/v1/forecast?latitude=40.4&longitude=49.8&current_weather=true');
```

### Example 2 – URL plus query parameters

```sql
SELECT http_request(
  'https://api.open-meteo.com/v1/forecast',
  map('latitude','40.4','longitude','49.8','current_weather','true')
);
```

### Example 3 – URL, parameters, and headers

```sql
SELECT http_request(
  'https://api.open-meteo.com/v1/forecast',
  map('latitude','40.4','longitude','49.8','current_weather','true'),
  map('User-Agent','SparkSQL','Accept','application/json')
);
```

`map(k1, v1, k2, v2, …)` constructs the `MAP<STRING,STRING>` values expected by the UDF. Null values are permitted—omit a key or pass `NULL` when a parameter should be blank.

### Example 4 – Registering a temporary alias (session-only)

```sql
CREATE TEMPORARY FUNCTION http_request AS 'com.iomete.spark.udf.HttpRequest' 
       USING JAR 's3a://your-bucket/libs/iomete-udfs_2.12-0.1.0.jar';
       
SELECT http_request('https://api.open-meteo.com/v1/forecast?latitude=40.4&longitude=49.8&current_weather=true');
```

---

## DataFrame API example

```scala
import org.apache.spark.sql.functions.expr

val df = spark.range(1)
  .selectExpr(
    "http_request(" +
    "'https://api.open-meteo.com/v1/forecast'," +
    "map('latitude','40.4','longitude','49.8','current_weather','true')" +
    ") as forecast"
  )

df.show(false)
```

You can construct `MapType` columns programmatically, or rely on SQL expressions as shown above.

---

## Operational notes

- **Timeouts:** The UDF uses a 5-second connect/read timeout. Adjust the code if longer waits are required.
- **Security:** Confirm that outbound HTTP calls are allowed from your Spark executors; configure proxies or VPC endpoints if necessary.
- **Error handling:** Non-2xx responses return the response body (typically the error payload). Null or blank URLs return `NULL`.
- **Updates:** When rolling out a new JAR, update the file in storage and re-run the `CREATE FUNCTION` statements (or `ALTER FUNCTION … SET FILE`).

That’s all you need to provide an HTTP GET UDF across your Spark SQL users. Reach out to your platform team if the cluster requires additional steps (custom trust stores, firewall rules, etc.).
