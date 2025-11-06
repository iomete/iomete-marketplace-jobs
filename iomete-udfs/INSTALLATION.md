# Installing the IOMETE UDF Package

Follow this guide to obtain the UDF JAR and expose it to your Spark cluster. Once installed, refer to `HttpRequest.md` for usage examples.

---

## 1. Build or download the JAR

### Build from source

```bash
git clone https://github.com/iomete/iomete-marketplace-jobs.git
cd iomete-marketplace-jobs/iomete-udfs
sbt package
```

The compiled artifact is stored at:

```
target/scala-2.12/iomete-udfs_2.12-0.1.0.jar
```

### Use the prebuilt artifact

If you prefer a ready-made build, use the packaged JAR provided with the repository:

```
builds/iomete-udfs_2.12-0.1.0.jar
```

---

## 2. Upload the JAR to shared storage

Place the JAR somewhere accessible by every Spark node (S3, HDFS, DBFS, Azure Storage, etc.). Example using Amazon S3:

```bash
aws s3 cp builds/iomete-udfs_2.12-0.1.0.jar \
  s3://your-bucket/libs/iomete-udfs_2.12-0.1.0.jar
```

Record the fully qualified URI (e.g. `s3a://your-bucket/libs/iomete-udfs_2.12-0.1.0.jar`); you will need it when registering the Hive function.

---

## 3. Register the Hive UDF

Run the following SQL command. It will persist the definition in the Hive metastore and will be available for all future sessions.

```sql
CREATE OR REPLACE FUNCTION spark_catalog.default.http_request
AS 'com.iomete.spark.udf.HttpRequest'
USING JAR 's3a://your-bucket/libs/iomete-udfs_2.12-0.1.0.jar';
```

Adjust the schema prefix (`default`) if you prefer to host the function in another schema.

After completing these steps, the `http_request` function is available cluster-wide. Proceed to `HttpRequest.md` to see detailed usage patterns.
