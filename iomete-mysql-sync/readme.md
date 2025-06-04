# IOMETE: MySQL Sync (DB Replication)

Quickly move your MySQL tables to IOMETE Lakehouse using our Spark Job that's simple to configure. All you need to do is provide the settings, and the job will handle the rest. Choose how you want to transfer your data: either a full load or in smaller increments. Scroll down for more details on these sync options.

**See [MySQL Database Replication Job](https://iomete.com/docs/iomete-spark-job/mysql-database-replication-job) to learn more about this Spark Job.**

## Development

**Prepare the dev environment**

```shell
virtualenv .env #or python3 -m venv .env
source .env/bin/activate

pip install -e ."[dev]"
```

**Run test**

```shell
pytest
```