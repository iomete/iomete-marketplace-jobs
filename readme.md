# iomete: Pyspark query scheduler job

## Deployment

- Go to `Spark Jobs`.
- Click on `Create New`.

Specify the following parameters (these are examples, you can change them based on your preference):

- **Name:** `query-scheduler-job`
- **Schedule:** `0 0/22 1/1 * *`
- **Docker Image:** `iomete/query_scheduler_job:1.0.0`
- **Main application file:** `local:///app/driver.py`
- **Config file:**

Example config:
```hocon
# Queries to be run sequentially
[
  # let's create an example database
  """
  CREATE DATABASE EXAMPLE
  """,

  # use the newly created database to run the further queries within this database
  """
  USE EXAMPLE
  """,

  # query example one
  """
  CREATE TABLE IF NOT EXISTS dept_manager_proxy
  USING org.apache.spark.sql.jdbc
  OPTIONS (
    url "jdbc:mysql://iomete-tutorial.cetmtjnompsh.eu-central-1.rds.amazonaws.com:3306/employees",
    dbtable "employees.dept_manager",
    driver 'com.mysql.cj.jdbc.Driver',
    user 'tutorial_user',
    password '9tVDVEKp'
  )
  """,

  # another query that depends on the previous query result
  """
  CREATE TABLE IF NOT EXISTS dept_manager AS SELECT  * FROM dept_manager_proxy
  """
]
```

## Development

**Prepare the dev environment**

```shell
virtualenv .env #or python3 -m venv .env
source .env/bin/activate

make install-dev-requirements
```

**Run test**

```shell
pytest
```
