# PySpark - Query Scheduler | IOMETE

## Deployment

- Go to `Spark Jobs`.
- Click on `Create New`.

Specify the following parameters (these are examples, you can change them based on your preference):

```json
{
  "mainApplicationFile": "https://raw.githubusercontent.com/iomete/query-scheduler-job/main/job.py",
  "deps": {
    "pyFiles": [
      "https://github.com/iomete/query-scheduler-job/raw/main/infra/dependencies.zip"
    ]
  }
}
``` 

And add config(`/etc/configs/application.conf`): 

```hocon
# EXAMPLE

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

## Build Dependencies

```shell
make build-dependencies
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
