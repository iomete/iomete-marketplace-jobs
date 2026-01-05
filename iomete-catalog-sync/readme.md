# Running Locally

```bash
# 1, Build code
../gradlew clean quarkusBuild

# 2. Update docker tag version in Makefile
# 3. Build image and push
make docker-push
```

### 1. Setup IOMETE Spark Folder
```shell
make download-spark
make download-libraries
```

### 2. Port Forward Metastore pod from dev kubernetes

### 3. Generate Random Tables
```shell
make spark-sql

---
use iceberg_catalog;
create database icedb;
use icedb;

create table sample_tbl(id int);
insert into sample_tbl (id) values(1);
select * from sample_tbl;
```

### 4. Setup Mock Server
```shell
python3 mock.py
```

### 5. Submit your jar
```shell
make submit-local
```
