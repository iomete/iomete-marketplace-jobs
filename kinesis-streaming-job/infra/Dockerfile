FROM iomete/spark-py:3.2.1-latest

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add source code to the /app directory
COPY infra/requirements.txt ./
COPY spark_conf/log4j.properties /opt/spark/iomete/log4j.properties
COPY jars/spark-sql-kinesis_2.13-1.2.2_spark-3.2.jar /opt/spark/jars/spark-sql-kinesis_2.13-1.2.2_spark-3.2.jar

RUN pip install -r requirements.txt

COPY job.py ./

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}