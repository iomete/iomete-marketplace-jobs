# Choose the IOMETE managed Spark image as the base image
FROM iomete/spark-py:3.5.3-v7


# Set the working directory to /app
WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add requirements.txt first to leverage Docker cache
COPY infra/requirements.txt ./
RUN pip install -r requirements.txt --break-system-packages

# Copy the application files
COPY config.py job.py ./

COPY jars/commons-logging-1.1.3.jar /opt/spark/jars/commons-logging-1.1.3.jar
COPY jars/commons-pool2-2.11.1.jar /opt/spark/jars/commons-pool2-2.11.1.jar
COPY jars/hadoop-client-api-3.3.2.jar /opt/spark/jars/hadoop-client-api-3.3.2.jar
COPY jars/hadoop-client-runtime-3.3.2.jar /opt/spark/jars/hadoop-client-runtime-3.3.2.jar
COPY jars/jsr305-3.0.0.jar /opt/spark/jars/jsr305-3.0.0.jar
COPY jars/kafka-clients-2.8.1.jar /opt/spark/jars/kafka-clients-2.8.1.jar
COPY jars/lz4-java-1.7.1.jar /opt/spark/jars/lz4-java-1.7.1.jar
COPY jars/scala-library-2.12.15.jar /opt/spark/jars/scala-library-2.12.15.jar
COPY jars/slf4j-api-1.7.30.jar /opt/spark/jars/slf4j-api-1.7.30.jar
COPY jars/snappy-java-1.1.8.1.jar /opt/spark/jars/snappy-java-1.1.8.1.jar
COPY jars/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar
COPY jars/spark-tags_2.12-3.5.3.jar /opt/spark/jars/spark-tags_2.12-3.5.3.jar
COPY jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar
COPY jars/unused-1.0.0.jar /opt/spark/jars/unused-1.0.0.jar

# Copy log4j confgiration. To use this log4j configuration, set JAVA options: -Dlog4j.configurationFile=/opt/spark/iomete/log4j2.properties
COPY spark_conf/log4j2.properties /opt/spark/iomete/log4j2.properties

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}