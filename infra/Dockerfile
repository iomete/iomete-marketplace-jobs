# Choose the IOMETE managed Spark image as the base image
FROM iomete/spark-py:3.3.3-latest


# Set the working directory to /app
WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add requirements.txt first to leverage Docker cache
COPY infra/requirements.txt ./
RUN pip install -r requirements.txt

# Copy the application files
COPY job.py ./

# Copy log4j confgiration. To use this log4j configuration, set JAVA options: -Dlog4j.configurationFile=/opt/spark/iomete/log4j2.properties
COPY spark_conf/log4j2.properties /opt/spark/iomete/log4j2.properties

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}