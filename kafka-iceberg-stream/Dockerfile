# Choose the IOMETE managed Spark image as the base image.
# It already ships Hadoop, Scala, snappy, lz4, zstd and slf4j, so only the Kafka connector is added below.
FROM iomete.azurecr.io/iomete/spark-py:3.5.7-v7

# Keep SPARK_VERSION in step with the base image.
ARG SPARK_VERSION=3.5.7
# Spark 3.5.7 pins kafka-clients 3.4.1, which has known CVEs; override with a patched release.
ARG KAFKA_CLIENTS_VERSION=3.9.2
ARG COMMONS_POOL2_VERSION=2.12.1
ARG MAVEN_REPO=https://repo1.maven.org/maven2

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# Download the Kafka connector jars and verify their checksums.
# CI offers a maven-settings secret pointing at the in-cluster Nexus; without it, MAVEN_REPO is used.
RUN --mount=type=secret,id=maven-settings,target=/run/secrets/maven-settings.xml \
    set -eux; \
    repo="${MAVEN_REPO}"; \
    if [ -s /run/secrets/maven-settings.xml ]; then \
      repo="$(sed -n 's:.*<url>\(.*\)</url>.*:\1:p' /run/secrets/maven-settings.xml | head -n 1)"; \
      repo="${repo%/}"; \
    fi; \
    cd /opt/spark/jars; \
    while read -r sha path; do \
      file="$(basename "$path")"; \
      curl -fsSL --retry 5 --retry-all-errors -o "$file" "${repo}/${path}"; \
      echo "${sha}  ${file}" | sha256sum -c -; \
    done <<EOF
184f8bd4a3cd22299c49090d88b894f81fd7546b4ee64879519a0d76a685dbb8 org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar
9701914db5c6d09628b58d2d2c1ccd2c9ea28f32f83dd6cb21f98b8237531ff4 org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar
014b4ef36884478a932641818b080c36a8f8e2e92dc771e800c9d2694f4f83d8 org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar
5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar
EOF

# add requirements.txt first to leverage Docker cache
COPY infra/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt --break-system-packages

# Copy the application files
COPY config.py job.py ./

# Copy log4j confgiration. To use this log4j configuration, set JAVA options: -Dlog4j.configurationFile=/opt/spark/iomete/log4j2.properties
COPY spark_conf/log4j2.properties /opt/spark/iomete/log4j2.properties

# Specify the User that the actual main process will run as (the base image's spark user)
ARG spark_uid=1001
USER ${spark_uid}
