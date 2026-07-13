# No default: Makefile injects SPARK_IMAGE from gradle.properties.
ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}

COPY build/libs/iomete-lakehouse-backup-*.jar /opt/spark/jars/
