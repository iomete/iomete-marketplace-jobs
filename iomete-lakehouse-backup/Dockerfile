# check=skip=InvalidDefaultArgInFrom
# No default: Makefile injects SPARK_IMAGE from gradle.properties.
ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}

COPY build/libs/iomete-lakehouse-backup-*.jar /opt/spark/jars/iomete-lakehouse-backup.jar

# Two copies of software.amazon.awssdk on one classpath link at random and break S3 rename (DEL-452).
RUN rm -f /opt/spark/jars/iceberg-aws-bundle-*.jar \
 && test "$(ls /opt/spark/jars | grep -cE '^(bundle|iceberg-aws-bundle)-')" -eq 1
