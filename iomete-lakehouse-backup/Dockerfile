# Keep in step with sparkVersion and sparkImageRevision in gradle.properties.
ARG SPARK_IMAGE=iomete.azurecr.io/iomete/spark:3.5.7-v2

# Builds the jar inside the image, so a clean checkout is enough.
FROM eclipse-temurin:17-jdk AS build
WORKDIR /build
COPY . .
RUN ./gradlew --no-daemon clean jar

FROM ${SPARK_IMAGE}
COPY --from=build /build/build/libs/iomete-lakehouse-backup-*.jar /opt/spark/jars/iomete-lakehouse-backup.jar

# Two copies of software.amazon.awssdk on one classpath link at random and break S3 rename (DEL-452).
RUN rm -f /opt/spark/jars/iceberg-aws-bundle-*.jar \
 && test "$(ls /opt/spark/jars | grep -cE '^(bundle|iceberg-aws-bundle)-')" -eq 1
