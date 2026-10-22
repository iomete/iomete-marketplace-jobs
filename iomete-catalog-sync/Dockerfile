# Builds the jar inside the image, so a clean checkout is enough.
FROM eclipse-temurin:17-jdk AS build
WORKDIR /build
COPY . .
RUN ./gradlew --no-daemon clean quarkusBuild

FROM iomete.azurecr.io/iomete/spark:3.5.7-v6
COPY --from=build /build/build/*-runner.jar /opt/spark/jars/
