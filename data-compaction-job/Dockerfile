FROM iomete.azurecr.io/iomete/spark-py:3.5.7-v7

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add source code to the /app directory
COPY setup.py setup.cfg driver.py ./
COPY data_compaction_job ./data_compaction_job

RUN pip install .  --break-system-packages

# Reduce base-image CVEs:
# - upgrade OS packages that have security fixes not yet in the base image
# - purge linux-libc-dev (kernel headers) and the C/C++ build toolchain that depends on it; not needed at runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade \
        openssl libssl3t64 apt libapt-pkg6.0t64 libbrotli1 \
    && apt-get purge -y linux-libc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Specify the User that the actual main process will run as (the base image's spark user)
ARG spark_uid=1001
USER ${spark_uid}
