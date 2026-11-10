FROM iomete.azurecr.io/iomete/spark-py:3.5.7-v7

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add source code to the /app directory
COPY setup.py setup.cfg driver.py ./
COPY data_compaction_job ./data_compaction_job

RUN pip install .  --break-system-packages

# Reduce base-image CVEs until the spark-py base image ships these fixes:
# - apply pending Ubuntu security updates (Java is not apt-managed, so it is unaffected)
# - purge linux-libc-dev (kernel headers) and the C/C++ build toolchain that depends on it; not needed at runtime
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && apt-get purge -y linux-libc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Specify the User that the actual main process will run as (the base image's spark user)
ARG spark_uid=1001
USER ${spark_uid}
