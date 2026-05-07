FROM iomete.azurecr.io/iomete/spark-py:3.5.5-v13

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add source code to the /app directory
COPY setup.py setup.cfg driver.py ./
COPY file_streaming ./file_streaming

RUN pip install --break-system-packages .

# Ensure the runtime Spark user can write to the working directory.
# Spark's entrypoint writes java_opts.txt in the current working directory.
ARG spark_uid=1001
RUN chown -R ${spark_uid}:${spark_uid} /app

# Specify the User that the actual main process will run as
USER ${spark_uid}
