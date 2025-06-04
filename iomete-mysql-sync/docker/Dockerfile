FROM iomete.azurecr.io/iomete/spark-py:3.5.3-v10

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add source code to the /app directory
COPY ../setup.py setup.cfg driver.py ./
COPY ../iomete_mysql_sync ./iomete_mysql_sync

# Create and activate a virtual environment
RUN python3 -m venv /opt/venv \
    && . /opt/venv/bin/activate \
    && pip install --upgrade pip \
    && pip install .

# Ensure future RUN commands use the venv's Python and pip
ENV PATH="/opt/venv/bin:$PATH"

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}