FROM iomete.azurecr.io/iomete/spark-py:3.5.3-v7

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# Install curl and install Poetry using the official script
RUN apt-get update && apt-get install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# add source code to the /app directory
COPY driver.py pyproject.toml ./
COPY data_compaction_job ./data_compaction_job

# Install dependencies
ENV POETRY_VIRTUALENVS_CREATE=true
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
RUN poetry install --no-root

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
