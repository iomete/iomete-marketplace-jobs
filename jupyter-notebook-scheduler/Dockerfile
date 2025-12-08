FROM iomete.azurecr.io/iomete/spark-py:3.5.3-v12

USER root

# Install poetry
RUN pip install poetry --break-system-packages

RUN apt-get update && \
    apt-get install -y git build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR "/app"

# Copy poetry configuration
COPY pyproject.toml poetry.lock* ./

# Install dependencies
ENV POETRY_VIRTUALENVS_CREATE=true
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="/app/.venv/bin:$PATH"
RUN poetry install --no-root

# Register a python3 Jupyter kernel in the same environment used by papermill
RUN python -m ipykernel install --name python3 --display-name "Python 3 (scheduler)" --prefix=/usr/local

# Copy application code
COPY config.py main.py ./
COPY scheduler ./scheduler

ARG spark_uid=1001
USER ${spark_uid}
