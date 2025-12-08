FROM iomete.azurecr.io/iomete/spark-py:3.5.3-v12

USER root

# Install poetry
RUN pip install poetry --break-system-packages

WORKDIR "/app"

# Copy poetry configuration
COPY pyproject.toml poetry.lock* ./

# Install dependencies
ENV POETRY_VIRTUALENVS_CREATE=true
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="/app/.venv/bin:$PATH"
RUN poetry install --no-root

# Copy application code
COPY config.py main.py ./
COPY scheduler ./scheduler

ARG spark_uid=1001
USER ${spark_uid}
