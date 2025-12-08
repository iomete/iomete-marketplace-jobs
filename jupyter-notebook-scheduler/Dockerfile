FROM iomete.azurecr.io/iomete/spark-py:3.5.5-v7

USER root

# Install poetry
RUN pip install poetry

WORKDIR /app

# Copy poetry configuration
COPY pyproject.toml poetry.lock* /app/

# Install dependencies
# We disable virtualenvs to install directly into the system python
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Copy application code
COPY . /app
COPY config.py main.py ./
COPY scheduler ./scheduler

ARG spark_uid=185
USER ${spark_uid}
