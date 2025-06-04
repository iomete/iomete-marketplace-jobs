FROM iomete.azurecr.io/iomete/spark-py:3.5.3-v12

# Reset to root to run installation tasks
USER 0

# Install curl and install Poetry using the official script
RUN apt-get update && apt-get install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variables to avoid Poetry creating virtualenvs inside the container
ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

# Set working directory
WORKDIR "/app"

# Copy only pyproject.toml and poetry.lock first to leverage Docker caching
COPY pyproject.toml ./

# Install dependencies
ENV POETRY_VIRTUALENVS_CREATE=true
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="/app/.venv/bin:$PATH"
RUN poetry install --no-root

COPY app.py ./

# Specify the User that the actual main process will run as
ARG spark_uid=1001
USER ${spark_uid}

