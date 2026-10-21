FROM iomete.azurecr.io/iomete/spark-py:3.5.5-v4

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY ../setup.py setup.cfg driver.py ./
COPY ../ras_onboarding ./ras_onboarding


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