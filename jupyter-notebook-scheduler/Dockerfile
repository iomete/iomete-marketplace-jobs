FROM iomete.azurecr.io/iomete/spark-py:3.5.7-latest

USER root

# git is required by GitPython for the `input.type: git` source. Without it the
# job fails at startup with "Bad git executable".
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/work-dir

# Install the locked Python dependencies. poetry.lock is committed so the image
# is reproducible; poetry is only used to export the lock to requirements and is
# removed afterwards to keep the layer small.
COPY pyproject.toml poetry.lock ./
RUN pip install --no-cache-dir poetry poetry-plugin-export && \
    poetry export -f requirements.txt --without-hashes --output requirements.txt && \
    pip install --no-cache-dir -r requirements.txt && \
    pip uninstall -y poetry poetry-plugin-export && \
    rm -f requirements.txt

# Application code.
COPY main.py config.py fetcher.py executor.py config.yaml ./

# Drop back to the non-root Spark user the base image runs as.
USER 185
