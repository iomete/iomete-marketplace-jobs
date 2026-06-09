FROM iomete.azurecr.io/iomete/spark-py:3.5.5-v11

USER root

WORKDIR /opt/spark/work-dir

COPY pyproject.toml poetry.lock ./
RUN pip install --no-cache-dir --break-system-packages poetry poetry-plugin-export && \
    poetry export -f requirements.txt --without-hashes --output requirements.txt && \
    pip install --no-cache-dir --break-system-packages -r requirements.txt && \
    pip uninstall -y --break-system-packages poetry poetry-plugin-export && \
    rm -f requirements.txt

COPY main.py config.py executor.py config.yaml ./

USER 185
