FROM iomete/spark-py:3.5.3-v2

WORKDIR "/app"

COPY iomete_oracle_sync ./iomete_oracle_sync
COPY driver.py .
COPY requirements/spark_job_requirements.txt .

RUN python3 -m venv venv
ENV PATH="/app/venv/bin:$PATH"

RUN pip install -r spark_job_requirements.txt
