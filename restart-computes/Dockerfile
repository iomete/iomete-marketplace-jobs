FROM iomete.azurecr.io/iomete/spark-py:3.5.5-v11
WORKDIR /app

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY restart_computes.py .
COPY .env.example .

CMD ["python3", "restart_computes.py"]