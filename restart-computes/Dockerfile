FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY restart_computes.py .
COPY README.md .
COPY .env.example .

CMD ["python", "restart_computes.py"]