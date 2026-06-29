FROM iomete.azurecr.io/iomete/spark-py:3.5.5-v11

WORKDIR /app
USER 0

#COPY test_connection.py /app/test_connection.py
COPY requirements.txt /app/requirements.txt

#RUN chown -R 1001:1001 /app /opt && chmod -R u+w /app /opt
#RUN apt-get update && apt-get install -yq --no-install-recommends && apt-get clean
RUN pip install --no-cache-dir -r /app/requirements.txt --break-system-packages

USER 185
# CMD ["python", "test_connection.py"]
