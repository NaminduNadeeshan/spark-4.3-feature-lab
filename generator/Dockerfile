FROM python:3.12-slim
RUN pip install --no-cache-dir kafka-python-ng==2.2.3 requests==2.32.*
COPY app.py /app/app.py
CMD ["python", "-u", "/app/app.py"]
