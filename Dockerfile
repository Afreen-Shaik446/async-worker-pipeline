FROM python:3.12-slim
RUN useradd --create-home --shell /usr/sbin/nologin appuser
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY worker ./worker
USER appuser
EXPOSE 8080
CMD ["python", "-m", "worker.consumer"]
