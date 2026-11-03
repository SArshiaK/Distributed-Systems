FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

ENV HOST=0.0.0.0
ENV PORT=8080
ENV SHARD_MAP_PATH=/config/shard_map.json
EXPOSE 8080

CMD ["python", "server.py"]
