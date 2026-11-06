FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

ENV HOST=0.0.0.0
ENV PORT=8096
EXPOSE 8096

CMD ["python", "server.py"]
