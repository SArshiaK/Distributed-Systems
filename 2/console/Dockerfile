FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .
COPY lab_experiments.py .
COPY bonus_live.py .
COPY static ./static

ENV HOST=0.0.0.0
ENV PORT=3000
EXPOSE 3000

CMD ["python", "server.py"]
