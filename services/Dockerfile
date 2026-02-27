FROM python:3.11-slim

WORKDIR /app

COPY services/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ .
COPY shared/ ./shared/
