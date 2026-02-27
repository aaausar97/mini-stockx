FROM python:3.11-slim

WORKDIR /app

COPY engine/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY engine/matcher.py .
COPY shared/ ./shared/

CMD ["python", "matcher.py"]
