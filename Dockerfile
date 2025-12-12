FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# IMPORTANT: run on port 80 for Zeabur
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
