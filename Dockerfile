FROM python:3.11-slim

# Fonts für PDF (DejaVu)
RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PORT=8080 PYTHONUNBUFFERED=1
CMD ["python", "menu_bot_7.py"]
