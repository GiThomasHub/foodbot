FROM python:3.11-slim

# Schnelle, saubere Logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONOPTIMIZE=1 \
    PORT=8080

# Fonts für PDF (DejaVu)
RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1) Requirements zuerst: maximiert Build-Cache
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 2) Dann den restlichen Code
COPY . .

# Startet deinen Bot (Cloud Run setzt $PORT → dein Code nutzt ihn)
CMD ["python", "menu_bot_7.py"]
