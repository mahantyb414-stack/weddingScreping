# Playwright 1.44.0 ships Chromium ~125 — use the matching base image.
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (Chromium only to save space)
RUN playwright install chromium

COPY . .

# Render injects $PORT at runtime; default to 10000 for local Docker runs.
ENV PORT=10000

CMD uvicorn app.api:app --host 0.0.0.0 --port $PORT
