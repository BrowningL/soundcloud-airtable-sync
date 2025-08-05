# Keep Playwright image since your playcount service needs it
FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

WORKDIR /app

# Install deps first for better caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire repo so both scripts exist
COPY . /app

# Default to your existing playcount app
ENV RUN_CMD="python app.py"

# Allow overriding with Railway Start Command or env var
CMD ["sh", "-lc", "$RUN_CMD"]
