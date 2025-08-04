# Fastest path: use Microsoft’s Playwright Python image (Chromium preinstalled)
FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

# System updates are already applied in the base image; just set up the app.
WORKDIR /app

# Copy dependency list first for better layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY app.py /app/app.py

# (Optional) If you want to pin the Chromium build explicitly:
# RUN playwright install chromium

# Railway will run this by default (or set Start Command to "python app.py")
CMD ["python", "app.py"]
