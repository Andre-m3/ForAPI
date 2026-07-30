# ---------------------------------------------------------------------------
# F1 Real-Time Data Lake API — Dockerfile
# ---------------------------------------------------------------------------
# Lightweight production image for the FastAPI + SignalR ingestion microservice.
# ---------------------------------------------------------------------------

FROM python:3.12-slim

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Default data directory for the SQLite database (overridden by Compose)
    DATA_DIR=/app/data

WORKDIR /app

# Install system dependencies required by some Python packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Create the data directory for the SQLite database
RUN mkdir -p /app/data

# Expose the FastAPI port
EXPOSE 8000

# Run Uvicorn with the FastAPI app
# Using --host 0.0.0.0 so the API is accessible outside the container
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]