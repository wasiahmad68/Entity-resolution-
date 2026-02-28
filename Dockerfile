FROM python:3.10-slim

WORKDIR /app

# Install system dependencies required for hashing & psycopg2
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Ensure standard import paths map natively
ENV PYTHONPATH=/app

# Make entrypoint executable
RUN chmod +x docker-entrypoint.sh

# Run entrypoint script
ENTRYPOINT ["/app/docker-entrypoint.sh"]
