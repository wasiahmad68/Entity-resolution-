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

# Keep the container running infinitely so users can shell in, run tests, or execute batch scripts seamlessly
CMD ["tail", "-f", "/dev/null"]
