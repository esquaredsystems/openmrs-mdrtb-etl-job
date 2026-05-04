FROM python:3.12-slim

# Install MySQL client libraries (required by PyMySQL/SQLAlchemy)
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Default: run the full ETL pipeline (extract → transform → load).
# Override CMD in docker-compose or docker run to run individual steps,
# e.g. ["python", "main.py", "--extract", "--hard-reset"]
CMD ["python", "main.py", "--extract", "--transform", "--load"]
