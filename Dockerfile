FROM python:3.11-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl apt-transport-https gnupg2 unixodbc-dev gcc g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft repo for SQL Server ODBC Driver 18
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port (informative, Railway ignores this and uses PORT env var)
EXPOSE 8000

# Command to run the application using Railway's dynamic PORT
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
