FROM python:3.14-slim

WORKDIR /app

# Install dependencies including openssl for cert generation and curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssl \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    fastapi==0.109.0 \
    uvicorn==0.27.0 \
    faker==28.0.0

# Generate self-signed certificate for api.hubapi.com
RUN mkdir -p /certs && \
    openssl req -x509 -newkey rsa:4096 -nodes \
    -keyout /certs/key.pem -out /certs/cert.pem -days 365 \
    -subj "/CN=api.hubapi.com" \
    -addext "subjectAltName=DNS:api.hubapi.com,DNS:localhost"

COPY . /app/

# Expose both HTTP (for health checks) and HTTPS (for tap-hubspot)
EXPOSE 443

# Health check using curl with -k to skip SSL verification
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -fk https://localhost:443/health || exit 1

# Run with SSL on port 443
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "443", "--ssl-keyfile", "/certs/key.pem", "--ssl-certfile", "/certs/cert.pem"]
