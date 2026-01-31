FROM python:3.12-slim

WORKDIR /project

# Install system dependencies for postgres and other taps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    git \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Meltano
RUN pip install --no-cache-dir "meltano>=3.0,<4.0"

# Copy Meltano project files
# Note: dbt project is mounted via docker-compose volume
# Note: Environment variables are injected via docker-compose env_file
COPY meltano.yml /project/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set environment variables
ENV MELTANO_ENVIRONMENT=dev

# Note: Plugins are installed at runtime via init.sh or on first meltano run
# This avoids network issues during build and allows for easier development

# Health check verifies meltano CLI is working
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD meltano --version || exit 1

# Use entrypoint to install certs before running
ENTRYPOINT ["/entrypoint.sh"]
CMD ["tail", "-f", "/dev/null"]
