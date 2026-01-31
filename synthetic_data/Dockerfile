FROM python:3.14-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    faker==28.0.0 \
    psycopg2-binary==2.9.11 \
    boto3==1.34.0 \
    openpyxl==3.1.2 \
    pandas>=2.2.0

COPY . /app/

# Run seeder on container start
CMD ["python", "seed_all.py"]
