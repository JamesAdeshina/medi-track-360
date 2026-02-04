#!/bin/bash
# Initialize Airflow database and create admin user

echo "Initializing Airflow database..."

# Check if PostgreSQL is ready
until docker-compose exec -T postgres pg_isready -U airflow; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

echo "PostgreSQL is ready"

# Initialize Airflow database
docker-compose run --rm airflow-webserver airflow db init

# Create admin user
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

echo "Airflow initialization complete!"
echo "Run 'docker-compose up -d' to start services"