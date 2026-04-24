docker compose up -d orders-db && mvn spring-boot:run -pl order-service

# 1. Start the DB container and wait for it to be healthy
docker compose up -d orders-db

# 2. Wait a few seconds, then confirm it's ready
docker compose ps orders-db

# 3. Then start the app
mvn spring-boot:run -pl order-service

# See what's listening on 5432
ss -tlnp | grep 5432

# If something else is there, stop it or change the port mapping in docker-compose.yml
# e.g. change "5432:5432" to "5433:5432" and update application.yml datasource url to port 5433