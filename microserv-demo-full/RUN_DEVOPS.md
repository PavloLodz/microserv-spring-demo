# DevOps Runbook

## Quick Start

```bash
# Start the DB and Kafka, then run the app on the host
docker compose up -d && mvn spring-boot:run -pl order-service
```

## Step-by-step

```bash
# 1. Start all infrastructure containers
docker compose up -d

# 2. Wait a few seconds, then confirm they are ready
docker compose ps

# 3. Start the application
mvn spring-boot:run -pl order-service
```

## Port Conflicts

```bash
# See what is listening on 5432
ss -tlnp | grep 5432

# If something else is there, stop it or change the port mapping in docker-compose.yml
# e.g. change "5432:5432" to "5433:5432" and update application.yml datasource url to port 5433
```

---

## Kafka Bootstrap Servers

Two bootstrap server addresses are available depending on how `order-service` is run:

| Address | When to use |
|---|---|
| `localhost:9092` | **Default.** Use when running `order-service` directly on the host (e.g. `java -jar` or `mvn spring-boot:run`). This is the `application.yml` fallback — no environment variable needed. |
| `kafka:29092` | Use when running `order-service` as a Docker container inside the same Compose network. Pass it via `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`. |

### Starting the full environment

```bash
docker compose up -d
```

After the T4 dual-listener change, Kafka starts with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`.
The broker will reject produces to topics that do not exist.
**The topic `orders.events.v1` is created automatically by the `KafkaTopicConfig` Spring bean
on `order-service` startup — the topic will not appear until `order-service` starts at least once.**

### Verifying the topic exists

After starting `order-service`, confirm the topic was created by `KafkaTopicConfig`:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 \
    --describe --topic orders.events.v1
```

Expected output: `PartitionCount: 3`, `ReplicationFactor: 1` (matching `application.yml` defaults).

### Verifying auto-create is disabled

```bash
# Should print UNKNOWN_TOPIC_OR_PARTITION — not silently create the topic
docker compose exec kafka kafka-console-producer --bootstrap-server kafka:29092 \
    --topic does-not-exist
```
