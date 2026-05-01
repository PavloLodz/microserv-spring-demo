# Several commands for building & testing the project

```bash
mvn clean install -pl common-lib
mvn clean package -DskipTests
mvn clean package

mvn generate-sources
```

# Run Tests

## Unit tests only. Fast, no Docker required. Tests are located in `src/test/java`.
```bash
mvn clean test
```

## Integration tests only. Requires Docker (Testcontainers). Tests are located in `src/integration-test/java`.
```bash
mvn clean integration-test
mvn clean verify -Pfailsafe
```

## All tests (Unit + Integration). Requires Docker (Testcontainers).
```bash
mvn clean verify
```

mvn dependency:tree | grep docker-java
cat ~/.testcontainers.properties

