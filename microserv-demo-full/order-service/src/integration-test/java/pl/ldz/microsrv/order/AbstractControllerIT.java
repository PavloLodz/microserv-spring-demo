package pl.ldz.microsrv.order;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for controller integration tests.
 *
 * <p>Starts a shared, reusable {@link PostgreSQLContainer} via Testcontainers and wires its
 * JDBC URL, username, and password into the Spring context through
 * {@link DynamicPropertySource}. Subclasses annotate themselves with
 * {@code @SpringBootTest(webEnvironment = RANDOM_PORT)} and inherit the container.
 *
 * <p>The container is declared {@code static} so JUnit 5 reuses it across all test methods
 * in the same class (and across subclasses in the same JVM run when Testcontainers reuse is
 * enabled), avoiding repeated container start/stop overhead.
 */
@Testcontainers
public abstract class AbstractControllerIT {

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("order_test")
                    .withUsername("test")
                    .withPassword("test");

    /**
     * Overrides Spring datasource properties with the Testcontainers-assigned values
     * before the application context is refreshed.
     */
    @DynamicPropertySource
    static void overrideDataSourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        // Disable Kafka for tests that do not require it
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:9999");
        registry.add("spring.autoconfigure.exclude",
                () -> "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration");
    }

    @Autowired
    protected TestRestTemplate restTemplate;
}
