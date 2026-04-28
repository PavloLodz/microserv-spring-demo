package pl.ldz.microsrv.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Base class for integration tests that mock Kafka entirely and therefore do not need
 * a running Kafka broker container.
 *
 * <p>Only a {@link PostgreSQLContainer} is started. The Kafka bootstrap-servers property
 * is pointed at an unreachable address ({@code localhost:9}), and
 * {@code spring.kafka.admin.fail-fast=false} prevents {@code KafkaAdmin} from aborting
 * application startup when the broker is unavailable. Since the {@code KafkaTemplate}
 * bean is replaced by a {@code @MockBean} in every subclass, no real Kafka traffic occurs.
 *
 * <p>Use this base class instead of {@link AbstractControllerIT} when the test scenario
 * deliberately makes Kafka unavailable (e.g. to verify the outbox FAILED terminal state).
 */
@Testcontainers(disabledWithoutDocker = false)
public abstract class AbstractNoKafkaControllerIT {

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("order_test")
                    .withUsername("test")
                    .withPassword("test");

    /**
     * Fails the entire test class when Docker is not reachable, consistent with the
     * policy in {@link AbstractControllerIT}.
     */
    @BeforeAll
    static void requireDocker() {
        if (DockerClientFactory.instance() == null) {
            fail("DockerClientFactory.instance() == null");
        }
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            fail("DockerClientFactory.instance().isDockerAvailable() == false, so it is not available!");
        }
    }

    /**
     * Wires Postgres from the container. Kafka bootstrap-servers is set to an unreachable
     * address; {@code spring.kafka.admin.fail-fast=false} prevents context startup failure
     * when the broker is absent. Subclasses supply a {@code @MockBean KafkaTemplate} so
     * no real Kafka sends are attempted.
     */
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        // Point Kafka at an unreachable address — KafkaTemplate is mocked in every subclass
        // so no real broker traffic occurs. fail-fast=false prevents startup abort.
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:9");
        registry.add("spring.kafka.admin.fail-fast", () -> "false");
    }

    @Autowired protected TestRestTemplate restTemplate;
    @Autowired protected ObjectMapper objectMapper;
}
