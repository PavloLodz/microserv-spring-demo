package pl.ldz.microsrv.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Base class for controller integration tests.
 *
 * <p>Starts a shared, reusable {@link PostgreSQLContainer} and {@link KafkaContainer} via
 * Testcontainers and wires their connection properties into the Spring context through
 * {@link DynamicPropertySource}. Subclasses annotate themselves with
 * {@code @SpringBootTest(webEnvironment = RANDOM_PORT)} and inherit both containers.
 *
 * <p>If Docker is not available in the current environment the entire test class is
 * <em>failed</em> (not failed) via a JUnit assumption in {@link #requireDocker()}.
 * This is a BUILD FAILURE.
 *
 * <p>Both containers are declared {@code static} so JUnit 5 reuses them across all test
 * methods in the same class, avoiding repeated container start/stop overhead.
 */
@Testcontainers(disabledWithoutDocker = false)
public abstract class AbstractControllerIT {

  @Container
  protected static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("order_test")
          .withUsername("test")
          .withPassword("test");

  @Container // TODO: Is 'org.testcontainers.containers.KafkaContainer' deprecated ?
  protected static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

  /**
     * Fails the entire test class when Docker is not reachable.
     *
     * <p>It is a problem, so the build in not GREEN in environments where
     * Docker is unavailable (CI without DinD, restricted sandboxes, etc.).
     */
  @BeforeAll
  static void requireDocker() {
    /*
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Docker is not available — skipping integration tests"
    );
    */
    // I think it MUST tell if there is a problem with Docker.
    if (DockerClientFactory.instance() == null) {
      fail("DockerClientFactory.instance() == null");
    }
    if (!DockerClientFactory.instance().isDockerAvailable()) {
      fail("DockerClientFactory.instance().isDockerAvailable() == false, so it is not available!");
    }
  }

  /**
     * Overrides Spring datasource and Kafka properties with the Testcontainers-assigned
     * values before the application context is refreshed.
     */
  @DynamicPropertySource
  static void overrideProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @Autowired protected TestRestTemplate restTemplate;
  @Autowired protected ObjectMapper objectMapper;

  /**
     * Creates a ready-to-poll {@link KafkaConsumer} subscribed to {@code orders.events.v1}.
     *
     * <p>Callers are responsible for closing the consumer when done.
     *
     * @param groupId a unique consumer group id per test to avoid offset interference
     * @return a configured consumer subscribed to the orders events topic
     */
  protected KafkaConsumer<String, String> createTestConsumer(String groupId) {
    Map<String, Object> props = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG, groupId,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
    );
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of("orders.events.v1"));
    return consumer;
  }

  protected String sha256Hex(byte[] data) throws NoSuchAlgorithmException {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
  }
}
