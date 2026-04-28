package pl.ldz.microsrv.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics as Spring beans so that {@code KafkaAdmin} — auto-configured
 * by Spring Boot whenever {@code spring-kafka} is on the classpath — can create or
 * update them on application startup.
 *
 * <p>{@code TopicBuilder} is idempotent: if the topic already exists with the same
 * settings it is a no-op; no error is thrown and no partitions are modified.
 * If the topic exists with different settings only safe changes (e.g. increasing
 * partition count) are applied; unsafe changes (e.g. decreasing partition count or
 * changing the replication factor on a running cluster) are silently ignored by the
 * broker.
 *
 * <p>No manual {@code KafkaAdmin} bean needs to be declared here — Spring Boot's
 * auto-configuration creates one automatically from
 * {@code spring.kafka.bootstrap-servers}.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.orders-events:orders.events.v1}")
    private String ordersEventsTopic;

    @Value("${kafka.topic.orders-events-partitions:3}")
    private int partitions;

    @Value("${kafka.topic.orders-events-replicas:1}")
    private short replicas;

    /**
     * Declares the {@code orders.events.v1} topic with a configurable partition count
     * and replication factor. The values are read from {@code application.yml} and can
     * be overridden per environment via environment variables or an external config source.
     */
    @Bean
    public NewTopic ordersEventsTopic() {
        return TopicBuilder.name(ordersEventsTopic)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }
}
