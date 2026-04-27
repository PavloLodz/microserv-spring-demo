package pl.ldz.microsrv.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
// @EnableScheduling is required for @Scheduled methods (e.g. OutboxService.pollAndPublish).
// Removing it will silently disable the outbox poller with no startup error — events will
// accumulate in outbox_event indefinitely and never be published to Kafka.
@EnableScheduling
public class OrderServiceApplication {
  public static void main(String[] args) {
    SpringApplication.run(OrderServiceApplication.class, args);
  }
}
