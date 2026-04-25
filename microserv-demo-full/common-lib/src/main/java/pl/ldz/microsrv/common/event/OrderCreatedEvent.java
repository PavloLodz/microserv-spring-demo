package pl.ldz.microsrv.common.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(UUID orderId, UUID customerId, BigDecimal totalAmount, Instant occurredAt) {}
