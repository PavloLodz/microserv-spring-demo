package pl.ldz.microsrv.common.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderUpdatedEvent(UUID orderId, UUID customerId, BigDecimal totalAmount, OrderStatus status, Instant occurredAt) {}
