package pl.ldz.microsrv.common.event;

import java.util.UUID;

public record OrderCreatedEvent(UUID orderId, UUID userId) {}
