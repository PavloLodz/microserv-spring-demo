package pl.ldz.microsrv.common.event;

import java.time.Instant;
import java.util.UUID;

public record OrderDeletedEvent(UUID orderId, Instant occurredAt) {}
