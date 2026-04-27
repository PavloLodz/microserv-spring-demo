package pl.ldz.microsrv.order.exception;

/**
 * Thrown by {@link pl.ldz.microsrv.order.service.OutboxService#saveEvent} when the
 * domain event payload cannot be serialised to JSON.
 *
 * <p>Because serialisation failure is unrecoverable (the same object will fail on every
 * retry), this exception causes the enclosing {@code @Transactional} scope to roll back
 * both the order write and the outbox row — leaving the system in a consistent state.
 *
 * <p>Maps to HTTP 500 via {@link GlobalExceptionHandler#handleOutboxSerialization}.
 * The response body is kept opaque ({@code "An unexpected error occurred."});
 * the full cause is logged internally at ERROR level.
 *
 * <p>Extends {@link RuntimeException} (unchecked) so callers do not need to declare it.
 * No Lombok annotations are used — the single constructor is hand-written and trivial.
 */
public class OutboxSerializationException extends RuntimeException {

  public OutboxSerializationException(String eventType, Throwable cause) {
    super("Failed to serialise outbox payload for eventType=" + eventType, cause);
  }
}
