package pl.ldz.microsrv.order.exception;

/**
 * Thrown when an idempotency-related payload cannot be serialised or deserialised.
 *
 * <p>Covers two failure sites in {@link pl.ldz.microsrv.order.service.OrderService}:
 * <ul>
 *   <li>Deserialising a cached {@code OrderResponse} from the idempotency store on replay.</li>
 *   <li>Serialising the {@code OrderResponse} to store in the idempotency record after a
 *       successful first execution.</li>
 * </ul>
 * Also thrown by {@link pl.ldz.microsrv.order.service.IdempotencyService#computeHash} when
 * the request body cannot be serialised to JSON for hashing.
 *
 * <p>Because serialisation failures are unrecoverable (the same object will fail on every
 * retry), this exception causes the enclosing {@code @Transactional} scope to roll back,
 * leaving the system in a consistent state.
 *
 * <p>Maps to HTTP 500 via
 * {@link GlobalExceptionHandler#handleIdempotencySerialization}.
 * The response body is kept opaque ("An unexpected error occurred.");
 * the full cause is logged internally at ERROR level.
 *
 * <p>Extends {@link RuntimeException} (unchecked) so callers do not need to declare it.
 */
public class IdempotencySerializationException extends RuntimeException {

  public IdempotencySerializationException(String detail, Throwable cause) {
    super(detail, cause);
  }
}
