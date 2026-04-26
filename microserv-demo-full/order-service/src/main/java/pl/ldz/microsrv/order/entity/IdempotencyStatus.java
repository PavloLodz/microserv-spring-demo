package pl.ldz.microsrv.order.entity;

/**
 * Type-safe status values for {@link IdempotencyKey}.
 *
 * <p>Stored as a {@code VARCHAR(50)} column via {@code @Enumerated(EnumType.STRING)}.
 *
 * <ul>
 *   <li>{@link #IN_PROGRESS} — the request is currently being processed; concurrent requests
 *       with the same key are rejected with HTTP 409 until this transitions to
 *       {@link #COMPLETED}.</li>
 *   <li>{@link #COMPLETED} — the request finished successfully; the serialised response body
 *       is stored and returned verbatim on any subsequent replay.</li>
 * </ul>
 */
public enum IdempotencyStatus {

  /** The owning request is currently executing. */
  IN_PROGRESS,

  /** The owning request completed; the response body is sealed in the record. */
  COMPLETED
}
