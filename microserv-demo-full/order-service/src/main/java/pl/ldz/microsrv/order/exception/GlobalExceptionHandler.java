package pl.ldz.microsrv.order.exception;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import pl.ldz.microsrv.order.api.model.ErrorResponse;

import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Centralised exception-to-HTTP-response mapping.
 *
 * <p>All responses use the {@link ErrorResponse} schema from the OpenAPI spec.
 * Stack traces and raw internal messages are never exposed in responses.
 *
 * <p>{@code debugId} (the order's internal BIGINT identity) is intentionally absent
 * from every response body and every OpenAPI schema, per the requirements.
 * A per-request correlation UUID is available to callers exclusively via the
 * {@code X-Debug-Id} response header written by
 * {@link pl.ldz.microsrv.order.filter.MdcFilter}. The same UUID is present in every
 * server-side log line via MDC so operators can correlate a client-visible token
 * with internal log entries without leaking internal identifiers in the response body.
 *
 * <h3>Fallback debugId in MDC</h3>
 * <p>If the MDC key {@code debugId} is absent when {@link #buildError} is called
 * (e.g. the request bypassed the servlet filter chain), a fresh UUID is generated and
 * written to MDC so that the ERROR-level log line always carries a correlation token.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

  // 404 Not Found

  @ExceptionHandler(OrderNotFoundException.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public ErrorResponse handleOrderNotFound(
      OrderNotFoundException ex, HttpServletRequest request) {
    // Task 4.6 - downgraded from log.info; 404s are normal client errors, not operator alerts
    log.debug("Order not found: {}", ex.getMessage());
    return buildError(HttpStatus.NOT_FOUND, "Not Found", ex.getMessage(), request.getRequestURI());
  }

  // 400 Bad Request

  @ExceptionHandler(MethodArgumentNotValidException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public ErrorResponse handleValidation(
      MethodArgumentNotValidException ex, HttpServletRequest request) {
    String message = ex.getBindingResult().getFieldErrors().stream()
        .map(FieldError::getDefaultMessage)
        .collect(Collectors.joining("; "));
    log.debug("Validation failure on {}: {}", request.getRequestURI(), message);
    return buildError(HttpStatus.BAD_REQUEST, "Bad Request", message, request.getRequestURI());
  }

  // Bad input from the client (e.g. invalid Idempotency-Key length) - not a server fault.
  @ExceptionHandler(IllegalArgumentException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public ErrorResponse handleIllegalArgument(
      IllegalArgumentException ex, HttpServletRequest request) {
    log.debug("Bad request on {}: {}", request.getRequestURI(), ex.getMessage());
    return buildError(HttpStatus.BAD_REQUEST, "Bad Request", ex.getMessage(), request.getRequestURI());
  }

  // 409 Conflict

  @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
  @ResponseStatus(HttpStatus.CONFLICT)
  public ErrorResponse handleOptimisticLock(
      ObjectOptimisticLockingFailureException ex, HttpServletRequest request) {
    log.warn("Optimistic locking conflict on {}", request.getRequestURI());
    return buildError(HttpStatus.CONFLICT, "Conflict",
        "The resource was modified concurrently. Please retry.", request.getRequestURI());
  }

  // Pessimistic lock timeout - another request holds the row lock on the idempotency key.
  @ExceptionHandler(PessimisticLockingFailureException.class)
  @ResponseStatus(HttpStatus.CONFLICT)
  public ErrorResponse handlePessimisticLock(
      PessimisticLockingFailureException ex, HttpServletRequest request) {
    log.warn("Pessimistic locking timeout on {}", request.getRequestURI());
    return buildError(HttpStatus.CONFLICT, "Conflict",
        "The request is already being processed. Please retry shortly.", request.getRequestURI());
  }

  // Safety-net: unique constraint violation on idempotency_key.key - advisory lock should
  // prevent this in normal operation, but a fallback 409 is far better than a 500.
  @ExceptionHandler(DataIntegrityViolationException.class)
  @ResponseStatus(HttpStatus.CONFLICT)
  public ErrorResponse handleDataIntegrity(
      DataIntegrityViolationException ex, HttpServletRequest request) {
    log.warn("Data integrity violation on {} (possible concurrent duplicate key): {}",
        request.getRequestURI(), ex.getMessage());
    return buildError(HttpStatus.CONFLICT, "Conflict",
        "The request is already being processed. Please retry shortly.", request.getRequestURI());
  }

  // 409 Conflict - idempotency key in progress

  @ExceptionHandler(IdempotencyConflictException.class)
  @ResponseStatus(HttpStatus.CONFLICT)
  public ErrorResponse handleIdempotencyConflict(
      IdempotencyConflictException ex, HttpServletRequest request) {
    log.warn("Idempotency conflict on {}: {}", request.getRequestURI(), ex.getMessage());
    return buildError(HttpStatus.CONFLICT, "Conflict", ex.getMessage(), request.getRequestURI());
  }

  // 422 Unprocessable Entity - idempotency hash mismatch

  @ExceptionHandler(IdempotencyMismatchException.class)
  @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
  public ErrorResponse handleIdempotencyMismatch(
      IdempotencyMismatchException ex, HttpServletRequest request) {
    log.warn("Idempotency mismatch on {}: {}", request.getRequestURI(), ex.getMessage());
    return buildError(HttpStatus.UNPROCESSABLE_ENTITY, "Unprocessable Entity",
        ex.getMessage(), request.getRequestURI());
  }

  // 500 Internal Server Error - outbox serialisation

  @ExceptionHandler(OutboxSerializationException.class)
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public ErrorResponse handleOutboxSerialization(
      OutboxSerializationException ex, HttpServletRequest request) {
    log.error("Outbox serialisation failure on {}: {}",
              request.getRequestURI(), ex.getMessage(), ex);
    return buildError(HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                      "An unexpected error occurred.", request.getRequestURI());
  }

  // 500 Internal Server Error - idempotency serialisation (Tasks 5.3-5.5)

  @ExceptionHandler(IdempotencySerializationException.class)
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public ErrorResponse handleIdempotencySerialization(
      IdempotencySerializationException ex, HttpServletRequest request) {
    log.error("Idempotency serialisation failure on {}: {}",
              request.getRequestURI(), ex.getMessage(), ex);
    return buildError(HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                      "An unexpected error occurred.", request.getRequestURI());
  }

  // 500 Internal Server Error (fallback)

  @ExceptionHandler(Exception.class)
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public ErrorResponse handleGeneric(Exception ex, HttpServletRequest request) {
    // Task 4.5 - log debugId explicitly so it appears even without MDC pattern formatting
    log.error("Unexpected error [debugId={}] on {}", MDC.get("debugId"),
              request.getRequestURI(), ex);
    return buildError(HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
        "An unexpected error occurred.", request.getRequestURI());
  }

  // helpers

  /**
   * Builds a clean {@link ErrorResponse} for the caller.
   *
   * <p>Tasks 4.1-4.3: reads debugId from MDC; generates a fallback UUID if absent
   * (filter chain was bypassed). The debugId is used only to ensure log lines always carry
   * a correlation token.
   *
   * <p>Task 4.4 intentionally NOT implemented: the requirements prohibit debugId from
   * appearing in any response body. Callers access the correlation token exclusively via
   * the {@code X-Debug-Id} response header written by MdcFilter.
   */
  private ErrorResponse buildError(
      HttpStatus status, String error, String message, String path) {

    // Tasks 4.2 and 4.3 - ensure MDC always has a debugId for log correlation
    String debugId = MDC.get("debugId");
    if (debugId == null) {
      // Fallback: MdcFilter did not run (e.g. request was short-circuited before chain)
      debugId = UUID.randomUUID().toString();
      MDC.put("debugId", debugId);
    }

    // debugId is used only for log correlation above - never set on the response object.
    // "debugId" must never appear in any REST response body per project requirements.
    ErrorResponse response = new ErrorResponse();
    response.setTimestamp(OffsetDateTime.now());
    response.setStatus(status.value());
    response.setError(error);
    response.setMessage(message);
    response.setPath(path);
    return response;
  }
}
