package pl.ldz.microsrv.order.exception;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import pl.ldz.microsrv.order.api.model.ErrorResponse;

import java.time.OffsetDateTime;
import java.util.stream.Collectors;

/**
 * Centralised exception-to-HTTP-response mapping.
 *
 * <p>All responses use the {@link ErrorResponse} schema from the OpenAPI spec.
 * Stack traces, {@code debugId}, and raw internal messages are never exposed.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

  // ── 404 Not Found ────────────────────────────────────────────────────────

  @ExceptionHandler(OrderNotFoundException.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public ErrorResponse handleOrderNotFound(
      OrderNotFoundException ex, HttpServletRequest request) {
    log.info("Order not found: {}", ex.getMessage());
    return buildError(HttpStatus.NOT_FOUND, "Not Found", ex.getMessage(), request.getRequestURI());
  }

  // ── 400 Bad Request ───────────────────────────────────────────────────────

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

  // ── 409 Conflict ──────────────────────────────────────────────────────────

  @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
  @ResponseStatus(HttpStatus.CONFLICT)
  public ErrorResponse handleOptimisticLock(
      ObjectOptimisticLockingFailureException ex, HttpServletRequest request) {
    log.warn("Optimistic locking conflict on {}", request.getRequestURI());
    return buildError(HttpStatus.CONFLICT, "Conflict",
        "The resource was modified concurrently. Please retry.", request.getRequestURI());
  }

  // ── 500 Internal Server Error (fallback) ─────────────────────────────────

  @ExceptionHandler(Exception.class)
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public ErrorResponse handleGeneric(Exception ex, HttpServletRequest request) {
    // Log the full exception internally; never send it to the caller.
    log.error("Unexpected error on {}", request.getRequestURI(), ex);
    return buildError(HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
        "An unexpected error occurred.", request.getRequestURI());
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private ErrorResponse buildError(
      HttpStatus status, String error, String message, String path) {
    ErrorResponse response = new ErrorResponse();
    response.setTimestamp(OffsetDateTime.now());
    response.setStatus(status.value());
    response.setError(error);
    response.setMessage(message);
    response.setPath(path);
    return response;
  }
}
