package pl.ldz.microsrv.order.exception;

import java.util.UUID;

/**
 * Thrown when an {@code Order} with the given id cannot be found or has been soft-deleted.
 * Maps to HTTP 404 via {@link GlobalExceptionHandler}.
 */
public class OrderNotFoundException extends RuntimeException {

  public OrderNotFoundException(UUID id) {
    super("Order not found: " + id);
  }
}
