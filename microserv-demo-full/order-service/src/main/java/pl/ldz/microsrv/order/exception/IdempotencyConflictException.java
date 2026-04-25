package pl.ldz.microsrv.order.exception;

public class IdempotencyConflictException extends RuntimeException {

  public IdempotencyConflictException(String key) {
    super("Idempotency key already in progress: " + key);
  }
}
