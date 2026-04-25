package pl.ldz.microsrv.order.exception;

public class IdempotencyMismatchException extends RuntimeException {

    public IdempotencyMismatchException(String key) {
        super("Request body does not match the original request for idempotency key: " + key);
    }
}
