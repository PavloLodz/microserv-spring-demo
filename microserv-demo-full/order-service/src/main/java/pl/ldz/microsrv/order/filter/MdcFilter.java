package pl.ldz.microsrv.order.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Servlet filter that establishes a structured logging context (MDC) for every inbound
 * HTTP request and guarantees that context is fully cleared when the request completes.
 *
 * <h3>What it sets</h3>
 * <ul>
 *   <li>{@code debugId} — a freshly generated UUID v4, unique per request. Used to
 *       correlate log lines and the {@code X-Debug-Id} response header with the
 *       {@code debugId} field in {@link pl.ldz.microsrv.order.exception.GlobalExceptionHandler}
 *       error responses.</li>
 *   <li>{@code idempotencyKey} — the value of the {@code Idempotency-Key} request header,
 *       or an empty string when the header is absent (so the MDC key is always present
 *       and log patterns never emit {@code null}).</li>
 * </ul>
 *
 * <h3>What it does NOT set</h3>
 * <p>The {@code id} MDC field (the order's UUIDv7) is set by
 * {@link pl.ldz.microsrv.order.service.OrderService} after the entity is persisted,
 * because the identifier is not available at filter time.
 *
 * <h3>MDC lifecycle</h3>
 * <p>{@link MDC#clear()} is always called in the {@code finally} block so that thread-pool
 * threads, which are reused across requests, never carry stale context into the next request.
 *
 * <h3>Response header</h3>
 * <p>The {@code X-Debug-Id} header is written to the response <em>before</em> the filter
 * chain executes, allowing callers to read the correlation token from the header even when
 * the response body is empty (e.g. 204 No Content).
 *
 * <h3>Filter ordering</h3>
 * <p>{@code @Order(1)} ensures this filter runs before any other application-level filter
 * that emits log statements, so those log lines already carry the full MDC context.
 */
@Component
@Order(1)
public class MdcFilter extends OncePerRequestFilter {

    static final String MDC_DEBUG_ID       = "debugId";
    static final String MDC_IDEMPOTENCY_KEY = "idempotencyKey";
    static final String HEADER_DEBUG_ID    = "X-Debug-Id";
    static final String HEADER_IDEMPOTENCY_KEY = "Idempotency-Key";

    @Override
    protected void doFilterInternal(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain filterChain) throws ServletException, IOException {

        // Task 2.3 — generate a fresh debugId UUID for this request
        String debugId = UUID.randomUUID().toString();

        // Task 2.4 — read the Idempotency-Key header; fall back to empty string when absent
        String idempotencyKey = request.getHeader(HEADER_IDEMPOTENCY_KEY);
        if (idempotencyKey == null) {
            idempotencyKey = "";
        }

        // Task 2.5 — populate MDC
        MDC.put(MDC_DEBUG_ID, debugId);
        MDC.put(MDC_IDEMPOTENCY_KEY, idempotencyKey);

        // Task 2.6 — write the X-Debug-Id response header before the chain runs so it is
        //            present on all responses including 204 No Content
        response.setHeader(HEADER_DEBUG_ID, debugId);

        // Task 2.7 — always clear MDC; prevents context leakage across thread-pool reuse
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.clear();
        }

        // Task 2.9 — intentionally no MDC.put("id", ...) here; that is the service layer's job
    }
}
