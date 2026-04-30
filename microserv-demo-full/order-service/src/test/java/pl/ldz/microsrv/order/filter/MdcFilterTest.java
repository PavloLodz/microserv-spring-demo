package pl.ldz.microsrv.order.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link MdcFilter}.
 *
 * <p>Tasks 8.7–8.13: verifies that the filter correctly populates MDC with a UUID
 * {@code debugId} and the {@code idempotencyKey} header value, sets the
 * {@code X-Debug-Id} response header, and always clears MDC after the chain completes —
 * even when the chain throws an exception.
 *
 * <p>Uses Spring's {@link MockHttpServletRequest}/{@link MockHttpServletResponse} so no
 * container or Spring context is required.  The {@link FilterChain} is implemented as a
 * lambda that captures MDC state mid-chain so assertions can be made on what the service
 * layer would see during a real request.
 */
@ExtendWith(MockitoExtension.class)
class MdcFilterTest {

  private final MdcFilter filter = new MdcFilter();

  /** Guarantee MDC is clean between tests regardless of test outcome. */
  @AfterEach
  void cleanMdc() {
    MDC.clear();
  }

  // ── Task 8.8 / 8.9 ── debugId is a valid UUID during chain execution ──────

  /**
   * Task 8.8: Invokes the filter with a mock FilterChain that captures MDC state mid-chain.
   * Task 8.9: debugId in MDC is non-null and a valid UUID string during execution.
   */
  @Test
  @DisplayName("8.9 debugId in MDC is a non-null valid UUID during filter chain execution")
  void doFilterInternal_setsDebugId_validUuidDuringChain() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest();
    MockHttpServletResponse response = new MockHttpServletResponse();

    AtomicReference<String> capturedDebugId = new AtomicReference<>();

    FilterChain chain = (req, resp) -> capturedDebugId.set(MDC.get(MdcFilter.MDC_DEBUG_ID));

    filter.doFilterInternal(request, response, chain);

    assertThat(capturedDebugId.get())
        .as("debugId must be non-null during filter chain execution")
        .isNotNull()
        .satisfies(id -> {
          // Must be parseable as a UUID
          UUID parsed = UUID.fromString(id);
          assertThat(parsed).isNotNull();
        });
  }

  // ── Task 8.10 ── idempotencyKey is present (empty string) when header absent ─

  /**
   * Task 8.10: idempotencyKey is present in MDC as empty string when the header is absent.
   */
  @Test
  @DisplayName("8.10 idempotencyKey MDC field is empty string when Idempotency-Key header is absent")
  void doFilterInternal_noIdempotencyKeyHeader_setsEmptyStringInMdc() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest(); // no Idempotency-Key header
    MockHttpServletResponse response = new MockHttpServletResponse();

    AtomicReference<String> capturedKey = new AtomicReference<>();
    FilterChain chain = (req, resp) -> capturedKey.set(MDC.get(MdcFilter.MDC_IDEMPOTENCY_KEY));

    filter.doFilterInternal(request, response, chain);

    assertThat(capturedKey.get())
        .as("idempotencyKey MDC field must be present (empty string) when header absent")
        .isNotNull()
        .isEmpty();
  }

  // ── Task 8.11 ── MDC is fully cleared after filter completes ─────────────

  /**
   * Task 8.11: MDC context map is null or empty after the filter completes normally.
   */
  @Test
  @DisplayName("8.11 MDC is fully cleared after filter chain completes")
  void doFilterInternal_clearsAllMdcAfterChain() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest();
    MockHttpServletResponse response = new MockHttpServletResponse();

    FilterChain chain = (req, resp) -> {
      // No-op: just verify the filter clears up after us
    };

    filter.doFilterInternal(request, response, chain);

    Map<String, String> contextAfter = MDC.getCopyOfContextMap();
    assertThat(contextAfter)
        .as("MDC must be fully cleared after filter completes")
        .isNullOrEmpty();
  }

  /**
   * Task 8.11 (exception path): MDC is still cleared even when the filter chain throws.
   */
  @Test
  @DisplayName("8.11 MDC is cleared even when the filter chain throws ServletException")
  void doFilterInternal_clearsAllMdcEvenWhenChainThrows() {
    MockHttpServletRequest request = new MockHttpServletRequest();
    MockHttpServletResponse response = new MockHttpServletResponse();

    FilterChain chain = (req, resp) -> {
      throw new ServletException("simulated downstream failure");
    };

    assertThatThrownBy(() -> filter.doFilterInternal(request, response, chain))
        .isInstanceOf(ServletException.class);

    Map<String, String> contextAfter = MDC.getCopyOfContextMap();
    assertThat(contextAfter)
        .as("MDC must be cleared even when filter chain throws")
        .isNullOrEmpty();
  }

  // ── Task 8.12 ── X-Debug-Id header matches MDC debugId during execution ──

  /**
   * Task 8.12: The X-Debug-Id response header is set to the same value as the debugId
   * that was in MDC during chain execution.
   */
  @Test
  @DisplayName("8.12 X-Debug-Id response header matches MDC debugId during chain execution")
  void doFilterInternal_xDebugIdHeaderMatchesMdcDebugId() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest();
    MockHttpServletResponse response = new MockHttpServletResponse();

    AtomicReference<String> capturedMdcDebugId = new AtomicReference<>();
    FilterChain chain = (req, resp) -> capturedMdcDebugId.set(MDC.get(MdcFilter.MDC_DEBUG_ID));

    filter.doFilterInternal(request, response, chain);

    String headerValue = response.getHeader(MdcFilter.HEADER_DEBUG_ID);

    assertThat(headerValue)
        .as("X-Debug-Id response header must be non-blank")
        .isNotBlank();

    assertThat(headerValue)
        .as("X-Debug-Id header must match the debugId that was in MDC during chain execution")
        .isEqualTo(capturedMdcDebugId.get());
  }

  /**
   * Task 8.12 (supplementary): X-Debug-Id is set on the response even when the chain
   * throws an exception (header is written before doFilter is called).
   */
  @Test
  @DisplayName("8.12 X-Debug-Id header is present even when filter chain throws")
  void doFilterInternal_xDebugIdHeaderPresentEvenWhenChainThrows() {
    MockHttpServletRequest request = new MockHttpServletRequest();
    MockHttpServletResponse response = new MockHttpServletResponse();

    FilterChain chain = (req, resp) -> {
      throw new IOException("simulated IO failure");
    };

    assertThatThrownBy(() -> filter.doFilterInternal(request, response, chain))
        .isInstanceOf(IOException.class);

    assertThat(response.getHeader(MdcFilter.HEADER_DEBUG_ID))
        .as("X-Debug-Id header must be set before chain is called, so present even on exception")
        .isNotBlank();
  }

  // ── Task 8.13 ── Idempotency-Key header value is captured in MDC ─────────

  /**
   * Task 8.13: When the Idempotency-Key header is present, its value appears verbatim
   * in the MDC under the {@code idempotencyKey} key during filter chain execution.
   */
  @Test
  @DisplayName("8.13 idempotencyKey MDC field contains the Idempotency-Key header value when present")
  void doFilterInternal_withIdempotencyKeyHeader_setsMdcValue() throws Exception {
    String expectedKey = UUID.randomUUID().toString();

    MockHttpServletRequest request = new MockHttpServletRequest();
    request.addHeader(MdcFilter.HEADER_IDEMPOTENCY_KEY, expectedKey);
    MockHttpServletResponse response = new MockHttpServletResponse();

    AtomicReference<String> capturedMdcKey = new AtomicReference<>();
    FilterChain chain = (req, resp) -> capturedMdcKey.set(MDC.get(MdcFilter.MDC_IDEMPOTENCY_KEY));

    filter.doFilterInternal(request, response, chain);

    assertThat(capturedMdcKey.get())
        .as("idempotencyKey MDC field must equal the Idempotency-Key request header")
        .isEqualTo(expectedKey);
  }

  /**
   * Task 8.13 (supplementary): each invocation generates a distinct debugId, confirming
   * UUID is freshly generated per-request and never reused.
   */
  @Test
  @DisplayName("8.13 each request receives a distinct debugId (not shared across requests)")
  void doFilterInternal_eachCallGeneratesDistinctDebugId() throws Exception {
    AtomicReference<String> id1 = new AtomicReference<>();
    AtomicReference<String> id2 = new AtomicReference<>();

    filter.doFilterInternal(
        new MockHttpServletRequest(), new MockHttpServletResponse(),
        (req, resp) -> id1.set(MDC.get(MdcFilter.MDC_DEBUG_ID)));

    filter.doFilterInternal(
        new MockHttpServletRequest(), new MockHttpServletResponse(),
        (req, resp) -> id2.set(MDC.get(MdcFilter.MDC_DEBUG_ID)));

    assertThat(id1.get()).isNotNull();
    assertThat(id2.get()).isNotNull();
    assertThat(id1.get())
        .as("Each request must receive a unique debugId")
        .isNotEqualTo(id2.get());
  }
}
