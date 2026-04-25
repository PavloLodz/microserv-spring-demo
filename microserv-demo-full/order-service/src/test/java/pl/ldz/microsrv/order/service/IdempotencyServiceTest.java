package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.ldz.microsrv.order.entity.IdempotencyKey;
import pl.ldz.microsrv.order.exception.IdempotencyConflictException;
import pl.ldz.microsrv.order.exception.IdempotencyMismatchException;
import pl.ldz.microsrv.order.repository.IdempotencyKeyRepository;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

  @Mock
  private IdempotencyKeyRepository idempotencyKeyRepository;

  @Mock
  private ObjectMapper objectMapper;

  @InjectMocks
  private IdempotencyService idempotencyService;

  private static final String KEY = UUID.randomUUID().toString();
  private static final String REQUEST_OBJ = "{\"customerId\":\"abc\",\"totalAmount\":\"99.99\"}";
  // SHA-256 of the byte representation returned by our mock objectMapper
  private static final String COMPUTED_HASH = computeSha256("mock-bytes");

  /**
     * Set ttlHours=24 via reflection — @Value is not processed by MockitoExtension,
     * so the field defaults to 0 after @InjectMocks construction.
     */
  @BeforeEach
  void setTtlHours() throws Exception {
    Field ttl = IdempotencyService.class.getDeclaredField("ttlHours");
    ttl.setAccessible(true);
    ttl.set(idempotencyService, 24L);
  }

  /** Stub objectMapper only in tests that invoke computeHash (i.e. checkAndStore). */
  private void stubObjectMapper() throws Exception {
    when(objectMapper.writeValueAsBytes(any())).thenReturn("mock-bytes".getBytes());
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private static String computeSha256(String input) {
    try {
      var digest = java.security.MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(input.getBytes());
      return java.util.HexFormat.of().formatHex(hash);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private IdempotencyKey buildKey(String status, String requestHash) {
    IdempotencyKey k = new IdempotencyKey();
    k.setId(UUID.randomUUID());
    k.setKey(KEY);
    k.setStatus(status);
    k.setRequestHash(requestHash);
    k.setCreatedAt(OffsetDateTime.now());
    k.setExpiresAt(OffsetDateTime.now().plusHours(24));
    return k;
  }

  // ── 14.2 Cache Miss ───────────────────────────────────────────────────────

  @Nested
  @DisplayName("14.2 Cache miss — new key inserted")
  class CacheMiss {

    @Test
    @DisplayName("14.2.1-3 returns empty Optional and saves IN_PROGRESS row")
    void checkAndStore_cacheMiss_insertsInProgressRowAndReturnsEmpty() throws Exception {
      stubObjectMapper();
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.empty());

      // Capture 'now' before calling the service so the assertion doesn't race
      OffsetDateTime before = OffsetDateTime.now();

      Optional<String> result = idempotencyService.checkAndStore(KEY, REQUEST_OBJ);

      assertThat(result).isEmpty();

      ArgumentCaptor<IdempotencyKey> captor = ArgumentCaptor.forClass(IdempotencyKey.class);
      verify(idempotencyKeyRepository).save(captor.capture());

      IdempotencyKey saved = captor.getValue();
      assertThat(saved.getStatus()).isEqualTo("IN_PROGRESS");
      assertThat(saved.getKey()).isEqualTo(KEY);
      assertThat(saved.getExpiresAt()).isAfter(before);   // expiresAt = now+24h > before
      assertThat(saved.getId()).isNotNull();
    }
  }

  // ── 14.7 TTL ─────────────────────────────────────────────────────────────

  @Nested
  @DisplayName("14.7 TTL default")
  class TtlDefault {

    @Test
    @DisplayName("14.7.1 expiresAt is approximately now + 24h")
    void checkAndStore_cacheMiss_expiresAtIsApproximatelyNowPlusTtl() throws Exception {
      stubObjectMapper();
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.empty());

      // Bracket the call with before/after so we have a valid time window
      OffsetDateTime before = OffsetDateTime.now();
      idempotencyService.checkAndStore(KEY, REQUEST_OBJ);
      OffsetDateTime after = OffsetDateTime.now();

      ArgumentCaptor<IdempotencyKey> captor = ArgumentCaptor.forClass(IdempotencyKey.class);
      verify(idempotencyKeyRepository).save(captor.capture());

      OffsetDateTime expiresAt = captor.getValue().getExpiresAt();
      assertThat(expiresAt).isAfter(before.plusHours(23));
      assertThat(expiresAt).isBefore(after.plusHours(25));
    }
  }

  // ── 14.3 Cache Hit — COMPLETED ────────────────────────────────────────────

  @Nested
  @DisplayName("14.3 Cache hit — completed key")
  class CacheHitCompleted {

    @Test
    @DisplayName("14.3.1-3 returns stored response body, never calls save")
    void checkAndStore_cacheHitCompleted_returnsStoredResponseBody() throws Exception {
      stubObjectMapper();
      String storedBody = "{\"id\":\"" + UUID.randomUUID() + "\"}";
      IdempotencyKey existing = buildKey("COMPLETED", COMPUTED_HASH);
      existing.setResponseBody(storedBody);
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.of(existing));

      Optional<String> result = idempotencyService.checkAndStore(KEY, REQUEST_OBJ);

      assertThat(result).isPresent().hasValue(storedBody);
      verify(idempotencyKeyRepository, never()).save(any());
    }
  }

  // ── 14.4 Cache Hit — IN_PROGRESS ─────────────────────────────────────────

  @Nested
  @DisplayName("14.4 Cache hit — in-progress key")
  class CacheHitInProgress {

    @Test
    @DisplayName("14.4.1-3 throws IdempotencyConflictException, never calls save")
    void checkAndStore_cacheHitInProgress_throwsIdempotencyConflictException() throws Exception {
      stubObjectMapper();
      IdempotencyKey existing = buildKey("IN_PROGRESS", COMPUTED_HASH);
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.of(existing));

      assertThatThrownBy(() -> idempotencyService.checkAndStore(KEY, REQUEST_OBJ))
          .isInstanceOf(IdempotencyConflictException.class);

      verify(idempotencyKeyRepository, never()).save(any());
    }
  }

  // ── 14.5 Hash Mismatch ────────────────────────────────────────────────────

  @Nested
  @DisplayName("14.5 Hash mismatch")
  class HashMismatch {

    @Test
    @DisplayName("14.5.1-2 throws IdempotencyMismatchException when stored hash differs")
    void checkAndStore_hashMismatch_throwsIdempotencyMismatchException() throws Exception {
      stubObjectMapper();
      // stored hash is different from the hash computed from REQUEST_OBJ
      IdempotencyKey existing = buildKey("COMPLETED", "0000000000000000000000000000000000000000000000000000000000000000");
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.of(existing));

      assertThatThrownBy(() -> idempotencyService.checkAndStore(KEY, REQUEST_OBJ))
          .isInstanceOf(IdempotencyMismatchException.class);
    }
  }

  // ── 14.6 markCompleted ────────────────────────────────────────────────────

  @Nested
  @DisplayName("14.6 markCompleted updates status and body")
  class MarkCompleted {

    @Test
    @DisplayName("14.6.1-3 sets status=COMPLETED and responseBody on saved entity")
    void markCompleted_updatesStatusAndResponseBody() {
      // markCompleted does NOT call objectMapper — no stub needed here
      IdempotencyKey existing = buildKey("IN_PROGRESS", COMPUTED_HASH);
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.of(existing));

      String responseBody = "{\"id\":\"" + UUID.randomUUID() + "\"}";
      idempotencyService.markCompleted(KEY, responseBody);

      ArgumentCaptor<IdempotencyKey> captor = ArgumentCaptor.forClass(IdempotencyKey.class);
      verify(idempotencyKeyRepository).save(captor.capture());

      IdempotencyKey saved = captor.getValue();
      assertThat(saved.getStatus()).isEqualTo("COMPLETED");
      assertThat(saved.getResponseBody()).isEqualTo(responseBody);
    }
  }
}
