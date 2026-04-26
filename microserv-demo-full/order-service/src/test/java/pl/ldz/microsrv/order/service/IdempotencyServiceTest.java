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
import pl.ldz.microsrv.order.entity.IdempotencyStatus;
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

  /**
   * Stub objectMapper for tests that invoke computeHash (i.e. checkAndStore after lock
   * is granted).  Also stubs the advisory lock to return true (lock granted) so
   * checkAndStore proceeds past the concurrency gate.
   */
  private void stubForCheckAndStore() throws Exception {
    when(objectMapper.writeValueAsBytes(any())).thenReturn("mock-bytes".getBytes());
    when(idempotencyKeyRepository.tryAdvisoryLock(anyLong())).thenReturn(true);
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

  private IdempotencyKey buildKey(IdempotencyStatus status, String requestHash) {
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
      stubForCheckAndStore();
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.empty());

      OffsetDateTime before = OffsetDateTime.now();

      Optional<String> result = idempotencyService.checkAndStore(KEY, REQUEST_OBJ);

      assertThat(result).isEmpty();

      ArgumentCaptor<IdempotencyKey> captor = ArgumentCaptor.forClass(IdempotencyKey.class);
      verify(idempotencyKeyRepository).save(captor.capture());

      IdempotencyKey saved = captor.getValue();
      assertThat(saved.getStatus()).isEqualTo(IdempotencyStatus.IN_PROGRESS);
      assertThat(saved.getKey()).isEqualTo(KEY);
      assertThat(saved.getExpiresAt()).isAfter(before);
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
      stubForCheckAndStore();
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.empty());

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
      stubForCheckAndStore();
      String storedBody = "{\"id\":\"" + UUID.randomUUID() + "\"}";
      IdempotencyKey existing = buildKey(IdempotencyStatus.COMPLETED, COMPUTED_HASH);
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
      stubForCheckAndStore();
      IdempotencyKey existing = buildKey(IdempotencyStatus.IN_PROGRESS, COMPUTED_HASH);
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
      stubForCheckAndStore();
      // stored hash is different from the hash computed from REQUEST_OBJ
      IdempotencyKey existing = buildKey(IdempotencyStatus.COMPLETED,
          "0000000000000000000000000000000000000000000000000000000000000000");
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
      // markCompleted uses findByKey (plain, non-locking) — no advisory lock involved
      IdempotencyKey existing = buildKey(IdempotencyStatus.IN_PROGRESS, COMPUTED_HASH);
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.of(existing));

      String responseBody = "{\"id\":\"" + UUID.randomUUID() + "\"}";
      idempotencyService.markCompleted(KEY, responseBody);

      ArgumentCaptor<IdempotencyKey> captor = ArgumentCaptor.forClass(IdempotencyKey.class);
      verify(idempotencyKeyRepository).save(captor.capture());

      IdempotencyKey saved = captor.getValue();
      assertThat(saved.getStatus()).isEqualTo(IdempotencyStatus.COMPLETED);
      assertThat(saved.getResponseBody()).isEqualTo(responseBody);
    }
  }

  // ── 4.4 Null-body DELETE replay ──────────────────────────────────────────

  @Nested
  @DisplayName("4.4 checkAndStore — COMPLETED key with null responseBody returns empty string")
  class NullBodyDeleteReplay {

    @Test
    @DisplayName("4.4.1 returns Optional.of(\"\") when stored responseBody is null (DELETE replay)")
    void checkAndStore_completedNullBody_returnsEmptyString() throws Exception {
      stubForCheckAndStore();
      IdempotencyKey existing = buildKey(IdempotencyStatus.COMPLETED, COMPUTED_HASH);
      existing.setResponseBody(null); // simulates a DELETE whose 204 has no body
      when(idempotencyKeyRepository.findByKey(KEY)).thenReturn(Optional.of(existing));

      Optional<String> result = idempotencyService.checkAndStore(KEY, REQUEST_OBJ);

      assertThat(result).isPresent();
      assertThat(result.get()).isEqualTo("");
      verify(idempotencyKeyRepository, never()).save(any());
    }
  }

  // ── Advisory lock — concurrent request ───────────────────────────────────

  @Nested
  @DisplayName("Advisory lock — concurrent request is rejected immediately")
  class AdvisoryLockConflict {

    @Test
    @DisplayName("lock not acquired → throws IdempotencyConflictException before any repo read")
    void checkAndStore_lockNotAcquired_throwsConflict() {
      when(idempotencyKeyRepository.tryAdvisoryLock(anyLong())).thenReturn(false);

      assertThatThrownBy(() -> idempotencyService.checkAndStore(KEY, REQUEST_OBJ))
          .isInstanceOf(IdempotencyConflictException.class);

      // No read, no write to idempotency rows should happen
      verify(idempotencyKeyRepository, never()).findByKey(any());
      verify(idempotencyKeyRepository, never()).save(any());
    }
  }

  // ── advisoryLockKeyFor stability ─────────────────────────────────────────

  @Nested
  @DisplayName("advisoryLockKeyFor — stable, deterministic, differs across keys")
  class AdvisoryLockKeyFor {

    @Test
    @DisplayName("same input produces same long every time")
    void advisoryLockKeyFor_sameInput_sameOutput() {
      long a = IdempotencyService.advisoryLockKeyFor("my-key");
      long b = IdempotencyService.advisoryLockKeyFor("my-key");
      assertThat(a).isEqualTo(b);
    }

    @Test
    @DisplayName("different inputs produce different longs")
    void advisoryLockKeyFor_differentInputs_differentOutputs() {
      long a = IdempotencyService.advisoryLockKeyFor("key-one");
      long b = IdempotencyService.advisoryLockKeyFor("key-two");
      assertThat(a).isNotEqualTo(b);
    }
  }

  // ── 6.8–6.10 Key validation ───────────────────────────────────────────────

  @Nested
  @DisplayName("6.x Key validation — illegal keys throw IllegalArgumentException before hitting DB")
  class KeyValidation {

    @Test
    @DisplayName("6.8 null key throws IllegalArgumentException")
    void checkAndStore_nullKey_throwsIllegalArgument() {
      assertThatThrownBy(() -> idempotencyService.checkAndStore(null, REQUEST_OBJ))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Idempotency-Key must be between 1 and 255 characters");
      // Guard fires before advisory lock or any repository call
      verify(idempotencyKeyRepository, never()).tryAdvisoryLock(anyLong());
      verify(idempotencyKeyRepository, never()).findByKey(any());
    }

    @Test
    @DisplayName("6.9 blank (whitespace-only) key throws IllegalArgumentException")
    void checkAndStore_blankKey_throwsIllegalArgument() {
      assertThatThrownBy(() -> idempotencyService.checkAndStore("   ", REQUEST_OBJ))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Idempotency-Key must be between 1 and 255 characters");
      verify(idempotencyKeyRepository, never()).tryAdvisoryLock(anyLong());
      verify(idempotencyKeyRepository, never()).findByKey(any());
    }

    @Test
    @DisplayName("6.10 256-character key throws IllegalArgumentException")
    void checkAndStore_oversizedKey_throwsIllegalArgument() {
      String longKey = "a".repeat(256);
      assertThatThrownBy(() -> idempotencyService.checkAndStore(longKey, REQUEST_OBJ))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Idempotency-Key must be between 1 and 255 characters");
      verify(idempotencyKeyRepository, never()).tryAdvisoryLock(anyLong());
      verify(idempotencyKeyRepository, never()).findByKey(any());
    }
  }

}
