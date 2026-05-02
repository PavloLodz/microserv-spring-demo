package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IdempotencyService_ConflictTest {

  @Mock
  private IdempotencyKeyRepository idempotencyKeyRepository;

  @Mock
  private ObjectMapper objectMapper;

  @InjectMocks
  private IdempotencyService idempotencyService;

  private static final String KEY = UUID.randomUUID().toString();
  private static final String REQUEST_OBJ = "{\"customerId\":\"abc\",\"totalAmount\":\"99.99\"}";
  private static final String COMPUTED_HASH = computeSha256("mock-bytes");

  @BeforeEach
  void setTtlHours() throws Exception {
    Field ttl = IdempotencyService.class.getDeclaredField("ttlHours");
    ttl.setAccessible(true);
    ttl.set(idempotencyService, 24L);
  }

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
}
