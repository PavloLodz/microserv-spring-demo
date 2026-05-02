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
import pl.ldz.microsrv.order.repository.IdempotencyKeyRepository;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IdempotencyService_LockTest {

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
}
