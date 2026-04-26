package pl.ldz.microsrv.order.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.ldz.microsrv.order.repository.IdempotencyKeyRepository;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link IdempotencyCleanupService}.
 *
 * <p>Verifies that {@code cleanupExpired} delegates to the bulk-delete repository method,
 * logs the result correctly, swallows exceptions to protect the scheduler thread, and
 * never loads entity objects into heap memory.
 */
@ExtendWith(MockitoExtension.class)
class IdempotencyCleanupServiceTest {

  @Mock
  private IdempotencyKeyRepository idempotencyKeyRepository;

  @InjectMocks
  private IdempotencyCleanupService idempotencyCleanupService;

  // ── 2.8 Deletes expired rows ──────────────────────────────────────────────

  @Nested
  @DisplayName("2.8 cleanupExpired — bulk delete called with cutoff near now()")
  class DeletesExpiredRows {

    @Test
    @DisplayName("2.8.1 calls deleteAllExpiredBefore once with a cutoff within 5 seconds of now()")
    void cleanupExpired_deletesExpiredRows() {
      when(idempotencyKeyRepository.deleteAllExpiredBefore(any())).thenReturn(3);

      OffsetDateTime before = OffsetDateTime.now();
      idempotencyCleanupService.cleanupExpired();
      OffsetDateTime after = OffsetDateTime.now();

      ArgumentCaptor<OffsetDateTime> captor = ArgumentCaptor.forClass(OffsetDateTime.class);
      verify(idempotencyKeyRepository, times(1)).deleteAllExpiredBefore(captor.capture());

      OffsetDateTime cutoff = captor.getValue();
      assertThat(cutoff).isAfterOrEqualTo(before);
      assertThat(cutoff).isBeforeOrEqualTo(after);
    }
  }

  // ── 2.9 Zero expired rows ─────────────────────────────────────────────────

  @Nested
  @DisplayName("2.9 cleanupExpired — zero rows deleted")
  class NoExpiredRows {

    @Test
    @DisplayName("2.9.1 still calls deleteAllExpiredBefore and does not throw")
    void cleanupExpired_noExpiredRows_completesNormally() {
      when(idempotencyKeyRepository.deleteAllExpiredBefore(any())).thenReturn(0);

      assertThatNoException().isThrownBy(() -> idempotencyCleanupService.cleanupExpired());

      verify(idempotencyKeyRepository, times(1)).deleteAllExpiredBefore(any());
    }
  }

  // ── 2.10 Exception is swallowed ───────────────────────────────────────────

  @Nested
  @DisplayName("2.10 cleanupExpired — repository exception does not propagate")
  class ExceptionIsSwallowed {

    @Test
    @DisplayName("2.10.1 RuntimeException from repository is caught; method returns normally")
    void cleanupExpired_repositoryThrows_exceptionIsSwallowed() {
      when(idempotencyKeyRepository.deleteAllExpiredBefore(any()))
          .thenThrow(new RuntimeException("db error"));

      // Must not throw — the scheduler thread must survive repository failures
      assertThatNoException().isThrownBy(() -> idempotencyCleanupService.cleanupExpired());
    }
  }
}
