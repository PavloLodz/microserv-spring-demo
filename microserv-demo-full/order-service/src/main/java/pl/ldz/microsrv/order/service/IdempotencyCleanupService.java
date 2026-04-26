package pl.ldz.microsrv.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import pl.ldz.microsrv.order.entity.IdempotencyKey;
import pl.ldz.microsrv.order.repository.IdempotencyKeyRepository;

import java.time.OffsetDateTime;

/**
 * Scheduled cleanup service that removes expired {@link IdempotencyKey} records.
 *
 * <p>Runs on a configurable cron schedule (default: every hour at the top of the hour).
 * Deletes all rows where {@code expiresAt} is before the current time using a single
 * bulk-delete JPQL query to avoid loading entity objects into heap memory.
 *
 * <p>Any exception is caught and logged at ERROR so the scheduler thread is never killed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyCleanupService {

  private final IdempotencyKeyRepository idempotencyKeyRepository;

  /**
   * Deletes all expired idempotency key records in a single bulk-delete statement.
   *
   * <p>Covers both {@code COMPLETED} rows past their TTL and stranded {@code IN_PROGRESS}
   * rows left by crashed requests — both are eligible for cleanup once {@code expiresAt}
   * has passed.
   *
   * <p>Runs on the cron schedule defined by {@code idempotency.cleanup-cron}
   * (default: {@code "0 0 * * * *"} — top of every hour).
   */
  @Scheduled(cron = "${idempotency.cleanup-cron:0 0 * * * *}")
  public void cleanupExpired() {
    try {
      int count = idempotencyKeyRepository.deleteAllExpiredBefore(OffsetDateTime.now());
      log.info("Deleted {} expired idempotency keys", count);
    } catch (Exception e) {
      log.error("Unexpected error during idempotency key cleanup", e);
    }
  }
}
