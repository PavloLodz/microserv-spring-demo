package pl.ldz.microsrv.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import pl.ldz.microsrv.order.entity.IdempotencyKey;
import pl.ldz.microsrv.order.repository.IdempotencyKeyRepository;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Scheduled cleanup service that removes expired {@link IdempotencyKey} records.
 *
 * <p>Runs on a configurable cron schedule (default: every hour at the top of the hour).
 * Deletes all rows where {@code expiresAt} is before the current time in a single
 * {@code deleteAll} call to avoid N+1 deletes.
 *
 * <p>Any exception is caught and logged at ERROR so the scheduler thread is never killed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyCleanupService {

    private final IdempotencyKeyRepository idempotencyKeyRepository;

    /**
     * Deletes all expired idempotency key records.
     *
     * <p>Runs on the cron schedule defined by {@code idempotency.cleanup-cron}
     * (default: {@code "0 0 * * * *"} — top of every hour).
     */
    @Scheduled(cron = "${idempotency.cleanup-cron:0 0 * * * *}")
    public void cleanupExpired() {
        try {
            List<IdempotencyKey> expired = idempotencyKeyRepository.findByExpiresAtBefore(OffsetDateTime.now());
            int count = expired.size();
            if (count > 0) {
                idempotencyKeyRepository.deleteAll(expired);
            }
            log.info("Deleted {} expired idempotency keys", count);
        } catch (Exception e) {
            log.error("Unexpected error during idempotency key cleanup", e);
        }
    }
}
