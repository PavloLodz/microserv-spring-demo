package pl.ldz.microsrv.order.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import pl.ldz.microsrv.order.entity.IdempotencyKey;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

public interface IdempotencyKeyRepository extends JpaRepository<IdempotencyKey, UUID> {

  /**
   * Plain (non-locking) lookup used by {@code checkAndStore} (after the advisory lock is
   * held) and by {@code markCompleted}.
   */
  Optional<IdempotencyKey> findByKey(String key);

  /**
   * Attempts to acquire a PostgreSQL transaction-scoped advisory lock whose key is the
   * given 64-bit integer (typically a hash of the idempotency key string).
   *
   * <p>{@code pg_try_advisory_xact_lock} is non-blocking: it returns {@code true} if the
   * lock was granted and {@code false} if another transaction already holds it. The lock is
   * automatically released at the end of the enclosing transaction — no explicit unlock is
   * needed.
   *
   * <p>Using an advisory lock rather than {@code SELECT … FOR UPDATE} on the idempotency
   * row correctly serialises <em>concurrent first-time inserts</em>: a row-level lock can
   * only guard a row that already exists, so two threads racing to insert the same key
   * would both see {@code Optional.empty()} and both proceed to insert, causing a unique
   * constraint violation. The advisory lock prevents this by queuing one thread before
   * either read occurs.
   *
   * @param lockKey a 64-bit integer that uniquely identifies the critical section
   * @return {@code true} if the lock was acquired; {@code false} if already held
   */
  @Query(value = "SELECT pg_try_advisory_xact_lock(:lockKey)", nativeQuery = true)
  boolean tryAdvisoryLock(@Param("lockKey") long lockKey);

  /**
   * Bulk-deletes all expired idempotency key records in a single SQL statement,
   * avoiding the heap cost of loading entity objects.
   *
   * @param cutoff rows with {@code expiresAt} strictly before this timestamp are deleted
   * @return the number of deleted rows
   */
  @Modifying
  @Transactional
  @Query("DELETE FROM IdempotencyKey k WHERE k.expiresAt < :cutoff")
  int deleteAllExpiredBefore(@Param("cutoff") OffsetDateTime cutoff);
}
