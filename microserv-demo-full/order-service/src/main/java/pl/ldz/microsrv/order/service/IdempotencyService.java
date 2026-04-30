package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pl.ldz.microsrv.order.entity.IdempotencyKey;
import pl.ldz.microsrv.order.entity.IdempotencyStatus;
import pl.ldz.microsrv.order.exception.IdempotencyConflictException;
import pl.ldz.microsrv.order.exception.IdempotencyMismatchException;
import pl.ldz.microsrv.order.exception.IdempotencySerializationException;
import pl.ldz.microsrv.order.repository.IdempotencyKeyRepository;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.Optional;

/**
 * Owns all reads and writes to {@link IdempotencyKey}.
 *
 * <h3>Transaction contract</h3>
 * <p>Both public methods ({@link #checkAndStore} and {@link #markCompleted}) are annotated
 * {@code @Transactional(propagation = MANDATORY)}: they <em>must</em> be called from within
 * an active transaction owned by the caller (typically {@code OrderService}).  If either
 * method is ever invoked without an enclosing transaction Spring will throw
 * {@link org.springframework.transaction.IllegalTransactionStateException} immediately,
 * making the mis-use visible at development time rather than silently writing outside a
 * unit-of-work.
 *
 * <h3>Concurrent-insert safety — advisory locks</h3>
 * <p>{@link #checkAndStore} acquires a PostgreSQL transaction-scoped advisory lock
 * ({@code pg_try_advisory_xact_lock}) keyed on a 64-bit hash of the idempotency key
 * string <em>before</em> reading the row.  This serialises concurrent first-time inserts:
 * a {@code SELECT … FOR UPDATE} row-level lock cannot protect a row that does not yet
 * exist, so two simultaneous requests would both see {@code Optional.empty()} and race
 * to insert, producing a unique-constraint violation (HTTP 500).  The advisory lock
 * prevents this: only one thread acquires it and proceeds; the other receives
 * {@link IdempotencyConflictException} → HTTP 409 immediately.  The lock is released
 * automatically at transaction commit/rollback.
 *
 * <h4>Crash-recovery behaviour</h4>
 * <ul>
 *   <li>If the outer transaction rolls back <em>after</em> {@link #checkAndStore} inserted
 *       the {@code IN_PROGRESS} row, that row is rolled back too — the key remains absent
 *       and the next retry starts fresh. ✔</li>
 *   <li>If the outer transaction rolls back <em>after</em> {@link #markCompleted} wrote the
 *       {@code COMPLETED} record, that record is also rolled back — the key stays
 *       {@code IN_PROGRESS} and will be removed by the TTL cleanup job. ✔</li>
 * </ul>
 *
 * <p>Never references {@code OrderRepository}, {@code OutboxEventRepository}, or any Kafka class.
 * No {@code debugId} is present anywhere in this class.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

  private final IdempotencyKeyRepository idempotencyKeyRepository;
  private final ObjectMapper objectMapper;

  @Value("${idempotency.ttl-hours:24}")
  long ttlHours;

  /**
   * Per-thread SHA-256 digest instance. {@link MessageDigest} is not thread-safe,
   * so we keep one per thread and call {@link MessageDigest#reset()} before each use
   * instead of allocating a new instance on every request.
   */
  private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 unavailable", e);
    }
  });

  // ── Public API ────────────────────────────────────────────────────────────

  /**
   * Main idempotency gate.
   *
   * <p><strong>Must be called within an active transaction</strong>
   * ({@code propagation = MANDATORY}).
   *
   * <p>Acquires a PostgreSQL advisory lock on the idempotency key before reading the row,
   * so concurrent first-time requests are serialised at the database level rather than
   * racing to insert and hitting the unique constraint.
   *
   * <ul>
   *   <li>Lock not granted → another thread is processing the same key → throw
   *       {@link IdempotencyConflictException} (HTTP 409).</li>
   *   <li>Key not found → insert {@code IN_PROGRESS} row, return {@link Optional#empty()}.</li>
   *   <li>Key found, hash mismatch → throw {@link IdempotencyMismatchException} (HTTP 422).</li>
   *   <li>Key found, {@code COMPLETED} → return stored response body (empty string for null
   *       bodies, e.g. DELETE replays where 204 has no body).</li>
   *   <li>Key found, {@code IN_PROGRESS} → throw {@link IdempotencyConflictException} (HTTP 409).</li>
   * </ul>
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public Optional<String> checkAndStore(String key, Object request) {
    // 1. Validate key before touching the repository — bad input is a 400, not a 500
    if (key == null || key.isBlank() || key.length() > 255) {
      throw new IllegalArgumentException(
          "Idempotency-Key must be between 1 and 255 characters");
    }

    // 2. Acquire a transaction-scoped advisory lock on a 64-bit hash of the key.
    //    This serialises concurrent first-time inserts: SELECT…FOR UPDATE cannot lock
    //    a row that doesn't exist yet, but the advisory lock queues threads before
    //    either read occurs, preventing the race that caused the unique-constraint 500.
    long advisoryLockKey = advisoryLockKeyFor(key);
    boolean lockAcquired = idempotencyKeyRepository.tryAdvisoryLock(advisoryLockKey);
    if (!lockAcquired) {
      log.warn("Idempotency advisory lock not acquired (key in progress): key={}", key);
      throw new IdempotencyConflictException(key);
    }

    // 3. Lock held — safe to read; no other transaction can insert the same key concurrently.
    String requestHash = computeHash(request);
    Optional<IdempotencyKey> existing = idempotencyKeyRepository.findByKey(key);

    if (existing.isEmpty()) {
      IdempotencyKey record = new IdempotencyKey();
      record.setId(Generators.timeBasedEpochGenerator().generate());
      record.setKey(key);
      record.setRequestHash(requestHash);
      record.setStatus(IdempotencyStatus.IN_PROGRESS);
      record.setCreatedAt(OffsetDateTime.now());
      record.setExpiresAt(OffsetDateTime.now().plus(ttlHours, ChronoUnit.HOURS));
      idempotencyKeyRepository.save(record);
      log.debug("Idempotency key stored as IN_PROGRESS: key={}", key);
      return Optional.empty();
    }

    IdempotencyKey stored = existing.get();

    // Hash mismatch takes priority over status check (per spec 6.3.7)
    if (!requestHash.equals(stored.getRequestHash())) {
      throw new IdempotencyMismatchException(key);
    }

    if (IdempotencyStatus.COMPLETED == stored.getStatus()) {
      log.debug("Idempotency cache hit (COMPLETED): key={}", key);
      // Null-safe: DELETE replays have no response body stored (204 → SQL NULL)
      String body = stored.getResponseBody();
      return Optional.of(body != null ? body : "");
    }

    // status == IN_PROGRESS (held by a prior request that has not yet completed)
    throw new IdempotencyConflictException(key);
  }

  /**
   * Seals the idempotency record after successful business logic.
   *
   * <p><strong>Must be called within an active transaction</strong>
   * ({@code propagation = MANDATORY}).
   *
   * @param key              the idempotency key to mark completed
   * @param responseBodyJson the serialised HTTP response body, or {@code null} for void
   *                         operations (e.g. DELETE / 204) where no body is returned.
   *                         When {@code null}, the {@code response_body} column is stored
   *                         as SQL {@code NULL}.
   * @throws IllegalStateException if the key is not found (programming error)
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public void markCompleted(String key, String responseBodyJson) {
    IdempotencyKey record = idempotencyKeyRepository.findByKey(key)
        .orElseThrow(() -> new IllegalStateException(
            "IdempotencyKey not found when marking completed: " + key));
    record.setStatus(IdempotencyStatus.COMPLETED);
    record.setResponseBody(responseBodyJson);
    idempotencyKeyRepository.save(record);
    log.info("Idempotency key marked COMPLETED: key={}", key);
  }

  // ── Private helpers ───────────────────────────────────────────────────────

  /**
   * Converts the idempotency key string to a stable 64-bit integer suitable for use
   * as a PostgreSQL advisory lock key.
   *
   * <p>Uses the first 8 bytes of the SHA-256 digest so that the distribution is uniform
   * and collisions are negligible in practice (collision probability ≈ 2⁻⁶³ per pair).
   * The digest is computed from the raw UTF-8 bytes of the key string.
   */
  static long advisoryLockKeyFor(String key) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      // Assemble first 8 bytes into a long (big-endian)
      long result = 0;
      for (int i = 0; i < 8; i++) {
        result = (result << 8) | (hash[i] & 0xFF);
      }
      return result;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 unavailable", e);
    }
  }

  /**
   * Serialises {@code request} to JSON and returns its SHA-256 digest as a
   * lowercase 64-character hex string. Reuses a per-thread {@link MessageDigest}
   * instance via {@link #SHA256} to avoid per-call allocation overhead.
   */
  private String computeHash(Object request) {
    try {
      byte[] json = objectMapper.writeValueAsBytes(request);
      MessageDigest digest = SHA256.get();
      digest.reset();
      byte[] hash = digest.digest(json);
      return HexFormat.of().formatHex(hash);
    } catch (JsonProcessingException e) {
      throw new IdempotencySerializationException("Failed to serialise request for hashing", e);
    }
  }
}
