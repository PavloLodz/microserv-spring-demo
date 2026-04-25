package pl.ldz.microsrv.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import pl.ldz.microsrv.order.entity.IdempotencyKey;
import pl.ldz.microsrv.order.exception.IdempotencyConflictException;
import pl.ldz.microsrv.order.exception.IdempotencyMismatchException;
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
 * <p>Two public methods:
 * <ul>
 *   <li>{@link #checkAndStore} — gate called at the top of every write operation.
 *   <li>{@link #markCompleted} — called after successful business logic to seal the record.
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

  // ── Public API ────────────────────────────────────────────────────────────

  /**
     * Main idempotency gate.
     *
     * <ul>
     *   <li>Key not found → insert {@code IN_PROGRESS} row, return {@link Optional#empty()}.
     *   <li>Key found, hash mismatch → throw {@link IdempotencyMismatchException} (checked first).
     *   <li>Key found, {@code COMPLETED} → return stored response body.
     *   <li>Key found, {@code IN_PROGRESS} → throw {@link IdempotencyConflictException}.
     * </ul>
     */
  public Optional<String> checkAndStore(String key, Object request) {
    String requestHash = computeHash(request);

    Optional<IdempotencyKey> existing = idempotencyKeyRepository.findByKey(key);

    if (existing.isEmpty()) {
      IdempotencyKey record = new IdempotencyKey();
      record.setId(Generators.timeBasedEpochGenerator().generate());
      record.setKey(key);
      record.setRequestHash(requestHash);
      record.setStatus("IN_PROGRESS");
      record.setCreatedAt(OffsetDateTime.now());
      record.setExpiresAt(OffsetDateTime.now().plus(ttlHours, ChronoUnit.HOURS));
      idempotencyKeyRepository.save(record);
      log.info("Idempotency key stored as IN_PROGRESS: key={}", key);
      return Optional.empty();
    }

    IdempotencyKey stored = existing.get();

    // Hash mismatch takes priority over status check (per spec 6.3.7)
    if (!requestHash.equals(stored.getRequestHash())) {
      throw new IdempotencyMismatchException(key);
    }

    if ("COMPLETED".equals(stored.getStatus())) {
      log.debug("Idempotency cache hit (COMPLETED): key={}", key);
      return Optional.of(stored.getResponseBody());
    }

    // status == IN_PROGRESS
    throw new IdempotencyConflictException(key);
  }

  /**
     * Seals the idempotency record after successful business logic.
     *
     * @throws IllegalStateException if the key is not found (programming error)
     */
  public void markCompleted(String key, String responseBodyJson) {
    IdempotencyKey record = idempotencyKeyRepository.findByKey(key)
        .orElseThrow(() -> new IllegalStateException(
            "IdempotencyKey not found when marking completed: " + key));
    record.setStatus("COMPLETED");
    record.setResponseBody(responseBodyJson);
    idempotencyKeyRepository.save(record);
    log.info("Idempotency key marked COMPLETED: key={}", key);
  }

  // ── Private helpers ───────────────────────────────────────────────────────

  /**
     * Serialises {@code request} to JSON and returns its SHA-256 digest as a
     * lowercase 64-character hex string.
     */
  private String computeHash(Object request) {
    try {
      byte[] json = objectMapper.writeValueAsBytes(request);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(json);
      return HexFormat.of().formatHex(hash);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialise request for hashing", e);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 algorithm not available", e);
    }
  }
}
