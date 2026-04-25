package pl.ldz.microsrv.order.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import pl.ldz.microsrv.order.entity.IdempotencyKey;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface IdempotencyKeyRepository extends JpaRepository<IdempotencyKey, UUID> {

    Optional<IdempotencyKey> findByKey(String key);

    List<IdempotencyKey> findByExpiresAtBefore(OffsetDateTime cutoff);
}
