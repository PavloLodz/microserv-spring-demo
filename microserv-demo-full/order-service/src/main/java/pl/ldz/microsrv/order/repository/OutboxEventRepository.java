package pl.ldz.microsrv.order.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import pl.ldz.microsrv.order.entity.OutboxEvent;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

  /**
   * Returns all outbox events with the given status, ordered by creation time ascending.
   *
   * <p>Used by existing unit tests. Retained alongside the pageable overload below
   * until those tests are migrated to use pagination (T8).
   */
  List<OutboxEvent> findByStatusOrderByCreatedAtAsc(String status);

  /**
   * Returns a bounded page of outbox events with the given status, ordered by creation
   * time ascending.
   *
   * <p>Used by the background poller ({@code OutboxService.pollAndPublish}) to cap the
   * number of rows loaded per cycle (configured via {@code outbox.poll-batch-size},
   * default 100). Both overloads coexist cleanly — Spring Data JPA dispatches on the
   * presence of a {@link Pageable} argument.
   */
  Page<OutboxEvent> findByStatusOrderByCreatedAtAsc(String status, Pageable pageable);
}
