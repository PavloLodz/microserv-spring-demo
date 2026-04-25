package pl.ldz.microsrv.order.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import pl.ldz.microsrv.order.entity.OutboxEvent;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

  List<OutboxEvent> findByStatusOrderByCreatedAtAsc(String status);
}
