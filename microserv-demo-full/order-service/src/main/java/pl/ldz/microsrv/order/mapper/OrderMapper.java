package pl.ldz.microsrv.order.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import pl.ldz.microsrv.order.api.model.OrderRequest;
import pl.ldz.microsrv.order.api.model.OrderResponse;
import pl.ldz.microsrv.order.entity.Order;

import java.util.List;

/**
 * MapStruct mapper between the {@link Order} JPA entity and the generated OpenAPI DTOs.
 *
 * <p>Spring-managed ({@code componentModel = "spring"}) so it can be injected via constructor.
 * The {@code mapstruct.defaultComponentModel=spring} compiler arg in {@code pom.xml} is the
 * project-wide default, but the annotation here makes the intent explicit.
 */
@Mapper(componentModel = "spring")
public interface OrderMapper {

  /**
   * Maps an {@link Order} entity to the generated {@link OrderResponse} DTO.
   * All fields are mapped by name; no custom rules needed on the read path.
   */
  OrderResponse toResponse(Order order);

  /**
   * Convenience overload for mapping a full list of entities.
   * MapStruct generates the implementation by delegating to {@link #toResponse(Order)}.
   */
  List<OrderResponse> toResponseList(List<Order> orders);

  /**
   * Maps an {@link OrderRequest} DTO to an {@link Order} entity.
   *
   * <p>Lifecycle fields ({@code id}, {@code debugId}, {@code createdAt}, {@code updatedAt},
   * {@code deletedAt}, {@code version}) are explicitly ignored — the service layer owns them.
   * {@code status} is also ignored here; the service always sets it to
   * {@code OrderStatus.CREATED} on creation and manages transitions thereafter.
   */
  @Mapping(target = "id", ignore = true)
  @Mapping(target = "debugId", ignore = true)
  @Mapping(target = "createdAt", ignore = true)
  @Mapping(target = "updatedAt", ignore = true)
  @Mapping(target = "deletedAt", ignore = true)
  @Mapping(target = "version", ignore = true)
  @Mapping(target = "status", ignore = true)
  Order toEntity(OrderRequest request);
}
