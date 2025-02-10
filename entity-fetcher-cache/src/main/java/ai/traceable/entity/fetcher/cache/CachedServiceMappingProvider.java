package ai.traceable.entity.fetcher.cache;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CachedServiceMappingProvider {
  Optional<ServiceIdentifierEntity> getServiceIdentifierEntity(
      RequestContext requestContext, @NonNull final String serviceId);

  Map<String, Optional<ServiceIdentifierEntity>> getServiceIdentifierEntities(
      RequestContext requestContext, @NonNull final Set<String> serviceIds);

  @Value
  class ServiceIdentifierEntity {
    String serviceName;
    Optional<String> environmentId;
  }
}
