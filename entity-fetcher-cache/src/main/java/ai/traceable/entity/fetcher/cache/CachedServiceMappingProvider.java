package ai.traceable.entity.fetcher.cache;

import java.util.Optional;
import lombok.NonNull;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CachedServiceMappingProvider {
  Optional<ServiceIdentifierEntity> getServiceIdentifierEntity(
      RequestContext requestContext, @NonNull final String serviceId);

  @Value
  class ServiceIdentifierEntity {
    String serviceName;
    Optional<String> environmentId;
  }
}
