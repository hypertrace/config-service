package ai.traceable.entity.fetcher.cache;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.NonNull;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface CachedApiMappingProvider {
  Map<String, Optional<ApiIdentifierEntity>> getApiIdentifierEntities(
      RequestContext requestContext, @NonNull final Set<String> apiIds);

  Map<String, Set<ApiIdentifierEntity>> getApiIdentifierEntitiesHavingLabels(
      RequestContext requestContext, @NonNull final Set<String> apiLabelIds);

  @Value
  class ApiIdentifierEntity {
    String apiId;
    String apiName;
    String apiUrlPattern;
    List<String> resolvedUrlPatterns;
    List<String> apiLabels;
  }
}
