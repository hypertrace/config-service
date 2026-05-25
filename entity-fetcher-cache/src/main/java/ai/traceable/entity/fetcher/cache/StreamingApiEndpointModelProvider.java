package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.config.ApiEndpointModelFetchConfig;
import ai.traceable.protection.data.context.v1.ApiDetectionModel;
import java.util.stream.Stream;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface StreamingApiEndpointModelProvider {

  Stream<ApiEndpointModelDetails> getApiEndpointModels(
      RequestContext requestContext,
      String serviceName,
      String environment,
      ApiEndpointModelFetchConfig config,
      ModelTypeFilter modelTypeFilter);

  @Value
  class ApiEndpointModelDetails {
    String apiId;
    ApiDetectionModel apiDetectionModel;
  }

  enum ModelTypeFilter {
    LEARNT,
    USER_DEFINED,
    BOTH
  }
}
