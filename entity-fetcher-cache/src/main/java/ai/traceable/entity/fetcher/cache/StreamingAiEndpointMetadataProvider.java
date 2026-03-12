package ai.traceable.entity.fetcher.cache;

import ai.traceable.protection.data.context.v1.AiEndpointMetadata;
import java.util.stream.Stream;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface StreamingAiEndpointMetadataProvider {
  Stream<ApiAiEndpointMetadataDetails> getAllAiEndpointMetadata(
      RequestContext requestContext, String serviceName, String environment);

  @Value
  class ApiAiEndpointMetadataDetails {
    String apiId;
    AiEndpointMetadata aiEndpointMetadata;
  }
}
