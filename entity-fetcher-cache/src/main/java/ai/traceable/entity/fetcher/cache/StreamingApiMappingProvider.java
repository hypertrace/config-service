package ai.traceable.entity.fetcher.cache;

import java.util.List;
import java.util.stream.Stream;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface StreamingApiMappingProvider {
  Stream<HttpApiDetails> getLearntHttpApiDetails(
      RequestContext requestContext, String serviceName, String environment);

  @Value
  class HttpApiDetails {
    String apiId;
    String httpMethod;
    List<String> resolvedUrlPatterns;
  }
}
