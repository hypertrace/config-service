package ai.traceable.entity.fetcher.cache;

import com.google.inject.Inject;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultStreamingApiMappingProvider implements StreamingApiMappingProvider {
  private final CachedServiceMappingProvider cachedServiceMappingProvider;
  private final EntityQueryServiceClient entityQueryServiceClient;

  @Override
  public Stream<HttpApiDetails> getLearntHttpApiDetails(
      RequestContext requestContext, String serviceName, String environment) {
    return entityQueryServiceClient.getAllLearntHttpApiEndpoints(
        requestContext, serviceName, environment);
  }
}
