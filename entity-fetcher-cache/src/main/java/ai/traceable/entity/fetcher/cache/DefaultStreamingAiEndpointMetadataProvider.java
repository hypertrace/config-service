package ai.traceable.entity.fetcher.cache;

import com.google.inject.Inject;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultStreamingAiEndpointMetadataProvider
    implements StreamingAiEndpointMetadataProvider {
  private final EntityQueryServiceClient entityQueryServiceClient;

  @Override
  public Stream<ApiAiEndpointMetadataDetails> getAllAiEndpointMetadata(
      RequestContext requestContext, String serviceName, String environment) {

    return entityQueryServiceClient.getAllAiEndpointMetadata(
        requestContext, serviceName, environment);
  }
}
