package ai.traceable.api.gateway.config.service.delegate;

import ai.traceable.api.gateway.config.service.store.MetadataConfigStore;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataResponse;
import java.util.List;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ConfigMetadataCreatorImpl implements ConfigMetadataCreator {
  private final MetadataConfigStore metadataConfigStore;

  @Override
  public CreateMetadataResponse create(
      final CreateMetadataRequest request, final RequestContext requestContext) {
    metadataConfigStore.upsertObjects(requestContext, List.of(request.getConfigMetadata()));
    final CreateMetadataResponse.Builder responseBuilder = CreateMetadataResponse.newBuilder();
    return responseBuilder.build();
  }
}
