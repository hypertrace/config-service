package ai.traceable.api.gateway.config.service.delegate;

import ai.traceable.api.gateway.config.service.store.MetadataConfigStore;
import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.GetMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.GetMetadataResponse;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ConfigMetadataGetterImpl implements ConfigMetadataGetter {
  private final MetadataConfigStore metadataConfigStore;

  @Override
  public GetMetadataResponse get(
      final GetMetadataRequest request, final RequestContext requestContext) {
    final List<ConfigMetadata> configMetadata =
        metadataConfigStore.getAllConfigData(requestContext, request.getMetadataFilter());
    return GetMetadataResponse.newBuilder().addAllConfigMetadata(configMetadata).build();
  }
}
