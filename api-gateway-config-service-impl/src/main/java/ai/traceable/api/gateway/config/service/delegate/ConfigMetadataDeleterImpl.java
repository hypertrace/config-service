package ai.traceable.api.gateway.config.service.delegate;

import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.api.gateway.config.service.store.GatewayConfigMetadataIdGenerator;
import ai.traceable.api.gateway.config.service.store.MetadataConfigStore;
import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataResponse;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ConfigMetadataDeleterImpl implements ConfigMetadataDeleter {
  private final MetadataConfigStore metadataConfigStore;
  private final GatewayConfigMetadataIdGenerator idGenerator;

  @Override
  public DeleteMetadataResponse delete(
      final DeleteMetadataRequest request, final RequestContext requestContext) {
    final List<ConfigMetadata> configMetadataToDelete =
        metadataConfigStore.getAllConfigData(requestContext, request.getMetadataFilter());
    final List<String> idsToDelete =
        configMetadataToDelete.stream().map(idGenerator::generateId).collect(toUnmodifiableList());
    if (!idsToDelete.isEmpty()) {
      metadataConfigStore.deleteObjects(requestContext, idsToDelete);
    }
    return DeleteMetadataResponse.newBuilder().build();
  }
}
