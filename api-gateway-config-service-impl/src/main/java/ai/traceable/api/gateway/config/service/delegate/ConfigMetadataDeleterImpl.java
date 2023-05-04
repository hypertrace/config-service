package ai.traceable.api.gateway.config.service.delegate;

import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.api.gateway.config.service.store.MetadataConfigStore;
import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataResponse;
import java.util.List;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ConfigMetadataDeleterImpl implements ConfigMetadataDeleter {
  private final MetadataConfigStore metadataConfigStore;

  @Override
  public DeleteMetadataResponse delete(
      final DeleteMetadataRequest request, final RequestContext requestContext) {
    final List<ConfigMetadata> configMetadataToDelete =
        metadataConfigStore.getAllConfigData(requestContext, request.getMetadataFilter());
    final List<String> orgIds =
        configMetadataToDelete.stream().map(ConfigMetadata::getOrgId).collect(toUnmodifiableList());
    if (!orgIds.isEmpty()) {
      metadataConfigStore.deleteObjects(requestContext, orgIds);
    }
    return DeleteMetadataResponse.newBuilder().build();
  }
}
