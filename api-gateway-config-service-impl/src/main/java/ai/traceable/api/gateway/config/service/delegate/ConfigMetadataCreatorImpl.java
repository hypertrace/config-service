package ai.traceable.api.gateway.config.service.delegate;

import static java.util.Collections.unmodifiableList;

import ai.traceable.api.gateway.config.service.store.MetadataConfigStore;
import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataResponse;
import ai.traceable.api.gateway.config.service.v1.SourceInfo;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ConfigMetadataCreatorImpl implements ConfigMetadataCreator {
  private final MetadataConfigStore metadataConfigStore;

  @Override
  public CreateMetadataResponse create(
      final CreateMetadataRequest request, final RequestContext requestContext) {
    final List<ConfigMetadata> metadataList = new ArrayList<>();
    final ConfigMetadata metadataInRequest = request.getConfigMetadata();

    // Split it into multiple metadata objects, one for each source info, to persist them separately
    for (final SourceInfo sourceInfo : metadataInRequest.getSourceInfoList()) {
      final ConfigMetadata newMetadata =
          metadataInRequest.toBuilder().clearSourceInfo().addSourceInfo(sourceInfo).build();
      metadataList.add(newMetadata);
    }

    metadataConfigStore.upsertObjects(requestContext, unmodifiableList(metadataList));
    final CreateMetadataResponse.Builder responseBuilder = CreateMetadataResponse.newBuilder();
    return responseBuilder.build();
  }
}
