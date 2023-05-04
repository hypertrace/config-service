package ai.traceable.api.gateway.config.service.delegate;

import ai.traceable.api.gateway.config.service.v1.CreateMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataResponse;
import com.google.inject.ImplementedBy;
import org.hypertrace.core.grpcutils.context.RequestContext;

@ImplementedBy(ConfigMetadataCreatorImpl.class)
public interface ConfigMetadataCreator {
  CreateMetadataResponse create(
      final CreateMetadataRequest request, final RequestContext requestContext);
}
