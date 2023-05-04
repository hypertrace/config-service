package ai.traceable.api.gateway.config.service.delegate;

import ai.traceable.api.gateway.config.service.v1.DeleteMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataResponse;
import com.google.inject.ImplementedBy;
import org.hypertrace.core.grpcutils.context.RequestContext;

@ImplementedBy(ConfigMetadataDeleterImpl.class)
public interface ConfigMetadataDeleter {
  DeleteMetadataResponse delete(
      final DeleteMetadataRequest request, final RequestContext requestContext);
}
