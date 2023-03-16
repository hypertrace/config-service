package ai.traceable.api.gateway.config.service.delegate;

import ai.traceable.api.gateway.config.service.v1.DeleteRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApiRoutesDeleter {
  DeleteRoutesResponse delete(
      final DeleteRoutesRequest request, final RequestContext requestContext);
}
