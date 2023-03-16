package ai.traceable.api.gateway.config.service.delegate;

import ai.traceable.api.gateway.config.service.v1.CreateRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApiRoutesCreator {
  CreateRoutesResponse create(
      final CreateRoutesRequest request, final RequestContext requestContext);
}
