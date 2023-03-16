package ai.traceable.api.gateway.config.service.delegate;

import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApiRoutesGetter {
  GetRoutesResponse get(final GetRoutesRequest request, final RequestContext requestContext);
}
