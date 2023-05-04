package ai.traceable.api.gateway.config.service.delegate;

import ai.traceable.api.gateway.config.service.v1.GetMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.GetMetadataResponse;
import com.google.inject.ImplementedBy;
import org.hypertrace.core.grpcutils.context.RequestContext;

@ImplementedBy(ConfigMetadataGetterImpl.class)
public interface ConfigMetadataGetter {
  GetMetadataResponse get(final GetMetadataRequest request, final RequestContext requestContext);
}
