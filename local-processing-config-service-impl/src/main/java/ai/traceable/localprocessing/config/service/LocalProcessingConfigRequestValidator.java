package ai.traceable.localprocessing.config.service;

import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class LocalProcessingConfigRequestValidator {
  public void validateOrThrow(RequestContext requestContext, GetApiNamingModelRequest request) {
    this.validateRequestContext(requestContext);
  }

  private void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }
}
