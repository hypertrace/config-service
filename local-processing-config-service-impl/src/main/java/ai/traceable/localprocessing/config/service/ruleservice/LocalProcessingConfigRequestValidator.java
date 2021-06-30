package ai.traceable.localprocessing.config.service.ruleservice;

import ai.traceable.localprocessing.config.service.v1.GetDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class LocalProcessingConfigRequestValidator {

  static void validateOrThrow(
      RequestContext requestContext, GetDefaultProtectionModeRequest request) {
    validateRequestContext(requestContext);
  }

  static void validateOrThrow(
      RequestContext requestContext, UpdateDefaultProtectionModeRequest request) {
    validateRequestContext(requestContext);
    validateDefaultProtectionMode(request.getDefaultProtectionMode());
  }

  private static void validateDefaultProtectionMode(ProtectionMode defaultProtectionMode) {
    switch (defaultProtectionMode) {
      case UNRECOGNIZED:
      case PROTECTION_MODE_UNSPECIFIED:
        throw new IllegalArgumentException("default protection mode should be a valid value");
      default:
    }
  }

  private static void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }
}
