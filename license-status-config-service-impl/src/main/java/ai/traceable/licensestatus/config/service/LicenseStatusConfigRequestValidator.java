package ai.traceable.licensestatus.config.service;

import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.UpdateLicenseStatusRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class LicenseStatusConfigRequestValidator {

  private LicenseStatusConfigRequestValidator() {}

  public static void validateOrThrow(
      RequestContext requestContext, UpdateLicenseStatusRequest request) {
    validateRequestContext(requestContext);
    validateLicenseStatus(request.getLicenseStatus());
  }

  public static void validateOrThrow(RequestContext requestContext) {
    validateRequestContext(requestContext);
  }

  private static void validateLicenseStatus(LicenseStatus licenseStatus) {
    switch (licenseStatus.getTracesLicenseLimit()) {
      case UNRECOGNIZED:
      case LICENSE_LIMIT_UNSPECIFIED:
        throw new IllegalArgumentException("traces license limit should be a valid value");
      default:
    }
  }

  private static void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }
}
