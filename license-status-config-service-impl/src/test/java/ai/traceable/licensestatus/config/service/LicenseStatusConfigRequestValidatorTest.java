package ai.traceable.licensestatus.config.service;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.UpdateLicenseStatusRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class LicenseStatusConfigRequestValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant-1");

  @Test
  void validateLicenseLimit() {
    final UpdateLicenseStatusRequest updateLicenseStatusRequest =
        getUpdateLicenseStatusRequest(LICENSE_LIMIT_UNSPECIFIED);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LicenseStatusConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT, updateLicenseStatusRequest));
  }

  private UpdateLicenseStatusRequest getUpdateLicenseStatusRequest(LicenseLimit licenseLimit) {
    return UpdateLicenseStatusRequest.newBuilder()
        .build()
        .newBuilder()
        .setLicenseStatus(LicenseStatus.newBuilder().setTracesLicenseLimit(licenseLimit).build())
        .build();
  }
}
