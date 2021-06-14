package ai.traceable.licensestatus.config.service;

import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfigServiceCoordinator {

  LicenseStatus upsertLicenseStatusConfig(
      RequestContext requestContext, LicenseStatus licenseStatus);

  LicenseStatus getLicenseStatusConfig(RequestContext requestContext);
}
