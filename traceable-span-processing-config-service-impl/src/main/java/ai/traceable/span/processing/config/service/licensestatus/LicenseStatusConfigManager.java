package ai.traceable.span.processing.config.service.licensestatus;

import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface LicenseStatusConfigManager {
  LicenseStatus getLicenseStatus(RequestContext requestContext);
}
