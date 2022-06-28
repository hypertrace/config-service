package ai.traceable.span.processing.config.service.licensestatus;

import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import com.google.inject.Inject;

public class DefaultLicenseStatusConfigManager implements LicenseStatusConfigManager {

  private final LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub
      licenseStatusConfigServiceBlockingStub;

  @Inject
  public DefaultLicenseStatusConfigManager(
      LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub
          licenseStatusConfigServiceBlockingStub) {
    this.licenseStatusConfigServiceBlockingStub = licenseStatusConfigServiceBlockingStub;
  }

  public LicenseStatus getLicenseStatus() {
    return licenseStatusConfigServiceBlockingStub
        .getLicenseStatus(GetLicenseStatusRequest.newBuilder().build())
        .getLicenseStatus();
  }
}
