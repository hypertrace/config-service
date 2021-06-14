package ai.traceable.licensestatus.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusResponse;
import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub;
import ai.traceable.licensestatus.config.service.v1.UpdateLicenseStatusRequest;
import ai.traceable.licensestatus.config.service.v1.UpdateLicenseStatusResponse;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.util.Map;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LicenseStatusConfigServiceImplTest {

  LicenseStatusConfigServiceBlockingStub licenseStatusConfigStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    Config config =
        ConfigFactory.parseMap(
            Map.of(
                ConfigServiceCoordinatorImpl.LICENSE_STATUS_CONFIG_SERVICE,
                Map.of(
                    ConfigServiceCoordinatorImpl.DEFAULT_LICENSE_LIMIT,
                    LicenseLimit.LICENSE_LIMIT_AVAILABLE.name())));

    Channel channel = mockGenericConfigService.channel();
    mockGenericConfigService
        .addService(new LicenseStatusConfigServiceImpl(channel, config))
        .start();

    licenseStatusConfigStub = LicenseStatusConfigServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void upsertLicenseStatus() {
    LicenseStatus licenseStatus =
        LicenseStatus.newBuilder()
            .setTracesLicenseLimit(LicenseLimit.LICENSE_LIMIT_AVAILABLE)
            .build();
    UpdateLicenseStatusResponse response =
        licenseStatusConfigStub.updateLicenseStatus(
            UpdateLicenseStatusRequest.newBuilder().setLicenseStatus(licenseStatus).build());
    assertEquals(licenseStatus, response.getLicenseStatus());
  }

  @Test
  void getLicenseStatus() {
    LicenseStatus licenseStatus =
        LicenseStatus.newBuilder()
            .setTracesLicenseLimit(LicenseLimit.LICENSE_LIMIT_AVAILABLE)
            .build();
    UpdateLicenseStatusResponse updateResponse =
        licenseStatusConfigStub.updateLicenseStatus(
            UpdateLicenseStatusRequest.newBuilder().setLicenseStatus(licenseStatus).build());
    assertEquals(licenseStatus, updateResponse.getLicenseStatus());
    GetLicenseStatusResponse response =
        licenseStatusConfigStub.getLicenseStatus(GetLicenseStatusRequest.newBuilder().build());
    assertEquals(licenseStatus, response.getLicenseStatus());
  }

  @Test
  void getLicenseStatusExhausted() {
    LicenseStatus licenseStatus =
        LicenseStatus.newBuilder()
            .setTracesLicenseLimit(LicenseLimit.LICENSE_LIMIT_EXHAUSTED)
            .build();
    UpdateLicenseStatusResponse updateResponse =
        licenseStatusConfigStub.updateLicenseStatus(
            UpdateLicenseStatusRequest.newBuilder().setLicenseStatus(licenseStatus).build());
    assertEquals(licenseStatus, updateResponse.getLicenseStatus());
    GetLicenseStatusResponse getResponse =
        licenseStatusConfigStub.getLicenseStatus(GetLicenseStatusRequest.newBuilder().build());
    assertEquals(licenseStatus, getResponse.getLicenseStatus());
  }
}
