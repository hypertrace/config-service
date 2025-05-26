package ai.traceable.certificate.management.config.service.v1;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.certificate.management.config.service.v1.manager.CertificateConfigManager;
import io.grpc.stub.StreamObserver;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CertificateManagementConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-certificate-test";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private CertificateConfigManager certificateConfigManager;
  private CertificateManagementConfigServiceImpl certificateManagementConfigService;

  @BeforeEach
  void setup() {
    certificateConfigManager = mock(CertificateConfigManager.class);
    certificateManagementConfigService =
        new CertificateManagementConfigServiceImpl(certificateConfigManager);
  }

  @Test
  void createCertificate() {
    CertificateMetadata certificateMetadata = mock(CertificateMetadata.class);
    CertificateStatusDetails statusDetails = mock(CertificateStatusDetails.class);
    CertificateStorageDetails storageDetails = mock(CertificateStorageDetails.class);
    Certificate certificate = mock(Certificate.class);
    CreateCertificateRequest request =
        CreateCertificateRequest.newBuilder()
            .setName("name")
            .setMetadata(certificateMetadata)
            .setStatusDetails(statusDetails)
            .addStorage(storageDetails)
            .build();

    when(certificateConfigManager.createCertificate(any(), eq(request))).thenReturn(certificate);

    StreamObserver<CreateCertificateResponse> responseStreamObserver = mock(StreamObserver.class);

    Runnable runnable =
        () -> certificateManagementConfigService.createCertificate(request, responseStreamObserver);

    REQUEST_CONTEXT.run(runnable);

    verify(responseStreamObserver, times(1))
        .onNext(CreateCertificateResponse.newBuilder().setCertificate(certificate).build());
    verify(responseStreamObserver, times(1)).onCompleted();
  }
}
