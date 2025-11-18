package ai.traceable.certificate.management.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.certificate.management.config.service.v1.manager.CertificateConfigManager;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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

  @Test
  void getCertificatesUpdatesExpiredStatus() {
    Instant now = Instant.now();
    Instant pastExpiration = now.minusSeconds(3600);
    Instant futureExpiration = now.plusSeconds(3600);

    Certificate expiredCertificate =
        Certificate.newBuilder()
            .setId("cert-1")
            .setName("Expired Cert")
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
                    .setExpirationTimestamp(
                        Timestamp.newBuilder()
                            .setSeconds(pastExpiration.getEpochSecond())
                            .setNanos(pastExpiration.getNano())
                            .build())
                    .build())
            .build();

    Certificate expiredCertificateWithUpdatedStatus =
        expiredCertificate.toBuilder()
            .setStatusDetails(
                expiredCertificate.getStatusDetails().toBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_EXPIRED)
                    .build())
            .build();

    Certificate validCertificate =
        Certificate.newBuilder()
            .setId("cert-2")
            .setName("Valid Cert")
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
                    .setExpirationTimestamp(
                        Timestamp.newBuilder()
                            .setSeconds(futureExpiration.getEpochSecond())
                            .setNanos(futureExpiration.getNano())
                            .build())
                    .build())
            .build();

    CertificateFilter filter = CertificateFilter.newBuilder().build();
    GetCertificatesRequest request = GetCertificatesRequest.newBuilder().setFilter(filter).build();

    // Manager now handles the status update logic and returns already-updated certificates
    when(certificateConfigManager.getCertificates(any(), eq(filter)))
        .thenReturn(List.of(expiredCertificateWithUpdatedStatus, validCertificate));

    StreamObserver<GetCertificatesResponse> responseStreamObserver = mock(StreamObserver.class);

    Runnable runnable =
        () -> certificateManagementConfigService.getCertificates(request, responseStreamObserver);

    REQUEST_CONTEXT.run(runnable);

    ArgumentCaptor<GetCertificatesResponse> responseCaptor =
        ArgumentCaptor.forClass(GetCertificatesResponse.class);
    verify(responseStreamObserver, times(1)).onNext(responseCaptor.capture());
    verify(responseStreamObserver, times(1)).onCompleted();

    GetCertificatesResponse response = responseCaptor.getValue();
    assertEquals(2, response.getCertificatesCount());
    assertEquals(
        CertificateStatus.CERTIFICATE_STATUS_EXPIRED,
        response.getCertificates(0).getStatusDetails().getStatus());
    assertEquals(
        CertificateStatus.CERTIFICATE_STATUS_ACTIVE,
        response.getCertificates(1).getStatusDetails().getStatus());
  }
}
