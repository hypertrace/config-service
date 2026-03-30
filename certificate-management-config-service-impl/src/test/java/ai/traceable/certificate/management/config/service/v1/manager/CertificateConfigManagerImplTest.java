package ai.traceable.certificate.management.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.certificate.management.config.service.v1.AwsStorageDetails;
import ai.traceable.certificate.management.config.service.v1.Certificate;
import ai.traceable.certificate.management.config.service.v1.CertificateFilter;
import ai.traceable.certificate.management.config.service.v1.CertificateMetadata;
import ai.traceable.certificate.management.config.service.v1.CertificateMetadataUpdate;
import ai.traceable.certificate.management.config.service.v1.CertificateStatus;
import ai.traceable.certificate.management.config.service.v1.CertificateStatusDetails;
import ai.traceable.certificate.management.config.service.v1.CertificateStorageDetails;
import ai.traceable.certificate.management.config.service.v1.CertificateType;
import ai.traceable.certificate.management.config.service.v1.CertificateUpdate;
import ai.traceable.certificate.management.config.service.v1.CreateCertificateRequest;
import ai.traceable.certificate.management.config.service.v1.DeleteCertificateRequest;
import ai.traceable.certificate.management.config.service.v1.StringList;
import ai.traceable.certificate.management.config.service.v1.StringMap;
import ai.traceable.certificate.management.config.service.v1.UpdateCertificateRequest;
import ai.traceable.certificate.management.config.service.v1.store.CertificateConfigStore;
import ai.traceable.certificate.management.config.service.v1.validator.CertificateUsageValidator;
import ai.traceable.config.utils.UuidGenerator;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CertificateConfigManagerImplTest {

  @Mock private CertificateConfigStore store;

  @Mock private CertificateValidator validator;

  @Mock private CertificateUsageValidator certificateUsageValidator;

  @Mock private UuidGenerator uuidGenerator;

  @Mock private RequestContext requestContext;

  private CertificateConfigManagerImpl manager;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    manager =
        new CertificateConfigManagerImpl(
            store, validator, uuidGenerator, certificateUsageValidator);

    // Mock updateCertificateSilently to return the same certificate passed to it
    when(store.updateCertificateSilently(any(RequestContext.class), any(Certificate.class)))
        .thenAnswer(invocation -> invocation.getArgument(1));
  }

  @Test
  void testCreateCertificate() {
    CertificateMetadata metadata = createValidMetadata();
    CertificateStatusDetails statusDetails =
        CertificateStatusDetails.newBuilder()
            .setStatus(CertificateStatus.CERTIFICATE_STATUS_PENDING)
            .setExpirationTimestamp(
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
            .build();
    List<CertificateStorageDetails> storage =
        Collections.singletonList(createValidStorageDetails());

    when(validator.validate(any(CreateCertificateRequest.class))).thenReturn(Status.OK);
    when(uuidGenerator.generateRandomId()).thenReturn("cert-123");

    Certificate expectedCertificate =
        Certificate.newBuilder()
            .setId("cert-123")
            .setMetadata(metadata)
            .setStatusDetails(statusDetails)
            .addAllStorage(storage)
            .build();

    when(store.createCertificate(eq(requestContext), any(Certificate.class)))
        .thenReturn(expectedCertificate);

    CreateCertificateRequest request =
        CreateCertificateRequest.newBuilder()
            .setName("ma,e")
            .setMetadata(metadata)
            .setStatusDetails(statusDetails)
            .addAllStorage(storage)
            .build();

    Certificate result = manager.createCertificate(requestContext, request);

    assertNotNull(result);
    assertEquals("cert-123", result.getId());
    assertEquals(metadata, result.getMetadata());
    assertEquals(statusDetails, result.getStatusDetails());
    assertEquals(storage, result.getStorageList());

    verify(validator).validate(eq(request));
    verify(store).createCertificate(eq(requestContext), any(Certificate.class));
  }

  @Test
  void testGetCertificates() {
    CertificateFilter filter = CertificateFilter.getDefaultInstance();
    List<Certificate> expectedCertificates =
        Arrays.asList(
            Certificate.newBuilder()
                .setId("cert-1")
                .setMetadata(createValidMetadata())
                .addStorage(createValidStorageDetails())
                .build(),
            Certificate.newBuilder()
                .setId("cert-2")
                .setMetadata(createValidMetadata())
                .addStorage(createValidStorageDetails())
                .build());

    when(store.getCertificates(requestContext, filter)).thenReturn(expectedCertificates);

    List<Certificate> result = manager.getCertificates(requestContext, filter);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("cert-1", result.get(0).getId());
    assertEquals("cert-2", result.get(1).getId());
    verify(store).getCertificates(requestContext, filter);
  }

  @Test
  void testGetCertificates_FilterById() {
    // Create a filter with specific IDs
    CertificateFilter filter = CertificateFilter.newBuilder().addIds("cert-1").build();

    // Create test certificates
    Certificate certificate1 =
        Certificate.newBuilder()
            .setId("cert-1")
            .setName("cert-name-1")
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .build();

    List<Certificate> expectedCertificates = List.of(certificate1);

    when(store.getCertificates(requestContext, filter)).thenReturn(expectedCertificates);

    List<Certificate> result = manager.getCertificates(requestContext, filter);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("cert-1", result.get(0).getId());
    verify(store).getCertificates(requestContext, filter);
  }

  @Test
  void testGetCertificates_FilterByAwsRegion() {
    // Create a filter with AWS-specific filtering
    CertificateFilter.AwsFilter awsFilter =
        CertificateFilter.AwsFilter.newBuilder().addRegions("us-east-1").build();

    CertificateFilter filter = CertificateFilter.newBuilder().setAwsFilter(awsFilter).build();

    // Create test certificates
    Certificate certificate1 =
        Certificate.newBuilder()
            .setId("cert-1")
            .setName("cert-name-1")
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .build();

    List<Certificate> expectedCertificates = List.of(certificate1);

    when(store.getCertificates(requestContext, filter)).thenReturn(expectedCertificates);

    List<Certificate> result = manager.getCertificates(requestContext, filter);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("cert-1", result.get(0).getId());
    verify(store).getCertificates(requestContext, filter);
  }

  @Test
  void testGetCertificates_FilterByLabelKeys() {
    // Create a filter with label keys
    CertificateFilter filter =
        CertificateFilter.newBuilder().addLabelKeys("env").addLabelKeys("team").build();

    // Create test certificates
    Certificate certificate1 =
        Certificate.newBuilder()
            .setId("cert-1")
            .setName("cert-name-1")
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .build();

    List<Certificate> expectedCertificates = List.of(certificate1);

    when(store.getCertificates(requestContext, filter)).thenReturn(expectedCertificates);

    List<Certificate> result = manager.getCertificates(requestContext, filter);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("cert-1", result.get(0).getId());
    verify(store).getCertificates(requestContext, filter);
  }

  @Test
  void testGetCertificates_UpdatesExpiredCertificateStatus() {
    CertificateFilter filter = CertificateFilter.getDefaultInstance();

    // Create an expired certificate (expiration 1 hour ago)
    long pastExpiration = (System.currentTimeMillis() / 1000) - 3600;
    Certificate expiredCertificate =
        Certificate.newBuilder()
            .setId("cert-expired")
            .setName("Expired Certificate")
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .setCertificateType(
                ai.traceable.certificate.management.config.service.v1.CertificateType
                    .CERTIFICATE_TYPE_HOSTED)
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
                    .setExpirationTimestamp(Timestamp.newBuilder().setSeconds(pastExpiration))
                    .build())
            .build();

    // Create a valid certificate (expires in 1 hour)
    long futureExpiration = (System.currentTimeMillis() / 1000) + 3600;
    Certificate validCertificate =
        Certificate.newBuilder()
            .setId("cert-valid")
            .setName("Valid Certificate")
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .setCertificateType(
                ai.traceable.certificate.management.config.service.v1.CertificateType
                    .CERTIFICATE_TYPE_HOSTED)
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
                    .setExpirationTimestamp(Timestamp.newBuilder().setSeconds(futureExpiration))
                    .build())
            .build();

    // Expected updated certificate with EXPIRED status
    Certificate expectedUpdatedCertificate =
        expiredCertificate.toBuilder()
            .setStatusDetails(
                expiredCertificate.getStatusDetails().toBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_EXPIRED)
                    .build())
            .build();

    when(store.getCertificates(requestContext, filter))
        .thenReturn(Arrays.asList(expiredCertificate, validCertificate));
    when(store.updateCertificate(eq(requestContext), any(Certificate.class)))
        .thenReturn(expectedUpdatedCertificate);

    List<Certificate> result = manager.getCertificates(requestContext, filter);

    assertNotNull(result);
    assertEquals(2, result.size());

    // First certificate should be marked as EXPIRED
    assertEquals("cert-expired", result.get(0).getId());
    assertEquals(
        CertificateStatus.CERTIFICATE_STATUS_EXPIRED, result.get(0).getStatusDetails().getStatus());

    // Second certificate should remain ACTIVE
    assertEquals("cert-valid", result.get(1).getId());
    assertEquals(
        CertificateStatus.CERTIFICATE_STATUS_ACTIVE, result.get(1).getStatusDetails().getStatus());

    verify(store).getCertificates(requestContext, filter);
    verify(store).updateCertificateSilently(eq(requestContext), any(Certificate.class));
  }

  @Test
  void testGetCertificates_SkipsAlreadyExpiredCertificates() {
    CertificateFilter filter = CertificateFilter.getDefaultInstance();

    // Create a certificate that is already marked as EXPIRED
    long pastExpiration = (System.currentTimeMillis() / 1000) - 3600;
    Certificate alreadyExpiredCertificate =
        Certificate.newBuilder()
            .setId("cert-already-expired")
            .setName("Already Expired Certificate")
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_EXPIRED)
                    .setExpirationTimestamp(Timestamp.newBuilder().setSeconds(pastExpiration))
                    .build())
            .build();

    when(store.getCertificates(requestContext, filter))
        .thenReturn(List.of(alreadyExpiredCertificate));

    List<Certificate> result = manager.getCertificates(requestContext, filter);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("cert-already-expired", result.get(0).getId());
    assertEquals(
        CertificateStatus.CERTIFICATE_STATUS_EXPIRED, result.get(0).getStatusDetails().getStatus());

    verify(store).getCertificates(requestContext, filter);
    // Should NOT call updateCertificate since it's already expired
    verify(store, never()).updateCertificate(any(), any());
  }

  @Test
  void testGetCertificates_SkipsCertificatesWithoutExpirationTimestamp() {
    CertificateFilter filter = CertificateFilter.getDefaultInstance();

    // Create a certificate without expiration timestamp
    Certificate certificateWithoutExpiration =
        Certificate.newBuilder()
            .setId("cert-no-expiration")
            .setName("Certificate Without Expiration")
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
                    .build())
            .build();

    when(store.getCertificates(requestContext, filter))
        .thenReturn(List.of(certificateWithoutExpiration));

    List<Certificate> result = manager.getCertificates(requestContext, filter);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("cert-no-expiration", result.get(0).getId());
    assertEquals(
        CertificateStatus.CERTIFICATE_STATUS_ACTIVE, result.get(0).getStatusDetails().getStatus());

    verify(store).getCertificates(requestContext, filter);
    // Should NOT call updateCertificate since there's no expiration timestamp
    verify(store, never()).updateCertificate(any(), any());
  }

  @Test
  void testUpdateCertificate_StatusUpdate() {
    String id = "cert-123";
    Timestamp expirationTimestamp =
        Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();

    // Create the certificate update with status change
    CertificateUpdate certificateUpdate =
        CertificateUpdate.newBuilder()
            .setUpdatedStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
            .setUpdatedExpirationTimestamp(expirationTimestamp)
            .build();

    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder()
            .setId(id)
            .setCertificateUpdate(certificateUpdate)
            .build();

    // Create existing certificate
    Certificate existingCertificate =
        Certificate.newBuilder()
            .setId(id)
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .build();

    // Create expected status details after update
    CertificateStatusDetails expectedStatusDetails =
        CertificateStatusDetails.newBuilder()
            .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
            .setExpirationTimestamp(expirationTimestamp)
            .build();

    // Create expected updated certificate
    Certificate expectedUpdatedCertificate =
        Certificate.newBuilder()
            .setId(id)
            .setMetadata(createValidMetadata())
            .setStatusDetails(expectedStatusDetails)
            .addStorage(createValidStorageDetails())
            .build();

    when(validator.validate(any(UpdateCertificateRequest.class))).thenReturn(Status.OK);
    when(store.getCertificate(requestContext, id)).thenReturn(existingCertificate);
    when(store.updateCertificate(requestContext, expectedUpdatedCertificate))
        .thenReturn(expectedUpdatedCertificate);

    Certificate result = manager.updateCertificate(requestContext, id, request);

    assertNotNull(result);
    assertEquals(id, result.getId());
    assertEquals(expectedStatusDetails, result.getStatusDetails());

    verify(validator).validate(any(UpdateCertificateRequest.class));
    verify(store).getCertificate(requestContext, id);
    verify(store).updateCertificate(requestContext, expectedUpdatedCertificate);
  }

  @Test
  void testUpdateCertificate_StatusAndExpirationWithCertificateUpdate() {
    String id = "cert-123";

    Timestamp expirationTimestamp =
        Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000 + 86400).build();

    // Create certificate update with status and expiration timestamp changes
    CertificateUpdate certificateUpdate =
        CertificateUpdate.newBuilder()
            .setUpdatedStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
            .setUpdatedExpirationTimestamp(expirationTimestamp)
            .build();

    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder()
            .setId(id)
            .setCertificateUpdate(certificateUpdate)
            .build();

    // Create existing certificate with metadata and status details
    CertificateMetadata existingMetadata = createValidMetadata();
    CertificateStatusDetails existingStatusDetails =
        CertificateStatusDetails.newBuilder()
            .setStatus(CertificateStatus.CERTIFICATE_STATUS_PENDING)
            .setExpirationTimestamp(
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
            .build();

    Certificate existingCertificate =
        Certificate.newBuilder()
            .setId(id)
            .setName("test-cert")
            .setMetadata(existingMetadata)
            .setStatusDetails(existingStatusDetails)
            .addStorage(createValidStorageDetails())
            .build();

    // Create expected updated status details
    CertificateStatusDetails expectedStatusDetails =
        CertificateStatusDetails.newBuilder()
            .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
            .setExpirationTimestamp(expirationTimestamp)
            .build();

    Certificate expectedUpdatedCertificate =
        existingCertificate.toBuilder().setStatusDetails(expectedStatusDetails).build();

    when(validator.validate(any(UpdateCertificateRequest.class))).thenReturn(Status.OK);
    when(store.getCertificate(requestContext, id)).thenReturn(existingCertificate);
    when(store.updateCertificate(requestContext, expectedUpdatedCertificate))
        .thenReturn(expectedUpdatedCertificate);

    Certificate result = manager.updateCertificate(requestContext, id, request);

    assertNotNull(result);
    assertEquals(id, result.getId());
    assertEquals(
        CertificateStatus.CERTIFICATE_STATUS_ACTIVE, result.getStatusDetails().getStatus());
    assertEquals(expirationTimestamp, result.getStatusDetails().getExpirationTimestamp());

    verify(validator).validate(any(UpdateCertificateRequest.class));
    verify(store).getCertificate(requestContext, id);
    verify(store).updateCertificate(requestContext, expectedUpdatedCertificate);
  }

  @Test
  void testUpdateCertificate_CertificateUpdate() {
    String id = "cert-123";

    // Create domain names list
    StringList domainNames =
        StringList.newBuilder().addStringValues("example.com").addStringValues("test.com").build();

    StringMap labelsMap =
        StringMap.newBuilder().putMap("env", "prod").putMap("team", "security").build();

    CertificateMetadataUpdate metadataUpdate =
        CertificateMetadataUpdate.newBuilder()
            .setUpdatedDomainNames(domainNames)
            .setUpdatedKeyAlgorithm("RSA-2048")
            .setUpdatedLabels(labelsMap)
            .build();

    CertificateUpdate certificateUpdate =
        CertificateUpdate.newBuilder()
            .setUpdatedName("updated-cert")
            .setMetadataUpdate(metadataUpdate)
            .build();

    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder()
            .setId(id)
            .setCertificateUpdate(certificateUpdate)
            .build();

    // Create existing certificate with metadata and status details
    CertificateMetadata existingMetadata = createValidMetadata();
    CertificateStatusDetails existingStatusDetails =
        CertificateStatusDetails.newBuilder()
            .setStatus(CertificateStatus.CERTIFICATE_STATUS_PENDING)
            .setExpirationTimestamp(
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
            .build();

    Certificate existingCertificate =
        Certificate.newBuilder()
            .setId(id)
            .setName("old-cert-name")
            .setMetadata(existingMetadata)
            .setStatusDetails(existingStatusDetails)
            .addStorage(createValidStorageDetails())
            .build();

    // Create expected updated metadata
    CertificateMetadata expectedUpdatedMetadata =
        CertificateMetadata.newBuilder()
            .addAllDomainNames(domainNames.getStringValuesList())
            .setKeyAlgorithm("RSA-2048")
            .putAllLabels(labelsMap.getMapMap())
            .build();

    // Create expected updated certificate
    Certificate expectedUpdatedCertificate =
        existingCertificate.toBuilder()
            .setName("updated-cert")
            .setMetadata(expectedUpdatedMetadata)
            .build();

    when(validator.validate(any(UpdateCertificateRequest.class))).thenReturn(Status.OK);
    when(store.getCertificate(requestContext, id)).thenReturn(existingCertificate);
    when(store.updateCertificate(requestContext, expectedUpdatedCertificate))
        .thenReturn(expectedUpdatedCertificate);

    Certificate result = manager.updateCertificate(requestContext, id, request);

    assertNotNull(result);
    assertEquals(id, result.getId());
    assertEquals("updated-cert", result.getName());
    assertEquals(domainNames.getStringValuesList(), result.getMetadata().getDomainNamesList());
    assertEquals("RSA-2048", result.getMetadata().getKeyAlgorithm());
    assertEquals(labelsMap.getMapMap(), result.getMetadata().getLabelsMap());

    verify(validator).validate(any(UpdateCertificateRequest.class));
    verify(store).getCertificate(requestContext, id);
    verify(store).updateCertificate(requestContext, expectedUpdatedCertificate);
  }

  @Test
  void testDeleteCertificate() {
    String id = "cert-123";
    Certificate existingCertificate =
        Certificate.newBuilder()
            .setId(id)
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .build();

    when(validator.validate(any(DeleteCertificateRequest.class))).thenReturn(Status.OK);
    when(store.getCertificate(requestContext, id)).thenReturn(existingCertificate);
    when(certificateUsageValidator.validateCertificateNotInUse(eq(requestContext), eq(id)))
        .thenReturn(Status.OK);

    manager.deleteCertificate(requestContext, id);

    verify(validator).validate(any(DeleteCertificateRequest.class));
    verify(store).getCertificate(requestContext, id);
    verify(store).deleteCertificate(requestContext, id);
    verify(certificateUsageValidator).validateCertificateNotInUse(eq(requestContext), eq(id));
  }

  @Test
  void testDeleteCertificate_CertificateNotFound() {
    String id = "non-existent-cert";

    when(validator.validate(any(DeleteCertificateRequest.class))).thenReturn(Status.OK);
    when(store.getCertificate(requestContext, id)).thenReturn(null);

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> manager.deleteCertificate(requestContext, id));

    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
    verify(validator).validate(any(DeleteCertificateRequest.class));
    verify(store).getCertificate(requestContext, id);
    verify(store, never()).deleteCertificate(any(), any());
    verify(certificateUsageValidator, never()).validateCertificateNotInUse(any(), any());
  }

  @Test
  void testDeleteCertificate_InUse() {
    String id = "cert-123";
    Certificate existingCertificate =
        Certificate.newBuilder()
            .setId(id)
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .build();

    when(validator.validate(any(DeleteCertificateRequest.class))).thenReturn(Status.OK);
    when(store.getCertificate(requestContext, id)).thenReturn(existingCertificate);
    when(certificateUsageValidator.validateCertificateNotInUse(eq(requestContext), eq(id)))
        .thenReturn(
            Status.FAILED_PRECONDITION.withDescription(
                "Certificate with ID "
                    + id
                    + " is in use by cloud-edge deployment deployment-123 and cannot be deleted"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class, () -> manager.deleteCertificate(requestContext, id));

    assertEquals(Status.Code.FAILED_PRECONDITION, exception.getStatus().getCode());
    verify(validator).validate(any(DeleteCertificateRequest.class));
    verify(store).getCertificate(requestContext, id);
    verify(store, never()).deleteCertificate(any(), any());
    verify(certificateUsageValidator).validateCertificateNotInUse(eq(requestContext), eq(id));
  }

  @Test
  void testCreateCertificate_AutoType() {
    CertificateMetadata metadata = createValidMetadata();
    CertificateStatusDetails statusDetails =
        CertificateStatusDetails.newBuilder()
            .setStatus(CertificateStatus.CERTIFICATE_STATUS_PENDING)
            .setExpirationTimestamp(
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
            .build();
    List<CertificateStorageDetails> storage =
        Collections.singletonList(createValidStorageDetails());

    when(validator.validate(any(CreateCertificateRequest.class))).thenReturn(Status.OK);
    when(uuidGenerator.generateRandomId()).thenReturn("cert-auto-123");

    Certificate expectedCertificate =
        Certificate.newBuilder()
            .setId("cert-auto-123")
            .setName("auto-certificate")
            .setMetadata(metadata)
            .setStatusDetails(statusDetails)
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_AUTO)
            .addAllStorage(storage)
            .build();

    when(store.createCertificate(eq(requestContext), any(Certificate.class)))
        .thenReturn(expectedCertificate);

    CreateCertificateRequest request =
        CreateCertificateRequest.newBuilder()
            .setName("auto-certificate")
            .setMetadata(metadata)
            .setStatusDetails(statusDetails)
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_AUTO)
            .addAllStorage(storage)
            .build();

    Certificate result = manager.createCertificate(requestContext, request);

    assertNotNull(result);
    assertEquals("cert-auto-123", result.getId());
    assertEquals(CertificateType.CERTIFICATE_TYPE_AUTO, result.getCertificateType());
    assertEquals(metadata, result.getMetadata());
    assertEquals(statusDetails, result.getStatusDetails());
    assertEquals(storage, result.getStorageList());

    verify(validator).validate(eq(request));
    verify(store).createCertificate(eq(requestContext), any(Certificate.class));
  }

  @Test
  void testGetCertificates_FilterByAutoType() {
    CertificateFilter filter =
        CertificateFilter.newBuilder()
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_AUTO)
            .build();

    Certificate autoCert1 =
        Certificate.newBuilder()
            .setId("cert-auto-1")
            .setName("Auto Cert 1")
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_AUTO)
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .build();

    Certificate autoCert2 =
        Certificate.newBuilder()
            .setId("cert-auto-2")
            .setName("Auto Cert 2")
            .setCertificateType(CertificateType.CERTIFICATE_TYPE_AUTO)
            .setMetadata(createValidMetadata())
            .addStorage(createValidStorageDetails())
            .build();

    List<Certificate> expectedCertificates = Arrays.asList(autoCert1, autoCert2);

    when(store.getCertificates(requestContext, filter)).thenReturn(expectedCertificates);

    List<Certificate> result = manager.getCertificates(requestContext, filter);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("cert-auto-1", result.get(0).getId());
    assertEquals("cert-auto-2", result.get(1).getId());
    assertEquals(CertificateType.CERTIFICATE_TYPE_AUTO, result.get(0).getCertificateType());
    assertEquals(CertificateType.CERTIFICATE_TYPE_AUTO, result.get(1).getCertificateType());

    verify(store).getCertificates(requestContext, filter);
  }

  private CertificateMetadata createValidMetadata() {
    return CertificateMetadata.newBuilder()
        .addDomainNames("example.com")
        .setKeyAlgorithm("RSA-2048")
        .putLabels("env", "dev")
        .putLabels("team", "security")
        .build();
  }

  private CertificateStorageDetails createValidStorageDetails() {
    return CertificateStorageDetails.newBuilder()
        .setAws(
            AwsStorageDetails.newBuilder()
                .setArn("arn:aws:acm:us-east-1:12:certificate/12-34-56-78")
                .setRegion("us-east-1"))
        .build();
  }
}
