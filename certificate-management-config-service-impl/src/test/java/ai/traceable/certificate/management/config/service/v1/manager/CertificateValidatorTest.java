package ai.traceable.certificate.management.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.certificate.management.config.service.v1.AwsStorageDetails;
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
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CertificateValidatorTest {

  private CertificateValidator validator;

  @BeforeEach
  void setUp() {
    validator = new CertificateValidator();
  }

  @Test
  void testValidateCreateCertificateRequest_Valid() {
    Status status =
        validator.validate(
            CreateCertificateRequest.newBuilder()
                .setMetadata(
                    CertificateMetadata.newBuilder()
                        .addDomainNames("example.com")
                        .setKeyAlgorithm("RSA-2048")
                        .putLabels("env", "dev")
                        .putLabels("team", "security"))
                .setStatusDetails(
                    CertificateStatusDetails.newBuilder()
                        .setStatus(CertificateStatus.CERTIFICATE_STATUS_PENDING)
                        .setExpirationTimestamp(
                            Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000)))
                .addStorage(createValidStorageDetails())
                .setName("test-cert")
                .build());
    assertTrue(status.isOk(), "Valid request should pass validation");
  }

  @Test
  void testValidateCreateCertificateRequest_EmptyName() {
    CreateCertificateRequest request =
        CreateCertificateRequest.newBuilder()
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("example.com"))
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
                    .setExpirationTimestamp(
                        Timestamp.newBuilder()
                            .setSeconds(System.currentTimeMillis() / 1000)
                            .build()))
            .addStorage(createValidStorageDetails())
            .build();
    Status status = validator.validate(request);
    assertFalse(status.isOk(), "Empty name should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCertificateRequest_NoDomainNames() {
    CreateCertificateRequest request =
        CreateCertificateRequest.newBuilder()
            .setName("test-cert")
            .setMetadata(CertificateMetadata.newBuilder())
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
                    .setExpirationTimestamp(
                        Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000)))
            .addStorage(createValidStorageDetails())
            .build();
    Status status = validator.validate(request);
    assertFalse(status.isOk(), "No domain names should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCertificateRequest_InvalidDomainName() {
    CreateCertificateRequest request =
        CreateCertificateRequest.newBuilder()
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("invalid-domain"))
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
                    .setExpirationTimestamp(
                        Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000)))
            .addStorage(createValidStorageDetails())
            .setName("test-cert")
            .build();
    Status status = validator.validate(request);
    assertFalse(status.isOk(), "Invalid domain name should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCertificateRequest_UnspecifiedStatus() {
    CreateCertificateRequest request =
        CreateCertificateRequest.newBuilder()
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("example.com"))
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_UNSPECIFIED)
                    .setExpirationTimestamp(
                        Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000)))
            .addStorage(createValidStorageDetails())
            .setName("test-cert")
            .build();
    Status status = validator.validate(request);
    assertFalse(status.isOk(), "Unspecified status should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCertificateRequest_NoStorage() {
    CreateCertificateRequest request =
        CreateCertificateRequest.newBuilder()
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("example.com"))
            .setStatusDetails(
                CertificateStatusDetails.newBuilder()
                    .setStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
                    .setExpirationTimestamp(
                        Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000)))
            .setName("test-cert")
            .build();
    Status status = validator.validate(request);
    assertFalse(status.isOk(), "No storage should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCertificateRequest_InvalidAwsStorage() {
    CreateCertificateRequest request =
        CreateCertificateRequest.newBuilder()
            .setMetadata(CertificateMetadata.newBuilder().addDomainNames("example.com"))
            .addStorage(
                CertificateStorageDetails.newBuilder().setAws(AwsStorageDetails.newBuilder()))
            .setName("test-cert")
            .build();
    Status status = validator.validate(request);
    assertFalse(status.isOk(), "Invalid AWS storage should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCertificateRequest_Valid() {
    Timestamp expirationTimestamp =
        Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();

    CertificateUpdate certificateUpdate =
        CertificateUpdate.newBuilder()
            .setUpdatedStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
            .setUpdatedExpirationTimestamp(expirationTimestamp)
            .build();

    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder()
            .setId("cert-123")
            .setCertificateUpdate(certificateUpdate)
            .build();
    Status status = validator.validate(request);
    assertTrue(status.isOk(), "Valid update request should pass validation");
  }

  @Test
  void testValidateUpdateCertificateRequest_ValidCertificateUpdate() {
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
            .setUpdatedExpirationTimestamp(
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build())
            .setUpdatedStatus(CertificateStatus.CERTIFICATE_STATUS_ACTIVE)
            .build();

    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder()
            .setId("cert-123")
            .setCertificateUpdate(certificateUpdate)
            .build();

    Status status = validator.validate(request);
    assertTrue(status.isOk(), "Valid certificate update request should pass validation");
  }

  @Test
  void testValidateUpdateCertificateRequest_EmptyDomainNames() {
    // Create an empty domain names list
    StringList emptyDomainNames = StringList.newBuilder().build();

    CertificateMetadataUpdate metadataUpdate =
        CertificateMetadataUpdate.newBuilder().setUpdatedDomainNames(emptyDomainNames).build();

    CertificateUpdate certificateUpdate =
        CertificateUpdate.newBuilder()
            .setUpdatedName("updated-cert")
            .setMetadataUpdate(metadataUpdate)
            .build();

    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder()
            .setId("cert-123")
            .setCertificateUpdate(certificateUpdate)
            .build();

    Status status = validator.validate(request);
    assertFalse(status.isOk(), "Empty domain names list should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCertificateRequest_InvalidDomainName() {
    // Create domain names list with an invalid domain
    StringList invalidDomainNames =
        StringList.newBuilder().addStringValues("invalid-domain").build();

    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder()
            .setId("cert-123")
            .setCertificateUpdate(
                CertificateUpdate.newBuilder()
                    .setUpdatedName("updated-cert")
                    .setMetadataUpdate(
                        CertificateMetadataUpdate.newBuilder()
                            .setUpdatedDomainNames(invalidDomainNames)))
            .build();

    Status status = validator.validate(request);
    assertFalse(status.isOk(), "Invalid domain name should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCertificateRequest_UnspecifiedStatus() {
    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder()
            .setId("cert-123")
            .setCertificateUpdate(
                CertificateUpdate.newBuilder()
                    .setUpdatedStatus(CertificateStatus.CERTIFICATE_STATUS_UNSPECIFIED))
            .build();
    Status status = validator.validate(request);
    assertFalse(status.isOk(), "UNSPECIFIED status should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCertificateRequest_NoUpdateFields() {
    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder().setId("cert-123").build();
    Status status = validator.validate(request);
    assertFalse(status.isOk(), "Request with no certificate_update should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCertificateRequest_ValidMetadata() {
    UpdateCertificateRequest request =
        UpdateCertificateRequest.newBuilder()
            .setId("cert-123")
            .setCertificateUpdate(
                CertificateUpdate.newBuilder()
                    .setUpdatedName("updated-cert")
                    .setMetadataUpdate(
                        CertificateMetadataUpdate.newBuilder()
                            .setUpdatedKeyAlgorithm("aes-2048")
                            .setUpdatedDomainNames(
                                StringList.newBuilder().addStringValues("example.com"))))
            .build();
    Status status = validator.validate(request);
    assertTrue(status.isOk(), "Valid metadata update should pass validation");
  }

  @Test
  void testValidateDeleteCertificateRequest_Valid() {
    DeleteCertificateRequest request =
        DeleteCertificateRequest.newBuilder().setId("cert-123").build();
    Status status = validator.validate(request);
    assertTrue(status.isOk(), "Valid delete request should pass validation");
  }

  @Test
  void testValidateDeleteCertificateRequest_EmptyId() {
    DeleteCertificateRequest request = DeleteCertificateRequest.newBuilder().build();
    Status status = validator.validate(request);
    assertFalse(status.isOk(), "Empty ID should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  private CertificateStorageDetails createValidStorageDetails() {
    return CertificateStorageDetails.newBuilder()
        .setAws(
            AwsStorageDetails.newBuilder()
                .setArn("arn:aws:acm:us-east-1:12:certificate/12-34-56-78")
                .setRegion("us-east-1"))
        .build();
  }

  @Test
  void testValidateCreateCertificateRequest_AutoType() {
    // AUTO certificates with all required fields
    Status status =
        validator.validate(
            CreateCertificateRequest.newBuilder()
                .setName("auto-cert")
                .setCertificateType(CertificateType.CERTIFICATE_TYPE_AUTO)
                .setMetadata(
                    CertificateMetadata.newBuilder()
                        .addDomainNames("api.example.com")
                        .addDomainNames("www.example.com")
                        .setKeyAlgorithm("RSA_2048")
                        .putLabels("env", "prod"))
                .setStatusDetails(
                    CertificateStatusDetails.newBuilder()
                        .setStatus(CertificateStatus.CERTIFICATE_STATUS_PENDING)
                        .setExpirationTimestamp(
                            Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000)))
                .addStorage(createValidStorageDetails())
                .build());
    assertTrue(status.isOk(), "AUTO certificate should pass validation");
  }

  @Test
  void testValidateCreateCertificateRequest_AutoType_MultipleDomains() {
    // AUTO certificates can have multiple domains (CN + SANs)
    Status status =
        validator.validate(
            CreateCertificateRequest.newBuilder()
                .setName("auto-cert-multi")
                .setCertificateType(CertificateType.CERTIFICATE_TYPE_AUTO)
                .setMetadata(
                    CertificateMetadata.newBuilder()
                        .addDomainNames("api.example.com")
                        .addDomainNames("www.example.com")
                        .addDomainNames("app.example.com")
                        .setKeyAlgorithm("RSA_2048"))
                .setStatusDetails(
                    CertificateStatusDetails.newBuilder()
                        .setStatus(CertificateStatus.CERTIFICATE_STATUS_PENDING)
                        .setExpirationTimestamp(
                            Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000)))
                .addStorage(createValidStorageDetails())
                .build());
    assertTrue(status.isOk(), "AUTO certificate with multiple domains should pass validation");
  }
}
