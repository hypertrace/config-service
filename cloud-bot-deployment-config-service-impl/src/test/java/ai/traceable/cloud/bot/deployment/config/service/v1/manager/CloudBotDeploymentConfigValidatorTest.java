package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaProviderDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaType;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigInput;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.ClusterStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.CreateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeleteCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentMode;
import ai.traceable.cloud.bot.deployment.config.service.v1.EdgeDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.MTCaptchaDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.SiteConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentStatusRequest;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CloudBotDeploymentConfigValidatorTest {

  private CloudBotDeploymentConfigValidator validator;

  @BeforeEach
  void setUp() {
    validator = new CloudBotDeploymentConfigValidator();
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_Valid() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(
                            SiteConfig.newBuilder()
                                .setSiteName("Test Site")
                                .setSiteKey("test-site-key")
                                .addDomains("example.com")
                                .setCaptchaConfig(
                                    CaptchaConfig.newBuilder()
                                        .setEnabled(true)
                                        .setCaptchaType(CaptchaType.CAPTCHA_TYPE_VISUAL)))
                        .setDeploymentDetails(
                            DeploymentDetails.newBuilder()
                                .setEnvironment("production")
                                .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_EDGE)
                                .setEdgeDeploymentConfig(
                                    EdgeDeploymentConfig.newBuilder()
                                        .setCloudEdgeDeploymentId("edge-123"))))
                .build());
    assertTrue(status.isOk(), "Valid request should pass validation");
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_MissingInput() {
    Status status = validator.validate(CreateCloudBotDeploymentConfigRequest.newBuilder().build());
    assertFalse(status.isOk(), "Missing input should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_EmptySiteName() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(
                            SiteConfig.newBuilder()
                                .setSiteKey("test-site-key")
                                .addDomains("example.com"))
                        .setDeploymentDetails(createValidDeploymentDetails()))
                .build());
    assertFalse(status.isOk(), "Empty site name should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_NoDomains() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(
                            SiteConfig.newBuilder()
                                .setSiteName("Test Site")
                                .setSiteKey("test-site-key"))
                        .setDeploymentDetails(createValidDeploymentDetails()))
                .build());
    assertFalse(status.isOk(), "No domains should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_InvalidDomain() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(
                            SiteConfig.newBuilder()
                                .setSiteName("Test Site")
                                .setSiteKey("test-site-key")
                                .addDomains("invalid-domain"))
                        .setDeploymentDetails(createValidDeploymentDetails()))
                .build());
    assertFalse(status.isOk(), "Invalid domain should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_WildcardDomain() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(
                            SiteConfig.newBuilder()
                                .setSiteName("Test Site")
                                .setSiteKey("test-site-key")
                                .addDomains("*.example.com"))
                        .setDeploymentDetails(createValidDeploymentDetails()))
                .build());
    assertTrue(status.isOk(), "Wildcard domain should pass validation");
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_EmptyEnvironment() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(createValidSiteConfig())
                        .setDeploymentDetails(
                            DeploymentDetails.newBuilder()
                                .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_EDGE)
                                .setEdgeDeploymentConfig(
                                    EdgeDeploymentConfig.newBuilder()
                                        .setCloudEdgeDeploymentId("edge-123"))))
                .build());
    assertFalse(status.isOk(), "Empty environment should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_UnspecifiedDeploymentMode() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(createValidSiteConfig())
                        .setDeploymentDetails(
                            DeploymentDetails.newBuilder().setEnvironment("production")))
                .build());
    assertFalse(status.isOk(), "Unspecified deployment mode should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_MissingEdgeConfig() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(createValidSiteConfig())
                        .setDeploymentDetails(
                            DeploymentDetails.newBuilder()
                                .setEnvironment("production")
                                .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_EDGE)))
                .build());
    assertFalse(status.isOk(), "Missing edge config should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_MissingOobConfig() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(createValidSiteConfig())
                        .setDeploymentDetails(
                            DeploymentDetails.newBuilder()
                                .setEnvironment("production")
                                .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND)))
                .build());
    assertFalse(status.isOk(), "Missing OOB config should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_EmptyEdgeDeploymentId() {
    Status status =
        validator.validate(
            CreateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(createValidSiteConfig())
                        .setDeploymentDetails(
                            DeploymentDetails.newBuilder()
                                .setEnvironment("production")
                                .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_EDGE)
                                .setEdgeDeploymentConfig(EdgeDeploymentConfig.newBuilder())))
                .build());
    assertFalse(status.isOk(), "Empty edge deployment ID should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCloudBotDeploymentConfigRequest_Valid() {
    Status status =
        validator.validate(
            UpdateCloudBotDeploymentConfigRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(createValidSiteConfig())
                        .setDeploymentDetails(createValidDeploymentDetails()))
                .build());
    assertTrue(status.isOk(), "Valid update request should pass validation");
  }

  @Test
  void testValidateUpdateCloudBotDeploymentConfigRequest_EmptyId() {
    Status status =
        validator.validate(
            UpdateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setSiteConfig(createValidSiteConfig())
                        .setDeploymentDetails(createValidDeploymentDetails()))
                .build());
    assertFalse(status.isOk(), "Empty ID should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_Valid() {
    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setClusterStatus(ClusterStatus.CLUSTER_STATUS_READY))
                .setCaptchaProviderDetails(
                    CaptchaProviderDetails.newBuilder()
                        .setMtCaptcha(MTCaptchaDetails.newBuilder().setSiteKey("mt-site-key")))
                .build());
    assertTrue(status.isOk(), "Valid status update request should pass validation");
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_EmptyId() {
    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setClusterStatus(ClusterStatus.CLUSTER_STATUS_READY))
                .build());
    assertFalse(status.isOk(), "Empty ID should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_UnspecifiedClusterStatus() {
    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setClusterStatus(ClusterStatus.CLUSTER_STATUS_UNSPECIFIED))
                .build());
    assertFalse(status.isOk(), "Unspecified cluster status should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_EmptyMtCaptchaSiteKey() {
    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setClusterStatus(ClusterStatus.CLUSTER_STATUS_READY))
                .setCaptchaProviderDetails(
                    CaptchaProviderDetails.newBuilder().setMtCaptcha(MTCaptchaDetails.newBuilder()))
                .build());
    assertFalse(status.isOk(), "Empty MTCaptcha site key should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateDeleteCloudBotDeploymentConfigRequest_Valid() {
    Status status =
        validator.validate(
            DeleteCloudBotDeploymentConfigRequest.newBuilder().setId("config-123").build());
    assertTrue(status.isOk(), "Valid delete request should pass validation");
  }

  @Test
  void testValidateDeleteCloudBotDeploymentConfigRequest_EmptyId() {
    Status status = validator.validate(DeleteCloudBotDeploymentConfigRequest.newBuilder().build());
    assertFalse(status.isOk(), "Empty ID should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  // Helper methods to create valid test objects
  private SiteConfig createValidSiteConfig() {
    return SiteConfig.newBuilder()
        .setSiteName("Test Site")
        .setSiteKey("test-site-key")
        .addDomains("example.com")
        .setCaptchaConfig(
            CaptchaConfig.newBuilder()
                .setEnabled(true)
                .setCaptchaType(CaptchaType.CAPTCHA_TYPE_VISUAL))
        .build();
  }

  private DeploymentDetails createValidDeploymentDetails() {
    return DeploymentDetails.newBuilder()
        .setEnvironment("production")
        .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_EDGE)
        .setEdgeDeploymentConfig(
            EdgeDeploymentConfig.newBuilder().setCloudEdgeDeploymentId("edge-123"))
        .build();
  }
}
