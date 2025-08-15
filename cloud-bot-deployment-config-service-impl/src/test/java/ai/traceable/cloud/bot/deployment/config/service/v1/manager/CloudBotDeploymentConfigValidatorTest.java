package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaProviderDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaType;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigInput;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.CreateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentMode;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.EdgeDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.IpWhitelistConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.MTCaptchaDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.OobDeploymentConfig;
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
                        .setEnabled(true)
                        .setSiteConfig(
                            SiteConfig.newBuilder()
                                .setSiteName("Test Site")
                                .setSiteKey("test-site-key")
                                .addDomains("agent.traceableai.com")
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
    // Create an existing config with the same deployment mode
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-123")
            .setEnabled(true)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidDeploymentDetails())
            .build();

    Status status =
        validator.validate(
            UpdateCloudBotDeploymentConfigRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setEnabled(true)
                        .setSiteConfig(createValidSiteConfig())
                        .setDeploymentDetails(createValidDeploymentDetails()))
                .build(),
            existingConfig);
    assertTrue(status.isOk(), "Valid update request should pass validation");
  }

  @Test
  void testValidateUpdateCloudBotDeploymentConfigRequest_EmptyId() {
    // Create an existing config
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-123")
            .setEnabled(true)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidDeploymentDetails())
            .build();

    Status status =
        validator.validate(
            UpdateCloudBotDeploymentConfigRequest.newBuilder()
                .setCloudBotDeploymentConfigInput(
                    CloudBotDeploymentConfigInput.newBuilder()
                        .setEnabled(true)
                        .setSiteConfig(createValidSiteConfig())
                        .setDeploymentDetails(createValidDeploymentDetails()))
                .build(),
            existingConfig);
    assertFalse(status.isOk(), "Empty ID should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_Valid() {
    // Create an existing config with edge deployment mode
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-123")
            .setEnabled(true)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidDeploymentDetails())
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED))
            .build();

    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setDeploymentStatus(
                            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY))
                .setCaptchaProviderDetails(
                    CaptchaProviderDetails.newBuilder()
                        .setMtCaptcha(MTCaptchaDetails.newBuilder().setSiteKey("mt-site-key")))
                .setTraceableCaptchaDomain("captcha.traceable.ai")
                .build(),
            existingConfig);
    assertTrue(status.isOk(), "Valid status update request should pass validation");
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_Unspecified() {
    // Create an existing config with edge deployment mode
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-123")
            .setEnabled(true)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidDeploymentDetails())
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED))
            .build();

    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_UNSPECIFIED))
                .build(),
            existingConfig);
    assertFalse(status.isOk(), "Unspecified deployment status should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_BlockedWithoutMessage() {
    // Create an existing config with edge deployment mode
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-123")
            .setEnabled(true)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidDeploymentDetails())
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED))
            .build();

    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_BLOCKED))
                .build(),
            existingConfig);
    assertFalse(status.isOk(), "Blocked status without message should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_BlockedWithMessage() {
    // Create an existing config with edge deployment mode
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-123")
            .setEnabled(true)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidDeploymentDetails())
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED))
            .build();

    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_BLOCKED)
                        .setMessage("Blocked due to security concerns"))
                .build(),
            existingConfig);
    assertTrue(status.isOk(), "Blocked status with message should pass validation");
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_EmptyMtCaptchaSiteKey() {
    // Create an existing config with edge deployment mode
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-123")
            .setEnabled(true)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidDeploymentDetails())
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED))
            .build();

    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setDeploymentStatus(
                            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY))
                .setCaptchaProviderDetails(
                    CaptchaProviderDetails.newBuilder().setMtCaptcha(MTCaptchaDetails.newBuilder()))
                .build(),
            existingConfig);
    assertFalse(status.isOk(), "Empty MTCaptcha site key should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }

  @Test
  void testValidateIdCloudBotDeploymentConfigRequest_Valid() {
    assertTrue(validator.validateId("config-123").isOk());
    assertFalse(validator.validateId("").isOk());
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_MissingTraceableCaptchaDomain() {
    // Create an existing config with OOB deployment mode
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-123")
            .setEnabled(true)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(
                DeploymentDetails.newBuilder()
                    .setEnvironment("production")
                    .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND)
                    .setOobDeploymentConfig(OobDeploymentConfig.newBuilder()))
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED))
            .build();

    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setDeploymentStatus(
                            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY))
                .build(),
            existingConfig);
    assertFalse(
        status.isOk(),
        "Missing traceable captcha domain for OOB deployment should fail validation");
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

  private DeploymentDetails createValidOobDeploymentDetails() {
    return DeploymentDetails.newBuilder()
        .setEnvironment("production")
        .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND)
        .setOobDeploymentConfig(
            OobDeploymentConfig.newBuilder().setTraceableCaptchaDomain("captcha.traceable.ai"))
        .build();
  }

  @Test
  void testValidateUpdateCloudBotDeploymentStatusRequest_WithOobDeploymentAndCaptchaDomain() {
    // Create an existing config with OOB deployment mode
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-123")
            .setEnabled(true)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidOobDeploymentDetails())
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setDeploymentStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED))
            .build();

    Status status =
        validator.validate(
            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                .setId("config-123")
                .setCloudBotDeploymentStatus(
                    CloudBotDeploymentStatus.newBuilder()
                        .setDeploymentStatus(
                            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY))
                .setTraceableCaptchaDomain("captcha.traceable.ai")
                .build(),
            existingConfig);
    assertTrue(
        status.isOk(), "Valid status update with traceable captcha domain should pass validation");
  }

  @Test
  void testValidateCreateCloudBotDeploymentConfigRequest_InvalidIpAddress() {
    // Create a site config with invalid IP address in the whitelist
    SiteConfig siteConfig =
        SiteConfig.newBuilder()
            .setSiteName("Test Site")
            .setSiteKey("test-site-key")
            .addDomains("example.com")
            .setCaptchaConfig(
                CaptchaConfig.newBuilder()
                    .setEnabled(true)
                    .setCaptchaType(CaptchaType.CAPTCHA_TYPE_VISUAL))
            .setIpWhitelistConfig(
                IpWhitelistConfig.newBuilder()
                    .addIpAddresses("300.168.1.1") // Invalid IP address
                    .build())
            .build();

    // Create a request with the invalid site config
    CreateCloudBotDeploymentConfigRequest request =
        CreateCloudBotDeploymentConfigRequest.newBuilder()
            .setCloudBotDeploymentConfigInput(
                CloudBotDeploymentConfigInput.newBuilder()
                    .setEnabled(true)
                    .setSiteConfig(siteConfig)
                    .setDeploymentDetails(createValidDeploymentDetails()))
            .build();

    Status status = validator.validate(request);
    assertFalse(status.isOk(), "Invalid IP address should fail validation");
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }
}
