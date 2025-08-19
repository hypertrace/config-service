package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaProviderDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaType;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigInput;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigServiceGrpc;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigServiceGrpc.CloudBotDeploymentConfigServiceBlockingStub;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.CreateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeleteCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentMode;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.EdgeDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.EnableCloudBotDeploymentRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.GetCloudBotDeploymentConfigsRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.MTCaptchaDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.OobDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.RotateApiTokenRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.SiteConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentStatusRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CloudBotDeploymentConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static CloudBotDeploymentConfigServiceBlockingStub cloudBotDeploymentConfigServiceStub;

  @BeforeAll
  static void init() {
    cloudBotDeploymentConfigServiceStub =
        CloudBotDeploymentConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void testCreateAndGetCloudBotDeploymentConfig() {
    // Create a cloud bot deployment config
    CloudBotDeploymentConfig createdConfig =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .createCloudBotDeploymentConfig(
                            CreateCloudBotDeploymentConfigRequest.newBuilder()
                                .setCloudBotDeploymentConfigInput(createConfigInput())
                                .build())
                        .getCloudBotDeployment());

    // Verify the created config
    assertNotNull(createdConfig.getId());
    assertEquals("Test Site", createdConfig.getSiteConfig().getSiteName());
    assertEquals(
        DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED,
        createdConfig.getCloudBotDeploymentStatus().getDeploymentStatus());

    // Get the created config
    List<CloudBotDeploymentConfig> configs =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .getCloudBotDeploymentConfigs(
                            GetCloudBotDeploymentConfigsRequest.newBuilder()
                                .addIds(createdConfig.getId())
                                .build())
                        .getCloudBotDeploymentsList());

    // Verify the retrieved config
    assertEquals(1, configs.size());
    CloudBotDeploymentConfig retrievedConfig = configs.get(0);
    assertEquals(createdConfig.getId(), retrievedConfig.getId());
    assertEquals(
        createdConfig.getSiteConfig().getSiteName(), retrievedConfig.getSiteConfig().getSiteName());
  }

  @Test
  public void testUpdateCloudBotDeploymentConfig() {
    // Create a cloud bot deployment config
    CloudBotDeploymentConfig createdConfig =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .createCloudBotDeploymentConfig(
                            CreateCloudBotDeploymentConfigRequest.newBuilder()
                                .setCloudBotDeploymentConfigInput(createConfigInput())
                                .build())
                        .getCloudBotDeployment());

    // Update the config
    CloudBotDeploymentConfigInput updatedInput =
        CloudBotDeploymentConfigInput.newBuilder()
            .setSiteConfig(
                createdConfig.getSiteConfig().toBuilder()
                    .setSiteName("Updated Test Site")
                    .setCaptchaConfig(
                        CaptchaConfig.newBuilder()
                            .setEnabled(true)
                            .setCaptchaType(CaptchaType.CAPTCHA_TYPE_VISUAL)))
            .setDeploymentDetails(
                DeploymentDetails.newBuilder()
                    .setEnvironment("production-2")
                    .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_EDGE)
                    .setEdgeDeploymentConfig(
                        EdgeDeploymentConfig.newBuilder().setCloudEdgeDeploymentId("abc")))
            .build();

    CloudBotDeploymentConfig updatedConfig =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .updateCloudBotDeploymentConfig(
                            UpdateCloudBotDeploymentConfigRequest.newBuilder()
                                .setId(createdConfig.getId())
                                .setCloudBotDeploymentConfigInput(updatedInput)
                                .build())
                        .getCloudBotDeployment());

    // Verify the updated config
    assertEquals(createdConfig.getId(), updatedConfig.getId());
    assertEquals("Updated Test Site", updatedConfig.getSiteConfig().getSiteName());

    // Verify that the site key and JWT signing details are preserved
    assertEquals(
        createdConfig.getSiteConfig().getSiteKey(), updatedConfig.getSiteConfig().getSiteKey());
    assertEquals(
        createdConfig.getSiteConfig().getJwtSigningDetails(),
        updatedConfig.getSiteConfig().getJwtSigningDetails());
    assertEquals(
        createdConfig.getSiteConfig().getDomainsList(),
        updatedConfig.getSiteConfig().getDomainsList());
  }

  @Test
  public void testUpdateCloudBotDeploymentStatus() {
    // Create a cloud bot deployment config
    CloudBotDeploymentConfig createdConfig =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .createCloudBotDeploymentConfig(
                            CreateCloudBotDeploymentConfigRequest.newBuilder()
                                .setCloudBotDeploymentConfigInput(createConfigInput())
                                .build())
                        .getCloudBotDeployment());

    // Update the deployment status to in progress first
    CloudBotDeploymentConfig statusUpdatedConfig_pre =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .updateCloudBotDeploymentStatus(
                            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                                .setId(createdConfig.getId())
                                .setCloudBotDeploymentStatus(
                                    CloudBotDeploymentStatus.newBuilder()
                                        .setDeploymentStatus(
                                            DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS))
                                .build())
                        .getCloudBotDeployment());

    // Update the deployment status to in deployed successfully
    CloudBotDeploymentConfig statusUpdatedConfig =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .updateCloudBotDeploymentStatus(
                            UpdateCloudBotDeploymentStatusRequest.newBuilder()
                                .setId(createdConfig.getId())
                                .setCloudBotDeploymentStatus(
                                    CloudBotDeploymentStatus.newBuilder()
                                        .setDeploymentStatus(
                                            DeploymentStatus
                                                .DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY))
                                .setCaptchaProviderDetails(
                                    CaptchaProviderDetails.newBuilder()
                                        .setMtCaptcha(
                                            MTCaptchaDetails.newBuilder()
                                                .setSiteKey("mt-captcha-key")))
                                .build())
                        .getCloudBotDeployment());

    // Verify the updated status
    assertEquals(createdConfig.getId(), statusUpdatedConfig.getId());
    assertEquals(
        DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY,
        statusUpdatedConfig.getCloudBotDeploymentStatus().getDeploymentStatus());
    assertEquals(
        "mt-captcha-key",
        statusUpdatedConfig
            .getSiteConfig()
            .getCaptchaProviderDetails()
            .getMtCaptcha()
            .getSiteKey());
  }

  @Test
  public void testDeleteCloudBotDeploymentConfig() {
    // Create a cloud bot deployment config
    CloudBotDeploymentConfig createdConfig =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .createCloudBotDeploymentConfig(
                            CreateCloudBotDeploymentConfigRequest.newBuilder()
                                .setCloudBotDeploymentConfigInput(createConfigInput())
                                .build())
                        .getCloudBotDeployment());

    // Delete the config
    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                cloudBotDeploymentConfigServiceStub.deleteCloudBotDeploymentConfig(
                    DeleteCloudBotDeploymentConfigRequest.newBuilder()
                        .setId(createdConfig.getId())
                        .build()));

    // Get all configs and verify the deleted config is not present
    List<CloudBotDeploymentConfig> allConfigs =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .getCloudBotDeploymentConfigs(
                            GetCloudBotDeploymentConfigsRequest.getDefaultInstance())
                        .getCloudBotDeploymentsList());

    boolean configFound =
        allConfigs.stream().anyMatch(config -> config.getId().equals(createdConfig.getId()));

    assertFalse(configFound, "Deleted config should not be present in the list of all configs");
  }

  @Test
  public void testRotateApiToken() {
    // Create a cloud bot deployment config
    DeploymentDetails deploymentDetails =
        DeploymentDetails.newBuilder()
            .setEnvironment("IB")
            .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND)
            .setOobDeploymentConfig(
                OobDeploymentConfig.newBuilder().setTraceableCaptchaDomain("captcha.traceable.ai"))
            .build();
    CloudBotDeploymentConfig createdConfig =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .createCloudBotDeploymentConfig(
                            CreateCloudBotDeploymentConfigRequest.newBuilder()
                                .setCloudBotDeploymentConfigInput(
                                    createConfigInput().toBuilder()
                                        .setDeploymentDetails(deploymentDetails))
                                .build())
                        .getCloudBotDeployment());

    // Store the original API token
    String originalToken =
        createdConfig.getDeploymentDetails().getOobDeploymentConfig().getApiToken().getKeyValue();

    // Rotate the API token
    CloudBotDeploymentConfig rotatedConfig =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .rotateApiToken(
                            RotateApiTokenRequest.newBuilder().setId(createdConfig.getId()).build())
                        .getCloudBotDeployment());

    // Verify the rotated token
    String newToken =
        rotatedConfig.getDeploymentDetails().getOobDeploymentConfig().getApiToken().getKeyValue();
    String previousToken =
        rotatedConfig
            .getDeploymentDetails()
            .getOobDeploymentConfig()
            .getPreviousApiToken()
            .getKeyValue();

    assertNotEquals(originalToken, newToken, "New token should be different from original token");
    assertEquals(originalToken, previousToken, "Previous token should match original token");

    // Verify the previous token has an expiry timestamp
    assertTrue(
        rotatedConfig
                .getDeploymentDetails()
                .getOobDeploymentConfig()
                .getPreviousApiToken()
                .getExpiryTimestamp()
                .getSeconds()
            > System.currentTimeMillis() / 1000,
        "Previous token should have an expiry timestamp");
  }

  @Test
  public void testEnableCloudBotDeployment() {
    // Create a cloud bot deployment config (initially disabled)
    CloudBotDeploymentConfig createdConfig =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .createCloudBotDeploymentConfig(
                            CreateCloudBotDeploymentConfigRequest.newBuilder()
                                .setCloudBotDeploymentConfigInput(createConfigInput())
                                .build())
                        .getCloudBotDeployment());

    assertTrue(createdConfig.getEnabled(), "Config should initially be enabled");

    // Enable the cloud bot deployment
    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                cloudBotDeploymentConfigServiceStub.enableCloudBotDeployment(
                    EnableCloudBotDeploymentRequest.newBuilder()
                        .setId(createdConfig.getId())
                        .setEnabled(false)
                        .build()));

    // Get the config and verify it's now enabled
    List<CloudBotDeploymentConfig> configs =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    cloudBotDeploymentConfigServiceStub
                        .getCloudBotDeploymentConfigs(
                            GetCloudBotDeploymentConfigsRequest.newBuilder()
                                .addIds(createdConfig.getId())
                                .build())
                        .getCloudBotDeploymentsList());

    assertEquals(1, configs.size());
    assertFalse(configs.get(0).getEnabled());
  }

  private CloudBotDeploymentConfigInput createConfigInput() {
    return CloudBotDeploymentConfigInput.newBuilder()
        .setEnabled(true)
        .setSiteConfig(
            SiteConfig.newBuilder()
                .setSiteName("Test Site")
                .addDomains("example.com")
                .setCaptchaConfig(
                    CaptchaConfig.newBuilder()
                        .setEnabled(true)
                        .setCaptchaType(CaptchaType.CAPTCHA_TYPE_VISUAL)))
        .setDeploymentDetails(
            DeploymentDetails.newBuilder()
                .setEnvironment("staging")
                .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_EDGE)
                .setEdgeDeploymentConfig(
                    EdgeDeploymentConfig.newBuilder().setCloudEdgeDeploymentId("xzy")))
        .build();
  }
}
