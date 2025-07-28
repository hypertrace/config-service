package ai.traceable.cloud.bot.deployment.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.bot.deployment.config.service.v1.ApiToken;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaProviderDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.CaptchaType;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigInput;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.ClusterStatus;
import ai.traceable.cloud.bot.deployment.config.service.v1.CreateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeleteCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentMode;
import ai.traceable.cloud.bot.deployment.config.service.v1.EdgeDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.IpWhitelistConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.JWTSigningKeyDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.MTCaptchaDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.OobDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.SiteConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentConfigRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.UpdateCloudBotDeploymentStatusRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.store.CloudBotDeploymentConfigStore;
import ai.traceable.config.utils.UuidGenerator;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CloudBotDeploymentConfigManagerImplTest {

  @Mock private CloudBotDeploymentConfigStore store;
  @Mock private CloudBotDeploymentConfigValidator validator;
  @Mock private UuidGenerator uuidGenerator;
  @Mock private KeyPairGenerator keyPairGenerator;
  @Mock private RequestContext requestContext;
  @Mock private Clock clock;

  private CloudBotDeploymentConfigManagerImpl manager;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    manager =
        new CloudBotDeploymentConfigManagerImpl(
            store, validator, uuidGenerator, keyPairGenerator, clock);
  }

  @Test
  void testCreateCloudBotDeploymentConfig() {
    SiteConfig siteConfig = createValidSiteConfig();

    // Create request
    CreateCloudBotDeploymentConfigRequest request =
        CreateCloudBotDeploymentConfigRequest.newBuilder()
            .setCloudBotDeploymentConfigInput(
                CloudBotDeploymentConfigInput.newBuilder()
                    .setEnabled(true)
                    .setSiteConfig(siteConfig)
                    .setDeploymentDetails(createValidOOBDeploymentDetails("random")))
            .build();

    when(validator.validate(eq(request))).thenReturn(Status.OK);

    // First call returns config ID, second call returns site key
    when(uuidGenerator.generateRandomId())
        .thenReturn("config-123")
        .thenReturn("site-key-456")
        .thenReturn("api-token-apple");

    JWTSigningKeyDetails mockJwtSign = mock(JWTSigningKeyDetails.class);
    when(keyPairGenerator.generateJwtSigningKeyPair()).thenReturn(mockJwtSign);
    when(clock.millis()).thenReturn(123456000L);

    // Create expected config with the site key set by the manager and status set to PROVISIONING
    CloudBotDeploymentConfig expectedConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setEnabled(true)
            .setId("config-123")
            .setSiteConfig(
                siteConfig.toBuilder().setSiteKey("site-key-456").setJwtSigningDetails(mockJwtSign))
            .setDeploymentDetails(createValidOOBDeploymentDetails("api-token-apple"))
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setClusterStatus(ClusterStatus.CLUSTER_STATUS_PROVISIONING))
            .setLastUpdatedTimestamp(Timestamp.newBuilder().setSeconds(123456))
            .build();

    when(store.createCloudBotDeploymentConfig(eq(requestContext), eq(expectedConfig)))
        .thenReturn(expectedConfig);

    CloudBotDeploymentConfig result =
        manager.createCloudBotDeploymentConfig(requestContext, request);

    assertEquals(result, expectedConfig);
    verify(uuidGenerator, times(3)).generateRandomId();
  }

  @Test
  void testCreateCloudBotDeploymentConfig_ValidationFails() {
    CloudBotDeploymentConfigInput input = CloudBotDeploymentConfigInput.newBuilder().build();
    CreateCloudBotDeploymentConfigRequest request =
        CreateCloudBotDeploymentConfigRequest.newBuilder()
            .setCloudBotDeploymentConfigInput(input)
            .build();

    when(validator.validate(eq(request)))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Validation failed"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.createCloudBotDeploymentConfig(requestContext, request));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
  }

  @Test
  void testUpdateCloudBotDeploymentConfig() {
    String id = "config-123";

    UpdateCloudBotDeploymentConfigRequest request =
        UpdateCloudBotDeploymentConfigRequest.newBuilder()
            .setId(id)
            .setCloudBotDeploymentConfigInput(
                CloudBotDeploymentConfigInput.newBuilder()
                    .setEnabled(false)
                    .setSiteConfig(
                        createValidSiteConfig().toBuilder()
                            .setSiteName("Apple-site")
                            .setCaptchaConfig(
                                CaptchaConfig.newBuilder()
                                    .setCaptchaType(CaptchaType.CAPTCHA_TYPE_PROOF_SHIELD))
                            .setIpWhitelistConfig(IpWhitelistConfig.getDefaultInstance()))
                    .setDeploymentDetails(createValidEdgeDeploymentDetails()))
            .build();

    // Create existing config with additional fields that should be preserved
    SiteConfig existingSiteConfig =
        createValidSiteConfig().toBuilder()
            .setCaptchaProviderDetails(
                CaptchaProviderDetails.newBuilder()
                    .setMtCaptcha(MTCaptchaDetails.newBuilder().setSiteKey("key")))
            .setIpWhitelistConfig(IpWhitelistConfig.newBuilder().addIpAddresses("1.2.3.4"))
            .build();

    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId(id)
            .setSiteConfig(existingSiteConfig)
            .setDeploymentDetails(
                createValidEdgeDeploymentDetails().toBuilder().setEnvironment("mango-env"))
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setClusterStatus(ClusterStatus.CLUSTER_STATUS_READY))
            .build();

    CloudBotDeploymentConfig expectedUpdatedConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId(id)
            .setEnabled(false)
            .setSiteConfig(
                existingSiteConfig.toBuilder()
                    .setSiteName("Apple-site")
                    .setCaptchaConfig(
                        CaptchaConfig.newBuilder()
                            .setCaptchaType(CaptchaType.CAPTCHA_TYPE_PROOF_SHIELD))
                    .setIpWhitelistConfig(IpWhitelistConfig.getDefaultInstance()))
            .setDeploymentDetails(createValidEdgeDeploymentDetails())
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setClusterStatus(ClusterStatus.CLUSTER_STATUS_READY))
            .setLastUpdatedTimestamp(Timestamp.newBuilder().setSeconds(123456))
            .build();

    when(validator.validate(eq(request), any(CloudBotDeploymentConfig.class)))
        .thenReturn(Status.OK);
    when(store.getCloudBotDeploymentConfig(requestContext, id)).thenReturn(existingConfig);
    when(store.updateCloudBotDeploymentConfig(eq(requestContext), eq(expectedUpdatedConfig)))
        .thenReturn(expectedUpdatedConfig);
    when(clock.millis()).thenReturn(123456000L);

    CloudBotDeploymentConfig result =
        manager.updateCloudBotDeploymentConfig(requestContext, id, request);

    // Verify the result has the expected values except for timestamp
    assertEquals(expectedUpdatedConfig.getId(), result.getId());
    assertEquals(expectedUpdatedConfig.getEnabled(), result.getEnabled());
    assertEquals(expectedUpdatedConfig.getSiteConfig(), result.getSiteConfig());
    assertEquals(expectedUpdatedConfig.getDeploymentDetails(), result.getDeploymentDetails());
    assertEquals(
        expectedUpdatedConfig.getCloudBotDeploymentStatus(), result.getCloudBotDeploymentStatus());
  }

  @Test
  void testUpdateCloudBotDeploymentConfig_NotFound() {
    String id = "non-existent-id";

    CloudBotDeploymentConfigInput input =
        CloudBotDeploymentConfigInput.newBuilder()
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidEdgeDeploymentDetails())
            .build();

    UpdateCloudBotDeploymentConfigRequest request =
        UpdateCloudBotDeploymentConfigRequest.newBuilder()
            .setId(id)
            .setCloudBotDeploymentConfigInput(input)
            .build();

    when(validator.validate(eq(request), any(CloudBotDeploymentConfig.class)))
        .thenReturn(Status.OK);
    when(store.getCloudBotDeploymentConfig(requestContext, id)).thenReturn(null);

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.updateCloudBotDeploymentConfig(requestContext, id, request));

    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
  }

  @Test
  void testUpdateCloudBotDeploymentStatus() {
    String id = "config-123";

    CloudBotDeploymentStatus status =
        CloudBotDeploymentStatus.newBuilder()
            .setClusterStatus(ClusterStatus.CLUSTER_STATUS_READY)
            .build();

    CaptchaProviderDetails captchaProviderDetails =
        CaptchaProviderDetails.newBuilder()
            .setMtCaptcha(MTCaptchaDetails.newBuilder().setSiteKey("mt-site-key"))
            .build();

    UpdateCloudBotDeploymentStatusRequest request =
        UpdateCloudBotDeploymentStatusRequest.newBuilder()
            .setId(id)
            .setCloudBotDeploymentStatus(status)
            .setCaptchaProviderDetails(captchaProviderDetails)
            .build();

    // Create existing config
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId(id)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidEdgeDeploymentDetails())
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setClusterStatus(ClusterStatus.CLUSTER_STATUS_PROVISIONING))
            .build();

    // Create expected updated config
    CloudBotDeploymentConfig expectedUpdatedConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId(id)
            .setSiteConfig(
                createValidSiteConfig().toBuilder()
                    .setCaptchaProviderDetails(captchaProviderDetails))
            .setDeploymentDetails(createValidEdgeDeploymentDetails())
            .setCloudBotDeploymentStatus(status)
            .build();

    when(validator.validate(eq(request), any(CloudBotDeploymentConfig.class)))
        .thenReturn(Status.OK);
    when(store.getCloudBotDeploymentConfig(requestContext, id)).thenReturn(existingConfig);
    when(store.updateCloudBotDeploymentConfig(eq(requestContext), eq(expectedUpdatedConfig)))
        .thenReturn(expectedUpdatedConfig);

    CloudBotDeploymentConfig result =
        manager.updateCloudBotDeploymentStatus(requestContext, id, request);

    assertEquals(result, expectedUpdatedConfig);
  }

  @Test
  void testDeleteCloudBotDeploymentConfig() {
    String id = "config-123";

    DeleteCloudBotDeploymentConfigRequest request =
        DeleteCloudBotDeploymentConfigRequest.newBuilder().setId(id).build();

    // Create existing config
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId(id)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidEdgeDeploymentDetails())
            .build();

    when(validator.validateId(id)).thenReturn(Status.OK);
    when(store.getCloudBotDeploymentConfig(requestContext, id)).thenReturn(existingConfig);

    manager.deleteCloudBotDeploymentConfig(requestContext, id);

    verify(store).getCloudBotDeploymentConfig(requestContext, id);
    verify(store).deleteCloudBotDeploymentConfig(requestContext, id);
  }

  @Test
  void testDeleteCloudBotDeploymentConfig_NotFound() {
    String id = "non-existent-id";

    when(validator.validateId(id)).thenReturn(Status.OK);
    when(store.getCloudBotDeploymentConfig(requestContext, id)).thenReturn(null);

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deleteCloudBotDeploymentConfig(requestContext, id));

    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
  }

  @Test
  void testGetCloudBotDeploymentConfigs() {
    List<String> ids = Arrays.asList("config-1", "config-2");

    // Create test configs
    CloudBotDeploymentConfig config1 =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-1")
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidEdgeDeploymentDetails())
            .build();

    CloudBotDeploymentConfig config2 =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-2")
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidEdgeDeploymentDetails())
            .build();

    when(store.getCloudBotDeploymentConfig(requestContext, "config-1")).thenReturn(config1);
    when(store.getCloudBotDeploymentConfig(requestContext, "config-2")).thenReturn(config2);

    List<CloudBotDeploymentConfig> result =
        manager.getCloudBotDeploymentConfigs(requestContext, ids);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("config-1", result.get(0).getId());
    assertEquals("config-2", result.get(1).getId());
  }

  @Test
  void testGetAllCloudBotDeploymentConfigs() {
    // Create test configs
    CloudBotDeploymentConfig config1 =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-1")
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidEdgeDeploymentDetails())
            .build();

    CloudBotDeploymentConfig config2 =
        CloudBotDeploymentConfig.newBuilder()
            .setId("config-2")
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(createValidEdgeDeploymentDetails())
            .build();

    List<CloudBotDeploymentConfig> expectedConfigs = Arrays.asList(config1, config2);

    when(store.getAllCloudBotDeploymentConfigs(requestContext)).thenReturn(expectedConfigs);

    // Test with empty list (should return all configs)
    List<CloudBotDeploymentConfig> result =
        manager.getCloudBotDeploymentConfigs(requestContext, Collections.emptyList());

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("config-1", result.get(0).getId());
    assertEquals("config-2", result.get(1).getId());
  }

  @Test
  void testRotateApiToken() {
    String id = "random-id";

    DeploymentDetails deploymentDetails = createValidOOBDeploymentDetails("originalToken");
    CloudBotDeploymentConfig existingConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId(id)
            .setSiteConfig(createValidSiteConfig())
            .setDeploymentDetails(deploymentDetails)
            .setCloudBotDeploymentStatus(
                CloudBotDeploymentStatus.newBuilder()
                    .setClusterStatus(ClusterStatus.CLUSTER_STATUS_READY))
            .build();

    when(validator.validateId(id)).thenReturn(Status.OK);

    // Mock the store to return the existing config
    when(store.getCloudBotDeploymentConfig(requestContext, id)).thenReturn(existingConfig);

    // Mock the UUID generator to return the new token
    when(uuidGenerator.generateRandomId()).thenReturn("newToken");

    // Mock the store to return the updated config
    when(store.updateCloudBotDeploymentConfig(
            eq(requestContext), any(CloudBotDeploymentConfig.class)))
        .thenAnswer(invocation -> invocation.getArgument(1));

    // Call the method under test
    CloudBotDeploymentConfig result = manager.rotateApiToken(requestContext, id);

    assertEquals(id, result.getId());
    assertEquals(
        "newToken",
        result.getDeploymentDetails().getOobDeploymentConfig().getApiToken().getKeyValue());
    assertEquals(
        "originalToken",
        result.getDeploymentDetails().getOobDeploymentConfig().getPreviousApiToken().getKeyValue());
    assertTrue(
        result
                .getDeploymentDetails()
                .getOobDeploymentConfig()
                .getPreviousApiToken()
                .getExpiryTimestampMillis()
            > System.currentTimeMillis());
    assertEquals("production", result.getDeploymentDetails().getEnvironment());
    assertEquals("Test Site", result.getSiteConfig().getSiteName());
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

  private DeploymentDetails createValidEdgeDeploymentDetails() {
    return DeploymentDetails.newBuilder()
        .setEnvironment("production")
        .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_EDGE)
        .setEdgeDeploymentConfig(
            EdgeDeploymentConfig.newBuilder().setCloudEdgeDeploymentId("edge-123"))
        .build();
  }

  private DeploymentDetails createValidOOBDeploymentDetails(String token) {
    return DeploymentDetails.newBuilder()
        .setEnvironment("production")
        .setDeploymentMode(DeploymentMode.DEPLOYMENT_MODE_OUT_OF_BAND)
        .setOobDeploymentConfig(
            OobDeploymentConfig.newBuilder()
                .setApiToken(ApiToken.newBuilder().setKeyValue(token))
                .setTraceableCaptchaDomain("captcha.traceable.ai"))
        .build();
  }
}
