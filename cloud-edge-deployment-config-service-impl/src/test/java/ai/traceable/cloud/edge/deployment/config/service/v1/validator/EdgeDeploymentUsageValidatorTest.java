package ai.traceable.cloud.edge.deployment.config.service.v1.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigServiceGrpc.CloudBotDeploymentConfigServiceBlockingStub;
import ai.traceable.cloud.bot.deployment.config.service.v1.DeploymentDetails;
import ai.traceable.cloud.bot.deployment.config.service.v1.EdgeDeploymentConfig;
import ai.traceable.cloud.bot.deployment.config.service.v1.GetCloudBotDeploymentConfigsRequest;
import ai.traceable.cloud.bot.deployment.config.service.v1.GetCloudBotDeploymentConfigsResponse;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EdgeDeploymentUsageValidatorTest {

  private static final String EDGE_DEPLOYMENT_ID = "test-edge-deployment-id";
  private static final String BOT_CONFIG_ID = "test-bot-config-id";

  @Mock private CloudBotDeploymentConfigServiceBlockingStub botConfigService;
  private static final RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");

  private EdgeDeploymentUsageValidator validator;

  @BeforeEach
  void setUp() {
    validator = new EdgeDeploymentUsageValidator(botConfigService);
  }

  @Test
  void testValidateEdgeDeploymentNotInUse_WhenNotInUse_ShouldReturnOk() {
    // Setup
    when(botConfigService.getCloudBotDeploymentConfigs(
            GetCloudBotDeploymentConfigsRequest.newBuilder()
                .setEdgeDeploymentId(EDGE_DEPLOYMENT_ID)
                .build()))
        .thenReturn(GetCloudBotDeploymentConfigsResponse.getDefaultInstance());

    // Execute
    Status status = validator.validateEdgeDeploymentNotInUse(requestContext, EDGE_DEPLOYMENT_ID);

    // Verify
    assertTrue(status.isOk());
    assertEquals(Status.OK, status);
  }

  @Test
  void testValidateEdgeDeploymentInUse() {
    // Setup
    CloudBotDeploymentConfig botConfig =
        CloudBotDeploymentConfig.newBuilder()
            .setId(BOT_CONFIG_ID)
            .setDeploymentDetails(
                DeploymentDetails.newBuilder()
                    .setEdgeDeploymentConfig(
                        EdgeDeploymentConfig.newBuilder()
                            .setCloudEdgeDeploymentId(EDGE_DEPLOYMENT_ID)))
            .build();

    GetCloudBotDeploymentConfigsResponse response =
        GetCloudBotDeploymentConfigsResponse.newBuilder().addCloudBotDeployments(botConfig).build();

    when(botConfigService.getCloudBotDeploymentConfigs(
            GetCloudBotDeploymentConfigsRequest.newBuilder()
                .setEdgeDeploymentId(EDGE_DEPLOYMENT_ID)
                .build()))
        .thenReturn(response);

    // Execute
    Status status = validator.validateEdgeDeploymentNotInUse(requestContext, EDGE_DEPLOYMENT_ID);

    // Verify
    assertFalse(status.isOk());
    assertEquals(Status.Code.FAILED_PRECONDITION, status.getCode());
    Assertions.assertNotNull(status.getDescription());
    assertTrue(status.getDescription().contains(EDGE_DEPLOYMENT_ID));
    assertTrue(status.getDescription().contains("in use"));
    assertTrue(status.getDescription().contains(BOT_CONFIG_ID));
  }

  @Test
  void testValidateEdgeDeploymentNotInUse_WithMultipleConfigs() {
    // Setup
    CloudBotDeploymentConfig botConfig1 =
        CloudBotDeploymentConfig.newBuilder()
            .setId(BOT_CONFIG_ID)
            .setDeploymentDetails(
                DeploymentDetails.newBuilder()
                    .setEdgeDeploymentConfig(
                        EdgeDeploymentConfig.newBuilder()
                            .setCloudEdgeDeploymentId(EDGE_DEPLOYMENT_ID)))
            .build();

    CloudBotDeploymentConfig botConfig2 =
        CloudBotDeploymentConfig.newBuilder()
            .setId("other-bot-config")
            .setDeploymentDetails(
                DeploymentDetails.newBuilder()
                    .setEdgeDeploymentConfig(
                        EdgeDeploymentConfig.newBuilder()
                            .setCloudEdgeDeploymentId(EDGE_DEPLOYMENT_ID)))
            .build();

    GetCloudBotDeploymentConfigsResponse response =
        GetCloudBotDeploymentConfigsResponse.newBuilder()
            .addCloudBotDeployments(botConfig1)
            .addCloudBotDeployments(botConfig2)
            .build();

    when(botConfigService.getCloudBotDeploymentConfigs(
            GetCloudBotDeploymentConfigsRequest.newBuilder()
                .setEdgeDeploymentId(EDGE_DEPLOYMENT_ID)
                .build()))
        .thenReturn(response);

    // Execute
    Status status = validator.validateEdgeDeploymentNotInUse(requestContext, EDGE_DEPLOYMENT_ID);

    // Verify
    assertFalse(status.isOk());
    assertEquals(Status.Code.FAILED_PRECONDITION, status.getCode());
    // Description should not be null when status is not OK
    String description = status.getDescription();
    Assertions.assertNotNull(description);
    assertTrue(description.contains(BOT_CONFIG_ID));
    assertTrue(description.contains("other-bot-config"));
  }
}
