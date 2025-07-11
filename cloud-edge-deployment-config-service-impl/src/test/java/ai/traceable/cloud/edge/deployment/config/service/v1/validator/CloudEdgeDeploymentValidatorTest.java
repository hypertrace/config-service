package ai.traceable.cloud.edge.deployment.config.service.v1.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.cloud.edge.deployment.config.service.v1.*;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CloudEdgeDeploymentValidatorTest {

  private CloudEdgeDeploymentValidator validator;

  @BeforeEach
  void setUp() {
    validator = new CloudEdgeDeploymentValidator();
  }

  @Test
  void testValidateCreateRequest_Valid() {
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("test-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    Status status = validator.validate(request);
    assertTrue(status.isOk());
  }

  @Test
  void testValidateCreateRequest_MissingPermission() {
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("test-service").build())
                    .build())
            .build();

    Status status = validator.validate(request);
    assertFalse(status.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    assertEquals("Config permission is required", status.getDescription());
  }

  @Test
  void testValidateUpdateRequest_Valid() {
    UpdateCloudEdgeDeploymentConfigRequest request =
        UpdateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId("test-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("test-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    Status status = validator.validate(request);
    assertTrue(status.isOk());
  }

  @Test
  void testValidateUpdateRequest_MissingId() {
    UpdateCloudEdgeDeploymentConfigRequest request =
        UpdateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("test-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    Status status = validator.validate(request);
    assertFalse(status.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    assertEquals("Cloud edge deployment config ID cannot be empty", status.getDescription());
  }

  @Test
  void testValidateDeleteRequest() {
    // Test case 1: Valid delete request
    DeleteCloudEdgeDeploymentConfigRequest validRequest =
        DeleteCloudEdgeDeploymentConfigRequest.newBuilder().setId("test-id").build();

    Status validStatus = validator.validate(validRequest);
    assertTrue(validStatus.isOk());
    assertEquals(Status.Code.OK, validStatus.getCode());

    // Test case 2: Invalid delete request with empty ID
    DeleteCloudEdgeDeploymentConfigRequest invalidRequest =
        DeleteCloudEdgeDeploymentConfigRequest.newBuilder().setId("").build();

    Status invalidStatus = validator.validate(invalidRequest);
    assertFalse(invalidStatus.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, invalidStatus.getCode());
    assertEquals("Cloud edge deployment config ID cannot be empty", invalidStatus.getDescription());
  }
}
