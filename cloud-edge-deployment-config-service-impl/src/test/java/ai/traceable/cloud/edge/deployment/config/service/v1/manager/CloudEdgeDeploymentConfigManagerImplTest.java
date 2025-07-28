package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentInputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ClusterConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigPermission;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeleteCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DomainConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsFilter;
import ai.traceable.cloud.edge.deployment.config.service.v1.OriginConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.UpdateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import ai.traceable.cloud.edge.deployment.config.service.v1.store.CloudEdgeDeploymentConfigStore;
import ai.traceable.cloud.edge.deployment.config.service.v1.validator.CloudEdgeDeploymentValidator;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CloudEdgeDeploymentConfigManagerImplTest {

  @Mock private CloudEdgeDeploymentConfigStore store;
  @Mock private CloudEdgeDeploymentValidator validator;
  @Mock private SharedConfigMetadataRegistry sharedConfigMetadataRegistry;
  @Mock private UuidGenerator uuidGenerator;
  @Mock private RequestContext requestContext;

  private CloudEdgeDeploymentConfigManagerImpl manager;

  @BeforeEach
  void setUp() {
    manager =
        new CloudEdgeDeploymentConfigManagerImpl(
            sharedConfigMetadataRegistry, store, validator, uuidGenerator);
  }

  @Test
  void testUpdateCloudEdgeDeploymentConfig() {
    // Setup
    String id = "test-id";
    UpdateCloudEdgeDeploymentConfigRequest request =
        UpdateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("updated-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("updated-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    // Test case 1: Validation fails
    when(validator.validate(request))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Invalid input"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.updateCloudEdgeDeploymentConfig(requestContext, id, request));
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    // Test case 2: Config not found
    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(null);

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.updateCloudEdgeDeploymentConfig(requestContext, id, request));
    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());

    // Test case 3: Successful update
    CloudEdgeDeploymentConfig existingConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("old-cluster").build())
                    .build())
            .build();

    CloudEdgeDeploymentConfig expectedUpdatedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(request.getCloudEdgeDeploymentInputConfig())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(existingConfig);
    when(store.upsertCloudEdgeDeploymentConfig(
            requestContext, expectedUpdatedConfig, request.getConfigPermission()))
        .thenReturn(expectedUpdatedConfig);

    CloudEdgeDeploymentConfig result =
        manager.updateCloudEdgeDeploymentConfig(requestContext, id, request);

    assertNotNull(result);
    assertEquals(id, result.getId());
    assertEquals(
        request.getCloudEdgeDeploymentInputConfig(), result.getCloudEdgeDeploymentInputConfig());
  }

  @Test
  void testDeleteCloudEdgeDeploymentConfig() {
    // Setup
    String id = "test-id";
    requestContext = RequestContext.forTenantId("tenant-id");
    DeleteCloudEdgeDeploymentConfigRequest expectedRequest =
        DeleteCloudEdgeDeploymentConfigRequest.newBuilder().setId(id).build();

    // Test case 1: Validation fails
    when(validator.validate(expectedRequest))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Invalid input"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deleteCloudEdgeDeploymentConfig(requestContext, id));
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    // Test case 2: Config not found
    when(validator.validate(expectedRequest)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(null);

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deleteCloudEdgeDeploymentConfig(requestContext, id));
    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());

    // Test case 3: Successful deletion
    CloudEdgeDeploymentConfig existingConfig =
        CloudEdgeDeploymentConfig.newBuilder().setId(id).build();

    when(validator.validate(expectedRequest)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(existingConfig);

    manager.deleteCloudEdgeDeploymentConfig(requestContext, id);

    verify(store).deleteCloudEdgeDeploymentConfig(requestContext, id);
  }

  @Test
  void testGetCloudEdgeDeploymentConfigs() {
    // Setup
    List<String> ids = Arrays.asList("id1", "id2");
    ConfigAccessType accessType = ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL;

    CloudEdgeDeploymentConfig config1 = CloudEdgeDeploymentConfig.newBuilder().setId("id1").build();
    CloudEdgeDeploymentConfig config2 = CloudEdgeDeploymentConfig.newBuilder().setId("id2").build();
    List<CloudEdgeDeploymentConfig> expectedConfigs = Arrays.asList(config1, config2);

    // Create filter with IDs
    GetCloudEdgeDeploymentConfigsFilter filter =
        GetCloudEdgeDeploymentConfigsFilter.newBuilder().addAllIds(ids).build();

    when(store.getCloudEdgeDeploymentConfigs(requestContext, filter, accessType))
        .thenReturn(expectedConfigs);

    // Execute and verify
    List<CloudEdgeDeploymentConfig> result =
        manager.getCloudEdgeDeploymentConfigs(requestContext, filter, accessType);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("id1", result.get(0).getId());
    assertEquals("id2", result.get(1).getId());
  }

  @Test
  void testGetCloudEdgeDeploymentConfigsFilteredByCertificateIds() {
    // Setup
    List<String> ids = Arrays.asList("id1", "id2", "id3");
    List<String> certificateIds = Arrays.asList("cert1", "cert2");
    ConfigAccessType accessType = ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL;

    // Create configs with different certificate usage patterns
    // Config1: Has cert1 in domain config
    CloudEdgeDeploymentConfig config1 =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("id1")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .addDomainConfigs(
                                DomainConfig.newBuilder().setCertificateId("cert1").build())
                            .build())
                    .build())
            .build();

    // Config2: Has cert2 in origin config
    CloudEdgeDeploymentConfig config2 =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("id2")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .addOriginConfigs(
                                OriginConfig.newBuilder().setCertificateId("cert2").build())
                            .build())
                    .build())
            .build();

    // Create filter with IDs and certificate IDs
    GetCloudEdgeDeploymentConfigsFilter filter =
        GetCloudEdgeDeploymentConfigsFilter.newBuilder()
            .addAllIds(ids)
            .addAllCertificateIds(certificateIds)
            .build();

    // Only configs with matching certificate IDs should be returned by the store
    List<CloudEdgeDeploymentConfig> expectedFilteredConfigs = Arrays.asList(config1, config2);

    when(store.getCloudEdgeDeploymentConfigs(requestContext, filter, accessType))
        .thenReturn(expectedFilteredConfigs);

    // Execute
    List<CloudEdgeDeploymentConfig> result =
        manager.getCloudEdgeDeploymentConfigs(requestContext, filter, accessType);

    // Verify that only configs with matching certificate IDs are returned
    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.stream().anyMatch(config -> config.getId().equals("id1")));
    assertTrue(result.stream().anyMatch(config -> config.getId().equals("id2")));
    assertTrue(result.stream().noneMatch(config -> config.getId().equals("id3")));
  }

  @Test
  void testGetCloudEdgeDeploymentConfigsWithEmptyCertificateIdsList() {
    // Setup
    List<String> ids = Arrays.asList("id1", "id2");
    ConfigAccessType accessType = ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL;

    CloudEdgeDeploymentConfig config1 = CloudEdgeDeploymentConfig.newBuilder().setId("id1").build();
    CloudEdgeDeploymentConfig config2 = CloudEdgeDeploymentConfig.newBuilder().setId("id2").build();
    List<CloudEdgeDeploymentConfig> expectedConfigs = Arrays.asList(config1, config2);

    // Create filter with IDs but no certificate IDs
    GetCloudEdgeDeploymentConfigsFilter filter =
        GetCloudEdgeDeploymentConfigsFilter.newBuilder().addAllIds(ids).build();

    when(store.getCloudEdgeDeploymentConfigs(requestContext, filter, accessType))
        .thenReturn(expectedConfigs);

    // Execute with filter that has no certificate IDs
    List<CloudEdgeDeploymentConfig> result =
        manager.getCloudEdgeDeploymentConfigs(requestContext, filter, accessType);

    // Verify that all configs are returned when certificate IDs list is empty
    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("id1", result.get(0).getId());
    assertEquals("id2", result.get(1).getId());
  }

  @Test
  void testGetSharedConfigMetadata() {
    // Setup
    ConfigAccessType accessType = ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL;

    SharedConfigMetadata expectedMetadata =
        SharedConfigMetadata.newBuilder()
            .putClusterConfigDetails(
                "name",
                ConfigValueDescriptor.newBuilder()
                    .setDescription("Cluster name")
                    .setDisplayName("Cluster Name")
                    .build())
            .build();

    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithReadPermission(accessType))
        .thenReturn(expectedMetadata);

    // Execute
    SharedConfigMetadata result = manager.getSharedConfigMetadata(requestContext, accessType);

    // Verify
    assertNotNull(result);
    assertEquals(1, result.getClusterConfigDetailsCount());
    assertTrue(result.getClusterConfigDetailsMap().containsKey("name"));
    assertEquals("Cluster Name", result.getClusterConfigDetailsMap().get("name").getDisplayName());
    assertEquals("Cluster name", result.getClusterConfigDetailsMap().get("name").getDescription());

    // Verify the store was called with the correct parameters
    verify(sharedConfigMetadataRegistry).getSharedConfigMetadataWithReadPermission(accessType);
  }
}
