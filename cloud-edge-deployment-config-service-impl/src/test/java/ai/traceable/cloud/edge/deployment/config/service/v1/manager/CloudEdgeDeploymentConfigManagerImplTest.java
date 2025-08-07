package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.edge.deployment.config.service.v1.Action;
import ai.traceable.cloud.edge.deployment.config.service.v1.CancelCloudEdgeDeploymentConfigActionRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentInputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentOutputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ClusterConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigPermission;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeleteCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeployCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.edge.deployment.config.service.v1.DomainConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsFilter;
import ai.traceable.cloud.edge.deployment.config.service.v1.HoldCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.OriginConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.RemoveCloudEdgeDeploymentConfigRequest;
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
    requestContext = RequestContext.forTenantId("tenant-id");
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

    // Test case 3: Successful update with deployment status change
    CloudEdgeDeploymentConfig existingConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("old-cluster").build())
                    .build())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    CloudEdgeDeploymentConfig expectedUpdatedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(request.getCloudEdgeDeploymentInputConfig())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED)
                    .build())
            .build();

    // Mock the validator to return CHANGE_REQUESTED status for the ACTION_EDIT action
    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(existingConfig);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY,
            ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE,
            Action.ACTION_EDIT))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED));

    // Mock the store upsert method
    when(store.upsertCloudEdgeDeploymentConfig(
            eq(requestContext),
            any(CloudEdgeDeploymentConfig.class),
            eq(request.getConfigPermission()),
            eq(DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED)))
        .thenReturn(expectedUpdatedConfig);

    CloudEdgeDeploymentConfig result =
        manager.updateCloudEdgeDeploymentConfig(requestContext, id, request);

    assertNotNull(result);
    assertEquals(id, result.getId());
    assertEquals(
        request.getCloudEdgeDeploymentInputConfig(), result.getCloudEdgeDeploymentInputConfig());

    // Verify that the validator and store methods were called with correct parameters
    verify(validator)
        .validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY,
            ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE,
            Action.ACTION_EDIT);
    verify(store)
        .upsertCloudEdgeDeploymentConfig(
            eq(requestContext),
            any(CloudEdgeDeploymentConfig.class),
            eq(request.getConfigPermission()),
            eq(DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED));
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

    // Test case 3: Action validation fails
    CloudEdgeDeploymentConfig existingConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    when(validator.validate(expectedRequest)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(existingConfig);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY,
            ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE,
            Action.ACTION_DELETE))
        .thenThrow(
            Status.PERMISSION_DENIED
                .withDescription("Delete operation not permitted")
                .asRuntimeException());

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deleteCloudEdgeDeploymentConfig(requestContext, id));
    assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    assertTrue(exception.getStatus().getDescription().contains("not permitted"));

    // Test case 4: Successful deletion
    reset(validator, store);
    CloudEdgeDeploymentConfig existingConfigSuccess =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD)
                    .build())
            .build();
    when(validator.validate(expectedRequest)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(existingConfigSuccess);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD,
            ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE,
            Action.ACTION_DELETE))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_UNSPECIFIED));

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
  void testCancelCloudEdgeDeploymentConfigAction() {
    // Setup
    String id = "test-id";
    requestContext = RequestContext.forTenantId("tenant-id");
    CancelCloudEdgeDeploymentConfigActionRequest request =
        CancelCloudEdgeDeploymentConfigActionRequest.newBuilder()
            .setId(id)
            .setAccessType(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
            .build();

    // Test case 1: Validation fails
    when(validator.validate(request))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Invalid input"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.cancelCloudEdgeDeploymentConfigAction(requestContext, request));
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    // Test case 2: Config not found
    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(null);

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.cancelCloudEdgeDeploymentConfigAction(requestContext, request));
    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());

    // Test case 3: Action not allowed by validator
    CloudEdgeDeploymentConfig configNotCancelable =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configNotCancelable);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY,
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
            Action.ACTION_CANCEL_REMOVAL_REQUEST))
        .thenThrow(
            Status.PERMISSION_DENIED
                .withDescription("This operation is not permitted")
                .asRuntimeException());

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.cancelCloudEdgeDeploymentConfigAction(requestContext, request));
    assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    assertTrue(exception.getStatus().getDescription().contains("not permitted"));

    // Test case 4a: Successful cancellation of removal request
    CloudEdgeDeploymentConfig configRemovalRequested =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_REMOVAL_REQUESTED)
                    .build())
            .build();

    CloudEdgeDeploymentConfig expectedUpdatedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configRemovalRequested);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_REMOVAL_REQUESTED,
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
            Action.ACTION_CANCEL_REMOVAL_REQUEST))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY));

    manager.cancelCloudEdgeDeploymentConfigAction(requestContext, request);

    verify(store).upsertCloudEdgeDeploymentConfig(requestContext, expectedUpdatedConfig);

    // Test case 4b: Successful cancellation of change request
    CloudEdgeDeploymentConfig configChangeRequested =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED)
                    .build())
            .build();

    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configChangeRequested);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED,
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
            Action.ACTION_CANCEL_CHANGE_REQUEST))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY));

    manager.cancelCloudEdgeDeploymentConfigAction(requestContext, request);

    verify(store, times(2)).upsertCloudEdgeDeploymentConfig(requestContext, expectedUpdatedConfig);
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

  @Test
  void testRemoveCloudEdgeDeploymentConfig() {
    // Setup
    String id = "test-id";
    requestContext = RequestContext.forTenantId("tenant-id");
    RemoveCloudEdgeDeploymentConfigRequest request =
        RemoveCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId(id)
            .setAccessType(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
            .build();

    // Test case 1: Validation fails
    when(validator.validate(request))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Invalid input"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.removeCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    // Test case 2: Config not found
    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(null);

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.removeCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());

    // Test case 3: Action not allowed by state transition registry
    CloudEdgeDeploymentConfig configNotRemovable =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configNotRemovable);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS,
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
            Action.ACTION_REQUEST_REMOVAL))
        .thenThrow(
            Status.PERMISSION_DENIED
                .withDescription("You are not allowed to perform this action")
                .asRuntimeException());

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.removeCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    assertTrue(
        exception.getStatus().getDescription().contains("not allowed to perform this action"));

    // Test case 4: Successful removal request
    CloudEdgeDeploymentConfig configRemovable =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configRemovable);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY,
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
            Action.ACTION_REQUEST_REMOVAL))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_REMOVAL_REQUESTED));

    CloudEdgeDeploymentConfig expectedUpdatedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_REMOVAL_REQUESTED)
                    .build())
            .build();

    manager.removeCloudEdgeDeploymentConfig(requestContext, request);

    verify(store).upsertCloudEdgeDeploymentConfig(requestContext, expectedUpdatedConfig);
  }

  @Test
  void testHoldCloudEdgeDeploymentConfig() {
    // Setup
    String id = "test-id";
    requestContext = RequestContext.forTenantId("tenant-id");
    HoldCloudEdgeDeploymentConfigRequest request =
        HoldCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId(id)
            .setAccessType(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
            .build();

    // Test case 1: Validation fails
    when(validator.validate(request))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Invalid input"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.holdCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    // Test case 2: Config not found
    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(null);

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.holdCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());

    // Test case 3: Action not allowed by state transition registry
    CloudEdgeDeploymentConfig configNotRemovable =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configNotRemovable);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS,
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
            Action.ACTION_HOLD))
        .thenThrow(
            Status.PERMISSION_DENIED
                .withDescription("This operation is not permitted")
                .asRuntimeException());

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.holdCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    assertTrue(exception.getStatus().getDescription().contains("not permitted"));

    // Test case 4: Successful removal request
    CloudEdgeDeploymentConfig configRemovable =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configRemovable);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED,
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
            Action.ACTION_HOLD))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD));

    CloudEdgeDeploymentConfig expectedUpdatedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD)
                    .build())
            .build();

    manager.holdCloudEdgeDeploymentConfig(requestContext, request);

    verify(store).upsertCloudEdgeDeploymentConfig(requestContext, expectedUpdatedConfig);
  }

  @Test
  void testDeployCloudEdgeDeploymentConfig() {
    // Setup
    String id = "test-id";
    requestContext = RequestContext.forTenantId("tenant-id");
    DeployCloudEdgeDeploymentConfigRequest request =
        DeployCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId(id)
            .setAccessType(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
            .build();

    // Test case 1: Validation fails
    when(validator.validate(request))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Invalid input"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deployCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    // Test case 2: Config not found
    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(null);

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deployCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());

    // Test case 3: Action not allowed by state transition registry
    CloudEdgeDeploymentConfig configNotRemovable =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configNotRemovable);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS,
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
            Action.ACTION_DEPLOY))
        .thenThrow(
            Status.PERMISSION_DENIED
                .withDescription("This operation is not permitted")
                .asRuntimeException());

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deployCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    assertTrue(exception.getStatus().getDescription().contains("not permitted"));

    // Test case 4: Successful removal request
    CloudEdgeDeploymentConfig configRemovable =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configRemovable);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD,
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
            Action.ACTION_DEPLOY))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED));

    CloudEdgeDeploymentConfig expectedUpdatedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED)
                    .build())
            .build();

    manager.deployCloudEdgeDeploymentConfig(requestContext, request);

    verify(store).upsertCloudEdgeDeploymentConfig(requestContext, expectedUpdatedConfig);
  }
}
