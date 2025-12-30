package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.edge.deployment.config.service.v1.Action;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigActionRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigWithActions;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentInputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentOutputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ClusterConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigPermission;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import ai.traceable.cloud.edge.deployment.config.service.v1.CreateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeleteCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentType;
import ai.traceable.cloud.edge.deployment.config.service.v1.DomainConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsFilter;
import ai.traceable.cloud.edge.deployment.config.service.v1.GetCloudEdgeDeploymentConfigsResponse;
import ai.traceable.cloud.edge.deployment.config.service.v1.OriginConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.UpdateCloudEdgeDeploymentConfigRequest;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import ai.traceable.cloud.edge.deployment.config.service.v1.state.transitions.StateTransitionsRegistry;
import ai.traceable.cloud.edge.deployment.config.service.v1.store.CloudEdgeDeploymentConfigStore;
import ai.traceable.cloud.edge.deployment.config.service.v1.validator.CloudEdgeDeploymentValidator;
import ai.traceable.cloud.edge.deployment.config.service.v1.validator.EdgeDeploymentUsageValidator;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  @Mock private StateTransitionsRegistry stateTransitionsRegistry;
  @Mock private EdgeDeploymentUsageValidator edgeDeploymentUsageValidator;

  private CloudEdgeDeploymentConfigManagerImpl manager;

  @BeforeEach
  void setUp() {
    manager =
        new CloudEdgeDeploymentConfigManagerImpl(
            sharedConfigMetadataRegistry,
            store,
            validator,
            uuidGenerator,
            stateTransitionsRegistry,
            edgeDeploymentUsageValidator);
    requestContext = RequestContext.forTenantId("tenant-id");
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
            eq(request.getConfigPermission())))
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
            eq(request.getConfigPermission()));
  }

  @Test
  void testDeleteCloudEdgeDeploymentConfig() {
    // Setup
    String id = "test-id";
    requestContext = RequestContext.forTenantId("tenant-id");
    DeleteCloudEdgeDeploymentConfigRequest request =
        DeleteCloudEdgeDeploymentConfigRequest.newBuilder().setId(id).build();

    // Test case 1: Validation fails
    when(validator.validate(request))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Invalid input"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deleteCloudEdgeDeploymentConfig(requestContext, request));
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    // Test case 2: Config not found
    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(null);

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deleteCloudEdgeDeploymentConfig(requestContext, request));
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

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(existingConfig);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY, Action.ACTION_DELETE))
        .thenThrow(
            Status.PERMISSION_DENIED
                .withDescription("Delete operation not permitted")
                .asRuntimeException());

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.deleteCloudEdgeDeploymentConfig(requestContext, request));
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
    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(existingConfigSuccess);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD, Action.ACTION_DELETE))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_UNSPECIFIED));

    {
      // Throw error if bot deployment is using edge
      when(edgeDeploymentUsageValidator.validateEdgeDeploymentNotInUse(requestContext, id))
          .thenReturn(Status.FAILED_PRECONDITION);
      assertThrows(
          StatusRuntimeException.class,
          () -> manager.deleteCloudEdgeDeploymentConfig(requestContext, request));
      verify(store, never()).deleteCloudEdgeDeploymentConfig(requestContext, id);
    }

    {
      // If no bot deployment is using edge allow deletion
      when(edgeDeploymentUsageValidator.validateEdgeDeploymentNotInUse(requestContext, id))
          .thenReturn(Status.OK);
      manager.deleteCloudEdgeDeploymentConfig(requestContext, request);
      verify(store).deleteCloudEdgeDeploymentConfig(requestContext, id);
    }
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

  @Test
  void testGetCloudEdgeDeploymentConfigsWithActions() {
    // Setup
    List<String> ids = Arrays.asList("id1", "id2");
    ConfigAccessType accessType = ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL;

    // Create configs with output configs containing status
    CloudEdgeDeploymentOutputConfig outputConfig1 =
        CloudEdgeDeploymentOutputConfig.newBuilder()
            .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
            .build();
    CloudEdgeDeploymentOutputConfig outputConfig2 =
        CloudEdgeDeploymentOutputConfig.newBuilder()
            .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS)
            .build();

    CloudEdgeDeploymentConfig config1 =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("id1")
            .setCloudEdgeDeployedOutputConfig(outputConfig1)
            .build();
    CloudEdgeDeploymentConfig config2 =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("id2")
            .setCloudEdgeDeployedOutputConfig(outputConfig2)
            .build();
    List<CloudEdgeDeploymentConfig> configs = Arrays.asList(config1, config2);

    // Create filter with IDs
    GetCloudEdgeDeploymentConfigsFilter filter =
        GetCloudEdgeDeploymentConfigsFilter.newBuilder().addAllIds(ids).build();

    // Mock store to return configs
    when(store.getCloudEdgeDeploymentConfigs(requestContext, filter, accessType))
        .thenReturn(configs);

    // Mock state transitions registry to return actions map
    Map<Action, List<DeploymentStatus>> actionsMap1 = new HashMap<>();
    actionsMap1.put(
        Action.ACTION_EDIT, Arrays.asList(DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED));
    actionsMap1.put(
        Action.ACTION_UPDATE_STATUS,
        Arrays.asList(
            DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED,
            DeploymentStatus.DEPLOYMENT_STATUS_REMOVAL_REQUESTED));

    Map<Action, List<DeploymentStatus>> actionsMap2 = new HashMap<>();
    actionsMap2.put(Action.ACTION_HOLD, Arrays.asList(DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD));

    when(stateTransitionsRegistry.getActionsMap(
            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY, accessType))
        .thenReturn(actionsMap1);
    when(stateTransitionsRegistry.getActionsMap(
            DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS, accessType))
        .thenReturn(actionsMap2);

    // Execute
    GetCloudEdgeDeploymentConfigsResponse response =
        manager.getCloudEdgeDeploymentConfigsWithActions(requestContext, filter, accessType);

    // Verify
    assertNotNull(response);

    // Verify original configs are included
    assertEquals(2, response.getCloudEdgeDeploymentsWithActionsCount());
    assertEquals(
        "id1",
        response.getCloudEdgeDeploymentsWithActions(0).getCloudEdgeDeploymentConfig().getId());
    assertEquals(
        "id2",
        response.getCloudEdgeDeploymentsWithActions(1).getCloudEdgeDeploymentConfig().getId());

    // Verify configs with actions
    assertEquals(2, response.getCloudEdgeDeploymentsWithActionsCount());

    // Verify first config with actions
    CloudEdgeDeploymentConfigWithActions configWithActions1 =
        response.getCloudEdgeDeploymentsWithActions(0);
    assertEquals("id1", configWithActions1.getCloudEdgeDeploymentConfig().getId());
    assertEquals(3, configWithActions1.getAllowedActionsCount()); // 2 from map + ACTION_VIEW
    assertTrue(configWithActions1.getAllowedActionsList().contains(Action.ACTION_EDIT));
    assertTrue(configWithActions1.getAllowedActionsList().contains(Action.ACTION_UPDATE_STATUS));
    assertTrue(configWithActions1.getAllowedActionsList().contains(Action.ACTION_VIEW));
    assertEquals(2, configWithActions1.getAllowedStatusesCount());

    // Verify second config with actions
    CloudEdgeDeploymentConfigWithActions configWithActions2 =
        response.getCloudEdgeDeploymentsWithActions(1);
    assertEquals("id2", configWithActions2.getCloudEdgeDeploymentConfig().getId());
    assertEquals(2, configWithActions2.getAllowedActionsCount()); // 1 from map + ACTION_VIEW
    assertTrue(configWithActions2.getAllowedActionsList().contains(Action.ACTION_HOLD));
    assertTrue(configWithActions2.getAllowedActionsList().contains(Action.ACTION_VIEW));
    assertEquals(0, configWithActions2.getAllowedStatusesCount());
  }

  @Test
  void testPerformCloudEdgeDeploymentConfigAction() {
    // Setup
    String id = "test-id";
    requestContext = RequestContext.forTenantId("tenant-id");
    CloudEdgeDeploymentConfigActionRequest request =
        CloudEdgeDeploymentConfigActionRequest.newBuilder()
            .setId(id)
            .setAction(Action.ACTION_CANCEL_CHANGE_REQUEST)
            .build();

    // Test case 1: Validation fails
    when(validator.validate(request))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("Invalid input"));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.performCloudEdgeDeploymentConfigAction(requestContext, request));
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    // Test case 2: Config not found
    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(null);

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.performCloudEdgeDeploymentConfigAction(requestContext, request));
    assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());

    // Test case 3: Action not allowed by validator
    CloudEdgeDeploymentConfig configNotActionable =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configNotActionable);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY,
            Action.ACTION_CANCEL_CHANGE_REQUEST))
        .thenThrow(
            Status.PERMISSION_DENIED
                .withDescription("This operation is not permitted")
                .asRuntimeException());

    exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> manager.performCloudEdgeDeploymentConfigAction(requestContext, request));
    assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    assertTrue(exception.getStatus().getDescription().contains("not permitted"));

    // Test case 4: Successful action - CANCEL_CHANGE_REQUEST with last applied input config
    CloudEdgeDeploymentInputConfig originalInputConfig =
        CloudEdgeDeploymentInputConfig.newBuilder()
            .setClusterConfig(ClusterConfig.newBuilder().setClusterName("original-cluster").build())
            .build();

    CloudEdgeDeploymentInputConfig changedInputConfig =
        CloudEdgeDeploymentInputConfig.newBuilder()
            .setClusterConfig(ClusterConfig.newBuilder().setClusterName("changed-cluster").build())
            .build();

    CloudEdgeDeploymentConfig configChangeRequested =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(changedInputConfig)
            .setLastAppliedInputConfig(originalInputConfig)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED)
                    .build())
            .build();

    // Expected config after cancellation - status reverted and input config restored from last
    // applied
    CloudEdgeDeploymentConfig expectedUpdatedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(originalInputConfig)
            .setLastAppliedInputConfig(originalInputConfig)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    when(validator.validate(request)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(configChangeRequested);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_CHANGE_REQUESTED,
            Action.ACTION_CANCEL_CHANGE_REQUEST))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY));

    // Execute
    manager.performCloudEdgeDeploymentConfigAction(requestContext, request);

    // Verify that store was called with the correct updated config
    verify(store).upsertCloudEdgeDeploymentConfig(requestContext, expectedUpdatedConfig);

    // Test case 5: Successful action - non-CANCEL_CHANGE_REQUEST action
    CloudEdgeDeploymentConfigActionRequest holdRequest =
        CloudEdgeDeploymentConfigActionRequest.newBuilder()
            .setId(id)
            .setAction(Action.ACTION_HOLD)
            .build();

    CloudEdgeDeploymentConfig deployedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(originalInputConfig)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    CloudEdgeDeploymentConfig expectedHeldConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(id)
            .setCloudEdgeDeploymentInputConfig(originalInputConfig)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD)
                    .build())
            .build();

    when(validator.validate(holdRequest)).thenReturn(Status.OK);
    when(store.getCloudEdgeDeploymentConfig(requestContext, id)).thenReturn(deployedConfig);
    when(validator.validateActionAndGetNextStates(
            DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY, Action.ACTION_HOLD))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_ON_HOLD));

    // Execute
    manager.performCloudEdgeDeploymentConfigAction(requestContext, holdRequest);

    // Verify that store was called with the correct updated config
    verify(store).upsertCloudEdgeDeploymentConfig(requestContext, expectedHeldConfig);
  }

  @Test
  void testCreateCloudEdgeDeploymentConfigWithDeploymentType() {
    // Setup
    String generatedId = "generated-id";
    when(uuidGenerator.generateRandomId()).thenReturn(generatedId);

    // Test case 1: Create with UNSPECIFIED deployment type - should default to HOSTED
    CloudEdgeDeploymentInputConfig inputConfigUnspecified =
        CloudEdgeDeploymentInputConfig.newBuilder()
            .setClusterConfig(
                ClusterConfig.newBuilder()
                    .setClusterName("test-cluster")
                    .setEnvironmentName("test-env")
                    .setDeploymentType(DeploymentType.DEPLOYMENT_TYPE_UNSPECIFIED)
                    .build())
            .addServiceConfigs(ServiceConfig.newBuilder().setServiceName("test-service").build())
            .build();

    CreateCloudEdgeDeploymentConfigRequest requestUnspecified =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(inputConfigUnspecified)
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    when(validator.validate(requestUnspecified)).thenReturn(Status.OK);
    when(validator.validateActionAndGetNextStates(
            null, ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE, Action.ACTION_CREATE))
        .thenReturn(List.of(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED));

    CloudEdgeDeploymentConfig expectedConfigWithHosted =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(generatedId)
            .setCloudEdgeDeploymentInputConfig(
                inputConfigUnspecified.toBuilder()
                    .setClusterConfig(
                        inputConfigUnspecified.getClusterConfig().toBuilder()
                            .setDeploymentType(DeploymentType.DEPLOYMENT_TYPE_HOSTED)
                            .build())
                    .build())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED)
                    .build())
            .build();

    when(store.upsertCloudEdgeDeploymentConfig(
            eq(requestContext),
            any(CloudEdgeDeploymentConfig.class),
            eq(requestUnspecified.getConfigPermission())))
        .thenReturn(expectedConfigWithHosted);

    CloudEdgeDeploymentConfig resultUnspecified =
        manager.createCloudEdgeDeploymentConfig(requestContext, requestUnspecified);

    assertNotNull(resultUnspecified);
    assertEquals(generatedId, resultUnspecified.getId());
    assertEquals(
        DeploymentType.DEPLOYMENT_TYPE_HOSTED,
        resultUnspecified
            .getCloudEdgeDeploymentInputConfig()
            .getClusterConfig()
            .getDeploymentType());

    // Test case 2: Create with explicit MANAGED deployment type - should preserve MANAGED
    CloudEdgeDeploymentInputConfig inputConfigManaged =
        CloudEdgeDeploymentInputConfig.newBuilder()
            .setClusterConfig(
                ClusterConfig.newBuilder()
                    .setClusterName("managed-cluster")
                    .setEnvironmentName("managed-env")
                    .setDeploymentType(DeploymentType.DEPLOYMENT_TYPE_MANAGED)
                    .build())
            .addServiceConfigs(ServiceConfig.newBuilder().setServiceName("managed-service").build())
            .build();

    CreateCloudEdgeDeploymentConfigRequest requestManaged =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(inputConfigManaged)
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    when(validator.validate(requestManaged)).thenReturn(Status.OK);

    CloudEdgeDeploymentConfig expectedConfigManaged =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(generatedId)
            .setCloudEdgeDeploymentInputConfig(inputConfigManaged)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED)
                    .build())
            .build();

    when(store.upsertCloudEdgeDeploymentConfig(
            eq(requestContext),
            any(CloudEdgeDeploymentConfig.class),
            eq(requestManaged.getConfigPermission())))
        .thenReturn(expectedConfigManaged);

    CloudEdgeDeploymentConfig resultManaged =
        manager.createCloudEdgeDeploymentConfig(requestContext, requestManaged);

    assertNotNull(resultManaged);
    assertEquals(generatedId, resultManaged.getId());
    assertEquals(
        DeploymentType.DEPLOYMENT_TYPE_MANAGED,
        resultManaged.getCloudEdgeDeploymentInputConfig().getClusterConfig().getDeploymentType());

    // Test case 3: Create with explicit HOSTED deployment type - should preserve HOSTED
    CloudEdgeDeploymentInputConfig inputConfigHosted =
        CloudEdgeDeploymentInputConfig.newBuilder()
            .setClusterConfig(
                ClusterConfig.newBuilder()
                    .setClusterName("hosted-cluster")
                    .setEnvironmentName("hosted-env")
                    .setDeploymentType(DeploymentType.DEPLOYMENT_TYPE_HOSTED)
                    .build())
            .addServiceConfigs(ServiceConfig.newBuilder().setServiceName("hosted-service").build())
            .build();

    CreateCloudEdgeDeploymentConfigRequest requestHosted =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(inputConfigHosted)
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    when(validator.validate(requestHosted)).thenReturn(Status.OK);

    CloudEdgeDeploymentConfig expectedConfigHosted =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId(generatedId)
            .setCloudEdgeDeploymentInputConfig(inputConfigHosted)
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_REQUESTED)
                    .build())
            .build();

    when(store.upsertCloudEdgeDeploymentConfig(
            eq(requestContext),
            any(CloudEdgeDeploymentConfig.class),
            eq(requestHosted.getConfigPermission())))
        .thenReturn(expectedConfigHosted);

    CloudEdgeDeploymentConfig resultHosted =
        manager.createCloudEdgeDeploymentConfig(requestContext, requestHosted);

    assertNotNull(resultHosted);
    assertEquals(generatedId, resultHosted.getId());
    assertEquals(
        DeploymentType.DEPLOYMENT_TYPE_HOSTED,
        resultHosted.getCloudEdgeDeploymentInputConfig().getClusterConfig().getDeploymentType());
  }
}
