package ai.traceable.cloud.edge.deployment.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentInputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentOutputConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ClusterAdvancedConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ClusterConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import ai.traceable.cloud.edge.deployment.config.service.v1.DeploymentStatus;
import ai.traceable.cloud.edge.deployment.config.service.v1.RegionConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceAdvancedConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.ServiceConfig;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PermissionBasedConfigResolverTest {

  @Mock private SharedConfigMetadataRegistry mockRegistry;

  private PermissionBasedConfigResolver resolver;

  // Test data
  private CloudEdgeDeploymentConfig fullConfig;
  private SharedConfigMetadata globalReadMetadata;
  private SharedConfigMetadata traceableReadMetadata;

  @BeforeEach
  void setUp() {
    resolver = new PermissionBasedConfigResolver(mockRegistry);

    // Create test config with advanced configurations
    Map<String, Value> clusterConfigMap = new HashMap<>();
    clusterConfigMap.put("key1", Value.newBuilder().setNumberValue(5).build());
    clusterConfigMap.put("key2", Value.newBuilder().setStringValue("INFO").build());
    clusterConfigMap.put("key3", Value.newBuilder().setStringValue("secret123").build());

    Map<String, Value> serviceConfigMap = new HashMap<>();
    serviceConfigMap.put("key4", Value.newBuilder().setNumberValue(1000).build());
    serviceConfigMap.put("key5", Value.newBuilder().setBoolValue(true).build());
    serviceConfigMap.put("key6", Value.newBuilder().setStringValue("internal").build());

    fullConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-config-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("test-cluster")
                            .setAdvancedConfig(
                                ClusterAdvancedConfig.newBuilder()
                                    .setGenericConfig(
                                        Struct.newBuilder().putAllFields(clusterConfigMap).build())
                                    .build())
                            .build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("test-service")
                            .setAdvancedConfig(
                                ServiceAdvancedConfig.newBuilder()
                                    .setGenericConfig(
                                        Struct.newBuilder().putAllFields(serviceConfigMap).build())
                                    .build())
                            .build())
                    .build())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS)
                    .build())
            .build();

    // Create metadata for global read access
    Map<String, ConfigValueDescriptor> globalReadClusterMap = new HashMap<>();
    globalReadClusterMap.put(
        "key1", ConfigValueDescriptor.newBuilder().setConfigKey("key1").build());
    globalReadClusterMap.put(
        "key2", ConfigValueDescriptor.newBuilder().setConfigKey("key2").build());

    Map<String, ConfigValueDescriptor> globalReadServiceMap = new HashMap<>();
    globalReadServiceMap.put(
        "key4", ConfigValueDescriptor.newBuilder().setConfigKey("key4").build());

    globalReadMetadata =
        SharedConfigMetadata.newBuilder()
            .putAllClusterConfigDetails(globalReadClusterMap)
            .putAllServiceConfigDetails(globalReadServiceMap)
            .build();

    // Create metadata for traceable read access (includes all fields)
    Map<String, ConfigValueDescriptor> traceableReadClusterMap = new HashMap<>();
    traceableReadClusterMap.put(
        "key1", ConfigValueDescriptor.newBuilder().setConfigKey("key1").build());
    traceableReadClusterMap.put(
        "key2", ConfigValueDescriptor.newBuilder().setConfigKey("key2").build());
    traceableReadClusterMap.put(
        "key3", ConfigValueDescriptor.newBuilder().setConfigKey("key3").build());

    Map<String, ConfigValueDescriptor> traceableReadServiceMap = new HashMap<>();
    traceableReadServiceMap.put(
        "key4", ConfigValueDescriptor.newBuilder().setConfigKey("key4").build());
    traceableReadServiceMap.put(
        "key5", ConfigValueDescriptor.newBuilder().setConfigKey("key5").build());
    traceableReadServiceMap.put(
        "key6", ConfigValueDescriptor.newBuilder().setConfigKey("key6").build());

    traceableReadMetadata =
        SharedConfigMetadata.newBuilder()
            .putAllClusterConfigDetails(traceableReadClusterMap)
            .putAllServiceConfigDetails(traceableReadServiceMap)
            .build();
  }

  @Test
  void testGetResolvedConfigForReadRequest_GlobalAccess() {
    // Setup mock to return global read metadata
    when(mockRegistry.getSharedConfigMetadataWithReadPermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(globalReadMetadata);

    // Execute
    CloudEdgeDeploymentConfig result =
        resolver.getResolvedConfigForReadRequest(
            fullConfig, ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL);

    // Verify
    assertNotNull(result);
    assertEquals("test-config-id", result.getId());

    // Verify cluster config filtering
    Map<String, Value> resultClusterFields =
        result
            .getCloudEdgeDeploymentInputConfig()
            .getClusterConfig()
            .getAdvancedConfig()
            .getGenericConfig()
            .getFieldsMap();

    assertEquals(2, resultClusterFields.size());
    assertTrue(resultClusterFields.containsKey("key1"));
    assertTrue(resultClusterFields.containsKey("key2"));
    assertFalse(resultClusterFields.containsKey("key3"));

    // Verify service config filtering
    Map<String, Value> resultServiceFields =
        result
            .getCloudEdgeDeploymentInputConfig()
            .getServiceConfigs(0)
            .getAdvancedConfig()
            .getGenericConfig()
            .getFieldsMap();

    assertEquals(1, resultServiceFields.size());
    assertTrue(resultServiceFields.containsKey("key4"));
    assertFalse(resultServiceFields.containsKey("key5"));
    assertFalse(resultServiceFields.containsKey("key6"));
  }

  @Test
  void testGetResolvedConfigForReadRequest_TraceableAccess() {
    // Setup mock to return traceable read metadata
    when(mockRegistry.getSharedConfigMetadataWithReadPermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE))
        .thenReturn(traceableReadMetadata);

    // Execute
    CloudEdgeDeploymentConfig result =
        resolver.getResolvedConfigForReadRequest(
            fullConfig, ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE);

    // Verify
    assertNotNull(result);

    // Verify cluster config filtering - should include all fields
    Map<String, Value> resultClusterFields =
        result
            .getCloudEdgeDeploymentInputConfig()
            .getClusterConfig()
            .getAdvancedConfig()
            .getGenericConfig()
            .getFieldsMap();

    assertEquals(3, resultClusterFields.size());
    assertTrue(resultClusterFields.containsKey("key1"));
    assertTrue(resultClusterFields.containsKey("key2"));
    assertTrue(resultClusterFields.containsKey("key3"));

    // Verify service config filtering - should include all fields
    Map<String, Value> resultServiceFields =
        result
            .getCloudEdgeDeploymentInputConfig()
            .getServiceConfigs(0)
            .getAdvancedConfig()
            .getGenericConfig()
            .getFieldsMap();

    assertEquals(3, resultServiceFields.size());
    assertTrue(resultServiceFields.containsKey("key4"));
    assertTrue(resultServiceFields.containsKey("key5"));
    assertTrue(resultServiceFields.containsKey("key6"));
  }

  @Test
  void testGetResolvedConfigForWriteRequest_TraceableAccess() {
    // Create a new config with changes
    CloudEdgeDeploymentConfig newConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-config-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("updated-cluster")
                            .setEnvironmentName("prod")
                            .addPrimaryRegions(
                                RegionConfig.newBuilder().setAwsRegion("us-east-1").build())
                            .setAdvancedConfig(
                                ClusterAdvancedConfig.newBuilder()
                                    .setGenericConfig(
                                        Struct.newBuilder()
                                            .putFields(
                                                "traceable-key",
                                                Value.newBuilder().setStringValue("value").build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    // Execute with traceable access
    CloudEdgeDeploymentConfig result =
        resolver.getResolvedConfigForWriteRequest(
            newConfig, fullConfig, ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE);

    // Verify - traceable access should replace the entire config
    assertNotNull(result);
    assertEquals("test-config-id", result.getId());
    assertEquals(
        "updated-cluster",
        result.getCloudEdgeDeploymentInputConfig().getClusterConfig().getClusterName());
    assertEquals(
        "prod", result.getCloudEdgeDeploymentInputConfig().getClusterConfig().getEnvironmentName());
    assertEquals(
        DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY,
        result.getCloudEdgeDeployedOutputConfig().getStatus());

    // Verify that the last applied input config is set correctly
    assertTrue(result.hasLastAppliedInputConfig());
    assertEquals(
        result.getCloudEdgeDeploymentInputConfig().getClusterConfig().getClusterName(),
        result.getLastAppliedInputConfig().getClusterConfig().getClusterName());
    assertEquals(
        result.getCloudEdgeDeploymentInputConfig().getClusterConfig().getEnvironmentName(),
        result.getLastAppliedInputConfig().getClusterConfig().getEnvironmentName());
  }

  @Test
  void testLastAppliedInputConfigSetWhenDeploymentSuccessful() {
    // Create a new config with deployment status as DEPLOYED_SUCCESSFULLY
    CloudEdgeDeploymentConfig newConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-config-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("new-cluster")
                            .setEnvironmentName("stage")
                            .build())
                    .build())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    // Create an existing config with different values
    CloudEdgeDeploymentConfig existingConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-config-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("old-cluster")
                            .setEnvironmentName("dev")
                            .build())
                    .build())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS)
                    .build())
            .build();

    // Execute with traceable access
    CloudEdgeDeploymentConfig result =
        resolver.getResolvedConfigForWriteRequest(
            newConfig, existingConfig, ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE);

    // Verify the last applied input config is set when deployment status is DEPLOYED_SUCCESSFULLY
    assertTrue(result.hasLastAppliedInputConfig());
    assertEquals(
        "new-cluster", result.getLastAppliedInputConfig().getClusterConfig().getClusterName());
    assertEquals(
        "stage", result.getLastAppliedInputConfig().getClusterConfig().getEnvironmentName());

    // Create another config with IN_PROGRESS status
    CloudEdgeDeploymentConfig inProgressConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-config-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("another-cluster").build())
                    .build())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS)
                    .build())
            .build();

    // Execute with traceable access but with IN_PROGRESS status
    CloudEdgeDeploymentConfig resultInProgress =
        resolver.getResolvedConfigForWriteRequest(
            inProgressConfig, existingConfig, ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE);

    // Verify the last applied input config is NOT set when status is not DEPLOYED_SUCCESSFULLY
    assertFalse(resultInProgress.hasLastAppliedInputConfig());
  }

  @Test
  void testGetResolvedConfigForWriteRequest_GlobalAccess() {

    Struct existingGenericConfig =
        Struct.newBuilder()
            .putFields(
                "traceable-key", Value.newBuilder().setStringValue("existing-traceable").build())
            .putFields("global-key", Value.newBuilder().setStringValue("existing-global").build())
            .build();

    Struct newGenericConfig =
        Struct.newBuilder()
            .putFields("global-key", Value.newBuilder().setStringValue("updated-global").build())
            .build();

    CloudEdgeDeploymentConfig existingConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-config-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("old-cluster")
                            .setAdvancedConfig(
                                ClusterAdvancedConfig.newBuilder()
                                    .setGenericConfig(existingGenericConfig)
                                    .build())
                            .build())
                    .build())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    // Create a new config with changes
    CloudEdgeDeploymentConfig newConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("updated-cluster")
                            .setAdvancedConfig(
                                ClusterAdvancedConfig.newBuilder()
                                    .setGenericConfig(newGenericConfig)
                                    .build())
                            .build())
                    .build())
            .build();

    // Execute with global access
    CloudEdgeDeploymentConfig result =
        resolver.getResolvedConfigForWriteRequest(
            newConfig, existingConfig, ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL);

    assertNotNull(result);
    ClusterConfig resultCluster = result.getCloudEdgeDeploymentInputConfig().getClusterConfig();

    // Name should be updated
    assertEquals("updated-cluster", resultCluster.getClusterName());

    // Generic config should merge respecting access
    Map<String, Value> mergedFields =
        resultCluster.getAdvancedConfig().getGenericConfig().getFieldsMap();
    assertEquals(
        "existing-traceable", mergedFields.get("traceable-key").getStringValue()); // preserved
    assertEquals("updated-global", mergedFields.get("global-key").getStringValue()); // updated

    // Output config status should be reset to IN_PROGRESS
    assertEquals(
        DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS,
        result.getCloudEdgeDeployedOutputConfig().getStatus());
  }

  @Test
  void testGetResolvedConfigForReadRequest_EmptyAdvancedConfig() {
    // Create config with empty advanced configs
    CloudEdgeDeploymentConfig emptyConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("empty-config")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("test-cluster")
                            .setAdvancedConfig(
                                ClusterAdvancedConfig.newBuilder()
                                    .setGenericConfig(Struct.newBuilder().build())
                                    .build())
                            .build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("test-service")
                            .setAdvancedConfig(
                                ServiceAdvancedConfig.newBuilder()
                                    .setGenericConfig(Struct.newBuilder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    // Setup mock
    when(mockRegistry.getSharedConfigMetadataWithReadPermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(globalReadMetadata);

    // Execute
    CloudEdgeDeploymentConfig result =
        resolver.getResolvedConfigForReadRequest(
            emptyConfig, ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL);

    // Verify
    assertNotNull(result);
    assertEquals("empty-config", result.getId());

    // Verify empty maps are handled correctly
    Map<String, Value> resultClusterFields =
        result
            .getCloudEdgeDeploymentInputConfig()
            .getClusterConfig()
            .getAdvancedConfig()
            .getGenericConfig()
            .getFieldsMap();
    assertEquals(0, resultClusterFields.size());

    Map<String, Value> resultServiceFields =
        result
            .getCloudEdgeDeploymentInputConfig()
            .getServiceConfigs(0)
            .getAdvancedConfig()
            .getGenericConfig()
            .getFieldsMap();
    assertEquals(0, resultServiceFields.size());
  }

  @Test
  void testLastAppliedInputConfigPreservedWhenConfigChangedToInProgress() {
    // Create an initial config with DEPLOYED_SUCCESSFULLY status and a last applied input config
    CloudEdgeDeploymentInputConfig initialInputConfig =
        CloudEdgeDeploymentInputConfig.newBuilder()
            .setClusterConfig(
                ClusterConfig.newBuilder()
                    .setClusterName("original-cluster")
                    .setEnvironmentName("prod")
                    .build())
            .build();

    CloudEdgeDeploymentConfig existingConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-config-id")
            .setCloudEdgeDeploymentInputConfig(initialInputConfig)
            .setLastAppliedInputConfig(
                initialInputConfig) // This was set during previous successful deployment
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    // Create a new config with changes that will set status to IN_PROGRESS
    CloudEdgeDeploymentInputConfig newInputConfig =
        CloudEdgeDeploymentInputConfig.newBuilder()
            .setClusterConfig(
                ClusterConfig.newBuilder()
                    .setClusterName("modified-cluster")
                    .setEnvironmentName("prod")
                    .build())
            .build();

    CloudEdgeDeploymentConfig newConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-config-id")
            .setCloudEdgeDeploymentInputConfig(newInputConfig)
            .build();

    // Execute with traceable access
    CloudEdgeDeploymentConfig result =
        resolver.getResolvedConfigForWriteRequest(
            newConfig, existingConfig, ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE);

    // Verify that the input config has been updated
    assertEquals(
        "modified-cluster",
        result.getCloudEdgeDeploymentInputConfig().getClusterConfig().getClusterName());

    // Verify that the status is now IN_PROGRESS
    assertEquals(
        DeploymentStatus.DEPLOYMENT_STATUS_IN_PROGRESS,
        result.getCloudEdgeDeployedOutputConfig().getStatus());

    // Verify that the last applied input config is preserved from the previous successful
    // deployment
    assertTrue(result.hasLastAppliedInputConfig());
    assertEquals(
        "original-cluster", result.getLastAppliedInputConfig().getClusterConfig().getClusterName());

    // Now simulate a successful deployment of the modified config
    CloudEdgeDeploymentConfig deployedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-config-id")
            .setCloudEdgeDeploymentInputConfig(result.getCloudEdgeDeploymentInputConfig())
            .setLastAppliedInputConfig(result.getLastAppliedInputConfig())
            .setCloudEdgeDeployedOutputConfig(
                CloudEdgeDeploymentOutputConfig.newBuilder()
                    .setStatus(DeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED_SUCCESSFULLY)
                    .build())
            .build();

    // Execute with traceable access to mark as deployed
    CloudEdgeDeploymentConfig finalResult =
        resolver.getResolvedConfigForWriteRequest(
            deployedConfig, result, ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE);

    // Verify that the last applied input config is updated to the new input config
    assertTrue(finalResult.hasLastAppliedInputConfig());
    assertEquals(
        "modified-cluster",
        finalResult.getLastAppliedInputConfig().getClusterConfig().getClusterName());
  }
}
