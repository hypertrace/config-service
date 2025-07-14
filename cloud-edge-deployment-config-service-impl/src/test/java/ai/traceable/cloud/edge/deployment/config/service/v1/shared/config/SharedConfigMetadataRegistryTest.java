package ai.traceable.cloud.edge.deployment.config.service.v1.shared.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigPermission;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SharedConfigMetadataRegistryImplTest {

  @Mock private SharedConfigMetadataParser mockParser;

  private SharedConfigMetadataRegistryImpl registry;

  // Test data
  private ConfigValueDescriptor globalReadGlobalWrite;
  private ConfigValueDescriptor globalReadTraceableWrite;
  private ConfigValueDescriptor traceableReadGlobalWrite;
  private ConfigValueDescriptor traceableReadTraceableWrite;

  @BeforeEach
  void setUp() {
    // Create test descriptors with different permission combinations
    globalReadGlobalWrite =
        ConfigValueDescriptor.newBuilder()
            .setConfigKey("global-global")
            .setDisplayName("Global Read Global Write")
            .setDescription("Config with global read and global write permissions")
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    globalReadTraceableWrite =
        ConfigValueDescriptor.newBuilder()
            .setConfigKey("global-traceable")
            .setDisplayName("Global Read Traceable Write")
            .setDescription("Config with global read and traceable write permissions")
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    traceableReadGlobalWrite =
        ConfigValueDescriptor.newBuilder()
            .setConfigKey("traceable-global")
            .setDisplayName("Traceable Read Global Write")
            .setDescription("Config with traceable read and global write permissions")
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    traceableReadTraceableWrite =
        ConfigValueDescriptor.newBuilder()
            .setConfigKey("traceable-traceable")
            .setDisplayName("Traceable Read Traceable Write")
            .setDescription("Config with traceable read and traceable write permissions")
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    // Mock the parser to return our test descriptors
    when(mockParser.loadConfigValueDescriptors("cluster-shared-config-metadata.yaml"))
        .thenReturn(Arrays.asList(globalReadGlobalWrite, globalReadTraceableWrite));

    when(mockParser.loadConfigValueDescriptors("service-shared-config-metadata.yaml"))
        .thenReturn(Arrays.asList(traceableReadGlobalWrite, traceableReadTraceableWrite));

    // Create the registry with our mock parser
    registry = new SharedConfigMetadataRegistryImpl(mockParser);
  }

  @Test
  void testGetSharedConfigMetadataWithReadPermission_Global() {
    // Test with global access type
    SharedConfigMetadata metadata =
        registry.getSharedConfigMetadataWithReadPermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL);

    // Verify
    assertNotNull(metadata);

    // Should include configs with global read permission
    assertEquals(2, metadata.getClusterConfigDetailsCount());
    assertTrue(metadata.getClusterConfigDetailsMap().containsKey("global-global"));
    assertTrue(metadata.getClusterConfigDetailsMap().containsKey("global-traceable"));

    // Should not include configs with traceable read permission
    assertEquals(0, metadata.getServiceConfigDetailsCount());

    // Verify the actual descriptors
    assertEquals(globalReadGlobalWrite, metadata.getClusterConfigDetailsMap().get("global-global"));
    assertEquals(
        globalReadTraceableWrite, metadata.getClusterConfigDetailsMap().get("global-traceable"));
  }

  @Test
  void testGetSharedConfigMetadataWithReadPermission_Traceable() {
    // Test with traceable access type
    SharedConfigMetadata metadata =
        registry.getSharedConfigMetadataWithReadPermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE);

    // Verify
    assertNotNull(metadata);

    // Traceable access should include all configs
    assertEquals(2, metadata.getClusterConfigDetailsCount());
    assertEquals(2, metadata.getServiceConfigDetailsCount());

    assertTrue(metadata.getClusterConfigDetailsMap().containsKey("global-global"));
    assertTrue(metadata.getClusterConfigDetailsMap().containsKey("global-traceable"));
    assertTrue(metadata.getServiceConfigDetailsMap().containsKey("traceable-global"));
    assertTrue(metadata.getServiceConfigDetailsMap().containsKey("traceable-traceable"));
  }

  @Test
  void testGetSharedConfigMetadataWithWritePermission_Global() {
    // Test with global access type
    SharedConfigMetadata metadata =
        registry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL);

    // Verify
    assertNotNull(metadata);

    // Should include configs with global write permission
    assertEquals(1, metadata.getClusterConfigDetailsCount());
    assertTrue(metadata.getClusterConfigDetailsMap().containsKey("global-global"));

    assertEquals(1, metadata.getServiceConfigDetailsCount());
    assertTrue(metadata.getServiceConfigDetailsMap().containsKey("traceable-global"));

    // Verify the actual descriptors
    assertEquals(globalReadGlobalWrite, metadata.getClusterConfigDetailsMap().get("global-global"));
    assertEquals(
        traceableReadGlobalWrite, metadata.getServiceConfigDetailsMap().get("traceable-global"));
  }

  @Test
  void testGetSharedConfigMetadataWithWritePermission_Traceable() {
    // Test with traceable access type
    SharedConfigMetadata metadata =
        registry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE);

    // Verify
    assertNotNull(metadata);

    // Traceable access should include all configs
    assertEquals(2, metadata.getClusterConfigDetailsCount());
    assertEquals(2, metadata.getServiceConfigDetailsCount());

    assertTrue(metadata.getClusterConfigDetailsMap().containsKey("global-global"));
    assertTrue(metadata.getClusterConfigDetailsMap().containsKey("global-traceable"));
    assertTrue(metadata.getServiceConfigDetailsMap().containsKey("traceable-global"));
    assertTrue(metadata.getServiceConfigDetailsMap().containsKey("traceable-traceable"));
  }

  @Test
  void testEmptyResults() {
    // Setup parser to return empty lists
    when(mockParser.loadConfigValueDescriptors("cluster-shared-config-metadata.yaml"))
        .thenReturn(List.of());
    when(mockParser.loadConfigValueDescriptors("service-shared-config-metadata.yaml"))
        .thenReturn(List.of());

    // Create new registry with empty results
    SharedConfigMetadataRegistryImpl emptyRegistry =
        new SharedConfigMetadataRegistryImpl(mockParser);

    // Test read permissions
    SharedConfigMetadata readMetadata =
        emptyRegistry.getSharedConfigMetadataWithReadPermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL);

    assertNotNull(readMetadata);
    assertEquals(0, readMetadata.getClusterConfigDetailsCount());
    assertEquals(0, readMetadata.getServiceConfigDetailsCount());

    // Test write permissions
    SharedConfigMetadata writeMetadata =
        emptyRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL);

    assertNotNull(writeMetadata);
    assertEquals(0, writeMetadata.getClusterConfigDetailsCount());
    assertEquals(0, writeMetadata.getServiceConfigDetailsCount());
  }
}
