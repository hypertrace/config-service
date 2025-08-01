package ai.traceable.cloud.edge.deployment.config.service.v1.shared.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SharedConfigMetadataParserTest {

  private SharedConfigMetadataParser parser;

  @BeforeEach
  void setUp() {
    parser = new SharedConfigMetadataParser();
  }

  @Test
  void testLoadConfigValueDescriptors_WithActualServiceYamlFile() throws Exception {
    // Setup with actual file path
    String yamlFilePath = "cluster_shared_config_metadata.yaml";

    // Create test class loader that returns the actual file;

    // Make sure we have a valid stream

    // Use reflection to set the class loader for the test

    // Execute
    List<ConfigValueDescriptor> descriptors = parser.loadConfigValueDescriptors(yamlFilePath);

    // Verify
    assertNotNull(descriptors);
    assertEquals(9, descriptors.size());

    // Verify first descriptor (idleTimeout)
    ConfigValueDescriptor idleTimeoutDescriptor = descriptors.get(0);
    assertEquals("Idle Timeout", idleTimeoutDescriptor.getDescription());
    assertEquals("Idle Timeout(s)", idleTimeoutDescriptor.getDisplayName());
    assertTrue(idleTimeoutDescriptor.hasConstraint());
    assertTrue(idleTimeoutDescriptor.getConstraint().hasIntRange());
    assertEquals(1, idleTimeoutDescriptor.getConstraint().getIntRange().getMin());
    assertEquals(120, idleTimeoutDescriptor.getConstraint().getIntRange().getMax());
    assertEquals(
        ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
        idleTimeoutDescriptor.getConfigPermission().getRead());
    assertEquals(
        ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
        idleTimeoutDescriptor.getConfigPermission().getWrite());
    assertEquals(1, idleTimeoutDescriptor.getIdentifiersCount());
    assertEquals("", idleTimeoutDescriptor.getIdentifiers(0).getChartName());
    assertEquals("", idleTimeoutDescriptor.getIdentifiers(0).getChartPath());

    // Verify second descriptor (maxConnectionDuration)
    ConfigValueDescriptor maxConnectionDurationDescriptor = descriptors.get(1);
    assertEquals(
        "Max connection duration for a  request", maxConnectionDurationDescriptor.getDescription());
    assertEquals("Max Connection Duration(s)", maxConnectionDurationDescriptor.getDisplayName());
    assertTrue(maxConnectionDurationDescriptor.hasConstraint());
    assertTrue(maxConnectionDurationDescriptor.getConstraint().hasIntRange());
    assertEquals(60, maxConnectionDurationDescriptor.getConstraint().getIntRange().getMin());
    assertEquals(86400, maxConnectionDurationDescriptor.getConstraint().getIntRange().getMax());
    assertEquals(
        ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
        maxConnectionDurationDescriptor.getConfigPermission().getRead());
    assertEquals(
        ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL,
        maxConnectionDurationDescriptor.getConfigPermission().getWrite());
    assertEquals(1, maxConnectionDurationDescriptor.getIdentifiersCount());
    assertEquals("", maxConnectionDurationDescriptor.getIdentifiers(0).getChartName());
    assertEquals("", maxConnectionDurationDescriptor.getIdentifiers(0).getChartPath());
  }
}
