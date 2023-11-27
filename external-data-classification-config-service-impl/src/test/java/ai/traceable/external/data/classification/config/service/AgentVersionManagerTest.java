package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.AgentCapabilities;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.Component;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AgentVersionManagerTest {
  @Mock SemanticVersioningComparator mockComparator;

  @InjectMocks AgentVersionManager agentVersionManager;

  // TODO update once known
  private static final String ACTUAL_REQUIRED_VERSION = "9.9.9-rc.0";

  @Test
  void testNewAgentVersion() {
    when(mockComparator.isVersionSupported("1.39.0", ACTUAL_REQUIRED_VERSION)).thenReturn(true);

    assertTrue(
        agentVersionManager.isContentTypeParseRuleSupported(
            AgentCapabilities.newBuilder()
                .addComponents(Component.newBuilder().setTraceablePlatformAgentVersion("1.39.0"))
                .addComponents(Component.getDefaultInstance()) // add some unrelated component
                .build()));
  }

  @Test
  void testOldAgentVersion() {
    when(mockComparator.isVersionSupported("1.5.0", ACTUAL_REQUIRED_VERSION)).thenReturn(false);

    assertFalse(
        agentVersionManager.isContentTypeParseRuleSupported(
            AgentCapabilities.newBuilder()
                .addComponents(Component.getDefaultInstance()) // add some unrelated component
                .addComponents(Component.newBuilder().setTraceablePlatformAgentVersion("1.5.0"))
                .build()));
  }

  @Test
  void testNoAgentVersion() {
    assertFalse(
        agentVersionManager.isContentTypeParseRuleSupported(
            AgentCapabilities.getDefaultInstance()));
    verifyNoInteractions(mockComparator);
  }
}
