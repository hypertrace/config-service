package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfigServiceGrpc.DataParsingConfigServiceBlockingStub;
import ai.traceable.data.parsing.config.service.v1.GetDataParsingRulesResponse;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule.DataParsingMode;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.AgentCapabilities;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.Component;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataParsingRuleManagerTest {

  private static final DataParsingRule MOCK_OLD_RULE =
      DataParsingRule.newBuilder().setMode(DataParsingMode.DATA_PARSING_MODE_JSON).build();
  private static final DataParsingRule MOCK_NEW_RULE =
      DataParsingRule.newBuilder()
          .setMode(DataParsingMode.DATA_PARSING_MODE_CONTENT_TYPE_HEADER)
          .build();
  private static final List<DataParsingRule> MOCK_ALL_RULES = List.of(MOCK_OLD_RULE, MOCK_NEW_RULE);
  @Mock ExternalDataClassificationConfig config;
  @Mock AgentVersionManager agentVersionManager;
  @Mock FeatureCachingClient featureCachingClient;
  @Mock DataParsingConfigServiceBlockingStub dataParsingConfigServiceBlockingStub;
  @Mock DataParsingRuleConverter dataParsingRuleConverter;
  @InjectMocks DataParsingRuleManager dataParsingRuleManager;

  @Test
  void testNewRules() {
    AgentCapabilities newCapabilities =
        AgentCapabilities.newBuilder()
            .addComponents(Component.newBuilder().setTraceablePlatformAgentVersion("1.40.0"))
            .build();
    when(agentVersionManager.isContentTypeParseRuleSupported(newCapabilities)).thenReturn(true);
    when(config.getDefaultDataParsingRules()).thenReturn(MOCK_ALL_RULES);
    when(dataParsingConfigServiceBlockingStub.getDataParsingRules(any()))
        .thenReturn(GetDataParsingRulesResponse.getDefaultInstance());
    assertEquals(
        MOCK_ALL_RULES,
        dataParsingRuleManager.getDataParsingRulesForAgent(
            RequestContext.forTenantId("tenant"), Optional.empty(), newCapabilities));
  }

  @Test
  void testOldRules() {
    AgentCapabilities oldCapabilities =
        AgentCapabilities.newBuilder()
            .addComponents(Component.newBuilder().setTraceablePlatformAgentVersion("1.20.0"))
            .build();
    when(agentVersionManager.isContentTypeParseRuleSupported(oldCapabilities)).thenReturn(false);
    when(config.getDefaultDataParsingRules()).thenReturn(MOCK_ALL_RULES);
    when(dataParsingConfigServiceBlockingStub.getDataParsingRules(any()))
        .thenReturn(GetDataParsingRulesResponse.getDefaultInstance());
    assertEquals(
        List.of(MOCK_OLD_RULE),
        dataParsingRuleManager.getDataParsingRulesForAgent(
            RequestContext.forTenantId("tenant"), Optional.empty(), oldCapabilities));
  }
}
