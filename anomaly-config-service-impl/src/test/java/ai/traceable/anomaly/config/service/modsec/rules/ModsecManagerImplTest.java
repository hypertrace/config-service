package ai.traceable.anomaly.config.service.modsec.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ModsecManagerImplTest {
  private ModsecRulesRegistry mockModsecRulesRegistry;
  private ModsecManagerImpl modsecManager;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    mockModsecRulesRegistry = mock(ModsecRulesRegistryImpl.class);
    modsecManager = new ModsecManagerImpl(mockModsecRulesRegistry);
    requestContext = RequestContext.forTenantId("default tenant");
  }

  @Test
  @DisplayName("Should return same rule type")
  void getModsecCrsRules() {
    when(mockModsecRulesRegistry.getModsecCrsRulesBlob(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR))
        .thenReturn("regular");
    when(mockModsecRulesRegistry.getModsecCrsRulesBlob(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE))
        .thenReturn("safe");
    when(mockModsecRulesRegistry.getModsecCrsRulesBlob(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
        .thenReturn("block");

    List<AnomalySubRuleType> request =
        List.of(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);
    List<ModsecCrsRulesData> expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                .setModsecCrsRulesBlob("safe")
                .build(),
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
                .setModsecCrsRulesBlob("regular")
                .build());
    List<ModsecCrsRulesData> response = modsecManager.getModsecCrsRules(requestContext, request);
    assertEquals(expectedResponse.size(), response.size());
    assertTrue(response.containsAll(expectedResponse));
    assertTrue(expectedResponse.containsAll(response));
  }
}
