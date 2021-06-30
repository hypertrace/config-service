package ai.traceable.anomaly.config.service.modsec.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesType;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
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
    when(mockModsecRulesRegistry.getModsecSafeCrsRulesBlob()).thenReturn("safe");
    when(mockModsecRulesRegistry.getModsecRegularCrsRulesBlob()).thenReturn("regular");

    Set<ModsecCrsRulesType> request =
        Set.of(
            ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_SAFE,
            ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_REGULAR);
    List<ModsecCrsRulesData> expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setModsecCrsRulesType(ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_SAFE)
                .setModsecCrsRulesBlob("safe")
                .build(),
            ModsecCrsRulesData.newBuilder()
                .setModsecCrsRulesType(ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_REGULAR)
                .setModsecCrsRulesBlob("regular")
                .build());
    List<ModsecCrsRulesData> response = modsecManager.getModsecCrsRules(requestContext, request);
    assertEquals(expectedResponse.size(), response.size());
    assertTrue(response.containsAll(expectedResponse));
    assertTrue(expectedResponse.containsAll(response));
  }
}
