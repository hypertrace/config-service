package ai.traceable.anomaly.config.service.global.ruleinfo;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnomalyRuleInfoManagerImplTest {
  private ApiDefinitionRegistry apiDefinitionRegistry;
  private ModsecRulesRegistry modsecRulesRegistry;
  private SessionRulesRegistry sessionRulesRegistry;
  private VolumetricRulesRegistryImpl volumetricRulesRegistry;
  private CredentialStuffingRulesRegistryImpl credentialStuffingRulesRegistry;
  private RuleInfoManager ruleInfoManager;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    apiDefinitionRegistry = mock(ApiDefinitionRegistryImpl.class);
    modsecRulesRegistry = mock(ModsecRulesRegistryImpl.class);
    sessionRulesRegistry = mock(SessionRulesRegistryImpl.class);
    volumetricRulesRegistry = mock(VolumetricRulesRegistryImpl.class);
    credentialStuffingRulesRegistry = mock(CredentialStuffingRulesRegistryImpl.class);
    ruleInfoManager =
        new AnomalyRuleInfoManagerImpl(
            apiDefinitionRegistry,
            modsecRulesRegistry,
            sessionRulesRegistry,
            volumetricRulesRegistry,
            credentialStuffingRulesRegistry);
    requestContext = RequestContext.forTenantId("default tenant");
  }

  @Test
  void getAnomalyRuleInfos() {
    when(apiDefinitionRegistry.getApiDefRuleInfos())
        .thenReturn(
            Map.of("id-0", buildAnomalyRuleInfo("id-0"), "id-00", buildAnomalyRuleInfo("id-0")));
    when(modsecRulesRegistry.getModsecRuleInfos(any()))
        .thenReturn(Map.of("id-1", buildAnomalyRuleInfo("id-1")));
    when(sessionRulesRegistry.getSessionRuleInfos())
        .thenReturn(
            Map.of(
                "id-1", buildAnomalyRuleInfo("id-1"),
                "id-2", buildAnomalyRuleInfo("id-2"),
                "id-3", buildAnomalyRuleInfo("id-3")));
    when(volumetricRulesRegistry.getVolumetricRuleInfos())
        .thenReturn(Map.of("id-1", buildAnomalyRuleInfo("id-1")));
    when(credentialStuffingRulesRegistry.getCredentialStuffingRuleInfos())
        .thenReturn(Map.of("id-2", buildAnomalyRuleInfo("id-4")));
    List<AnomalyRuleInfo> response =
        ruleInfoManager.getAnomalyRuleInfos(
            requestContext,
            List.of(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC),
            ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED);
    assertEquals(1, response.size());
    response =
        ruleInfoManager.getAnomalyRuleInfos(
            requestContext,
            List.of(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF),
            ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED);
    // As two are repeated
    assertEquals(1, response.size());
    response =
        ruleInfoManager.getAnomalyRuleInfos(
            requestContext,
            List.of(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_VOLUMETRIC),
            ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED);
    assertEquals(1, response.size());
    response =
        ruleInfoManager.getAnomalyRuleInfos(
            requestContext,
            List.of(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_CREDENTIAL_STUFFING),
            ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED);
    assertEquals(1, response.size());
    response =
        ruleInfoManager.getAnomalyRuleInfos(
            requestContext,
            List.of(
                AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF,
                AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC),
            ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED);
    assertEquals(2, response.size());

    response =
        ruleInfoManager.getAnomalyRuleInfos(
            requestContext,
            List.of(
                AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF,
                AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC,
                AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION,
                AnomalyEventFamily.ANOMALY_EVENT_FAMILY_VOLUMETRIC,
                AnomalyEventFamily.ANOMALY_EVENT_FAMILY_CREDENTIAL_STUFFING),
            ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED);
    // Common rules are not duplicated
    assertEquals(5, response.size());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ruleInfoManager.getAnomalyRuleInfos(
                requestContext,
                List.of(
                    AnomalyEventFamily.ANOMALY_EVENT_FAMILY_CUSTOM_SIGNATURE,
                    AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC),
                ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED));
  }

  private AnomalyRuleInfo buildAnomalyRuleInfo(String id) {
    return AnomalyRuleInfo.newBuilder()
        .addSubRuleInfos(AnomalySubRuleInfo.newBuilder().setRuleId(id).build())
        .build();
  }
}
