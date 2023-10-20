package ai.traceable.anomaly.config.service.modsec.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecCrsRulesHandler;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.utils.modsec.ModsecRuleUtils;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ModsecManagerImplTest {
  private static final String environmentId = "env-id";

  private ModsecRulesRegistry modsecRulesRegistry;
  private AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;
  private ModsecManagerImpl modsecManager;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    modsecRulesRegistry = mock(ModsecRulesRegistryImpl.class);
    anomalyDetectionConfigManager = mock(AnomalyDetectionConfigManager.class);
    globalAnomalyConfigStatusManager = mock(GlobalAnomalyConfigStatusManager.class);
    modsecManager =
        new ModsecManagerImpl(
            modsecRulesRegistry, anomalyDetectionConfigManager, globalAnomalyConfigStatusManager);
    requestContext = RequestContext.forTenantId("default tenant");
  }

  @Test
  @DisplayName("Should return same rule type")
  void getModsecCrsRules() {
    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of()))
        .thenReturn("regular");
    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of()))
        .thenReturn("safe");
    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of()))
        .thenReturn("block");

    when(anomalyDetectionConfigManager.getScopedAnomalyDetectionConfig(
            eq(requestContext),
            eq(
                AnomalyConfigScope.newBuilder()
                    .setEnvironmentScope(
                        AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentId))
                    .build()),
            any()))
        .thenReturn(ScopedAnomalyDetectionConfig.getDefaultInstance());

    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext,
            AnomalyConfigScope.newBuilder()
                .setEnvironmentScope(
                    AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentId))
                .build()))
        .thenReturn(ScopedAnomalyConfigStatus.getDefaultInstance());

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
    List<ModsecCrsRulesData> response =
        modsecManager.getModsecCrsRules(
            requestContext,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            request,
            true,
            AnomalyConfigScope.newBuilder()
                .setEnvironmentScope(
                    AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentId))
                .build());
    assertEquals(expectedResponse.size(), response.size());
    assertTrue(response.containsAll(expectedResponse));
    assertTrue(expectedResponse.containsAll(response));

    // using disabled modsec rules
    when(anomalyDetectionConfigManager.getScopedAnomalyDetectionConfig(
            eq(requestContext), eq(AnomalyConfigScopeUtils.getDefaultCustomerConfigScope()), any()))
        .thenReturn(getModsecRules());
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, AnomalyConfigScopeUtils.getDefaultCustomerConfigScope()))
        .thenReturn(ScopedAnomalyConfigStatus.getDefaultInstance());

    when(modsecRulesRegistry.getModsecRuleInfos()).thenReturn(getModsecRuleInfoMap());
    // config status for subRule1 and subRule4 is disabled, subRule2 is blockingDisabled
    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of("subRule1", "subRule2", "subRule4")))
        .thenReturn("excluded_blocked");

    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of("subRule1", "subRule4")))
        .thenReturn("excluded_safe");

    request = List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);
    expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                .setModsecCrsRulesBlob("excluded_blocked")
                .build());
    response =
        modsecManager.getModsecCrsRules(
            requestContext,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            request,
            true,
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build());
    assertEquals(expectedResponse, response);

    request = List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
    expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                .setModsecCrsRulesBlob("excluded_safe")
                .build());
    response =
        modsecManager.getModsecCrsRules(
            requestContext,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            request,
            true,
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build());
    assertEquals(expectedResponse, response);

    // global config disabled
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, AnomalyConfigScopeUtils.getDefaultCustomerConfigScope()))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
                .build());
    request = List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
    expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                .build());
    response =
        modsecManager.getModsecCrsRules(
            requestContext,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            request,
            true,
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build());
    assertEquals(expectedResponse, response);

    // default blocking rules
    modsecRulesRegistry =
        new ModsecRulesRegistryImpl(
            new ConfigConverter(), new ModsecCrsRulesHandler(new ModsecRuleUtils()));

    when(anomalyDetectionConfigManager.getScopedAnomalyDetectionConfig(
            eq(requestContext), eq(AnomalyConfigScopeUtils.getDefaultCustomerConfigScope()), any()))
        .thenReturn(ScopedAnomalyDetectionConfig.getDefaultInstance());

    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, AnomalyConfigScopeUtils.getDefaultCustomerConfigScope()))
        .thenReturn(ScopedAnomalyConfigStatus.getDefaultInstance());
    modsecManager =
        new ModsecManagerImpl(
            modsecRulesRegistry, anomalyDetectionConfigManager, globalAnomalyConfigStatusManager);
    request = List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);
    // crsBlob is empty
    expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                .build());
    response =
        modsecManager.getModsecCrsRules(
            requestContext,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            request,
            true,
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build());
    assertEquals(expectedResponse, response);
  }

  private ScopedAnomalyDetectionConfig getModsecRules() {
    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subRule1")
                                    .setBlockingEnabled(true))
                            .build())
                    .build())
            .build();

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule2")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder().setSubRuleId("subRule2"))
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subRule3")
                                    .setBlockingEnabled(true))
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subRule4")
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(true)
                                            .build())
                                    .setBlockingEnabled(true))
                            .build())
                    .build())
            .build();

    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(detectionConfig1)
        .addAnomalyDetectionConfigs(detectionConfig2)
        .build();
  }

  private Map<String, AnomalyRuleInfo> getModsecRuleInfoMap() {
    return Map.of(
        "rule1",
        AnomalyRuleInfo.newBuilder()
            .addSubRuleInfos(AnomalySubRuleInfo.newBuilder().setRuleId("subRule1").build())
            .build(),
        "rule2",
        AnomalyRuleInfo.newBuilder()
            .addSubRuleInfos(
                AnomalySubRuleInfo.newBuilder()
                    .setRuleId("subRule2")
                    .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                    .build())
            .addSubRuleInfos(
                AnomalySubRuleInfo.newBuilder()
                    .setRuleId("subRule3")
                    .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                    .build())
            .addSubRuleInfos(
                AnomalySubRuleInfo.newBuilder()
                    .setRuleId("subRule4")
                    .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                    .build())
            .build());
  }
}
