package ai.traceable.anomaly.config.service.modsec.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.ruleinfo.WebAppRuleInfoProvider;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecCrsRulesHandler;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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
  private WebAppRuleInfoProvider webAppRuleInfoProvider;
  private FeatureCachingClient featureCachingClient;

  @BeforeEach
  void setUp() {
    modsecRulesRegistry = mock(ModsecRulesRegistryImpl.class);
    anomalyDetectionConfigManager = mock(AnomalyDetectionConfigManager.class);
    globalAnomalyConfigStatusManager = mock(GlobalAnomalyConfigStatusManager.class);
    webAppRuleInfoProvider = mock(WebAppRuleInfoProvider.class);
    featureCachingClient = mock(FeatureCachingClient.class);

    modsecManager =
        new ModsecManagerImpl(
            modsecRulesRegistry,
            webAppRuleInfoProvider,
            featureCachingClient,
            anomalyDetectionConfigManager,
            globalAnomalyConfigStatusManager);
    requestContext = RequestContext.forTenantId("default tenant");
  }

  @Test
  @DisplayName("Should return same rule type")
  void getModsecCrsRules() {
    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR),
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of(),
            false))
        .thenReturn("regular");
    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE),
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of(),
            false))
        .thenReturn("safe");
    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK),
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of(),
            false))
        .thenReturn("block");
    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            argThat(list -> list.size() > 1),
            eq(ModsecRuleVersion.MODSEC_RULE_VERSION_V3),
            eq(Set.of()),
            anyBoolean()))
        .thenReturn("combined");

    {
      ModsecManager.ModsecCrsRules crsRules =
          modsecManager.getModsecCrsRules(
              List.of(),
              ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
              false,
              RuleVersion.getDefaultInstance(),
              true);
      List<ModsecCrsRulesData> expectedResponse =
          ImmutableList.of(
              ModsecCrsRulesData.newBuilder()
                  .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                  .setModsecCrsRulesBlob("block")
                  .build(),
              ModsecCrsRulesData.newBuilder()
                  .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                  .setModsecCrsRulesBlob("safe")
                  .build(),
              ModsecCrsRulesData.newBuilder()
                  .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
                  .setModsecCrsRulesBlob("regular")
                  .build());
      verifyLists(expectedResponse, crsRules);
    }

    when(anomalyDetectionConfigManager.getGlobalResolvedScopedAnomalyDetectionConfig(
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

    List<ModsecCrsRulesData> expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                .setModsecCrsRulesBlob("block")
                .build(),
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                .setModsecCrsRulesBlob("safe")
                .build());
    ModsecManager.ModsecCrsRules crsRules =
        modsecManager.getModsecCrsRules(
            requestContext,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING,
            List.of(),
            true,
            AnomalyConfigScope.newBuilder()
                .setEnvironmentScope(
                    AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentId))
                .build());
    verifyLists(expectedResponse, crsRules);

    // using disabled modsec rules
    when(anomalyDetectionConfigManager.getGlobalResolvedScopedAnomalyDetectionConfig(
            eq(requestContext), eq(AnomalyConfigScopeUtils.getDefaultCustomerConfigScope()), any()))
        .thenReturn(getModsecRules());
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, AnomalyConfigScopeUtils.getDefaultCustomerConfigScope()))
        .thenReturn(ScopedAnomalyConfigStatus.getDefaultInstance());

    when(modsecRulesRegistry.getModsecRuleInfos(any(), anyBoolean()))
        .thenReturn(getModsecRuleInfoMap());
    // config status for subRule1 and subRule4 is disabled, subRule2 is blockingDisabled
    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK),
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of("subRule1", "subRule2", "subRule4"),
            false))
        .thenReturn("excluded_blocked");

    when(modsecRulesRegistry.getModsecCrsRulesBlob(
            List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE),
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            Set.of("subRule1", "subRule4"),
            false))
        .thenReturn("excluded_safe");

    List<AnomalySubRuleType> subRuleTypes = List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);
    expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                .setModsecCrsRulesBlob("excluded_blocked")
                .build());
    crsRules =
        modsecManager.getModsecCrsRules(
            requestContext,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_UNSPECIFIED,
            subRuleTypes,
            true,
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build());
    verifyLists(expectedResponse, crsRules);

    subRuleTypes = List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
    expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                .setModsecCrsRulesBlob("excluded_safe")
                .build());
    crsRules =
        modsecManager.getModsecCrsRules(
            requestContext,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_UNSPECIFIED,
            subRuleTypes,
            true,
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build());
    verifyLists(expectedResponse, crsRules);

    // default blocking rules
    modsecRulesRegistry =
        new ModsecRulesRegistryImpl(
            new ConfigConverter(), new ModsecCrsRulesHandler(new ModsecRuleUtils()));

    when(anomalyDetectionConfigManager.getGlobalResolvedScopedAnomalyDetectionConfig(
            eq(requestContext), eq(AnomalyConfigScopeUtils.getDefaultCustomerConfigScope()), any()))
        .thenReturn(ScopedAnomalyDetectionConfig.getDefaultInstance());

    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, AnomalyConfigScopeUtils.getDefaultCustomerConfigScope()))
        .thenReturn(ScopedAnomalyConfigStatus.getDefaultInstance());
    modsecManager =
        new ModsecManagerImpl(
            modsecRulesRegistry,
            webAppRuleInfoProvider,
            featureCachingClient,
            anomalyDetectionConfigManager,
            globalAnomalyConfigStatusManager);
    subRuleTypes = List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);
    // crsBlob is empty
    expectedResponse =
        ImmutableList.of(
            ModsecCrsRulesData.newBuilder()
                .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                .build());
    crsRules =
        modsecManager.getModsecCrsRules(
            requestContext,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_UNSPECIFIED,
            subRuleTypes,
            true,
            AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build());
    verifyLists(expectedResponse, crsRules);
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
                                    .setBlockingEnabled(true)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
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
                                    .setBlockingEnabled(true)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subRule4")
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE)
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

  private void verifyLists(
      List<ModsecCrsRulesData> expected, ModsecManager.ModsecCrsRules crsRules) {
    List<ModsecCrsRulesData> actual = crsRules.getModsecCrsRulesData();
    assertEquals(expected.size(), actual.size());
    Map<AnomalySubRuleType, ModsecCrsRulesData> expectedMap =
        expected.stream()
            .collect(Collectors.toMap(ModsecCrsRulesData::getSubRuleType, Function.identity()));
    actual.forEach(data -> assertEquals(expectedMap.get(data.getSubRuleType()), data));
    if (expected.size() > 1) {
      assertEquals("combined", crsRules.getAggregatedModsecBlob());
    } else {
      assertEquals(expected.get(0).getModsecCrsRulesBlob(), crsRules.getAggregatedModsecBlob());
    }
  }
}
