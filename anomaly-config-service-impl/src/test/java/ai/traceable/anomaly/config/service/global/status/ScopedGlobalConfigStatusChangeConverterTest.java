package ai.traceable.anomaly.config.service.global.status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionDataChange;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.global.ApiDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfigChange;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScopedGlobalConfigStatusChangeConverterTest {

  private ScopedGlobalConfigStatusChangeConverter converter;
  private AnomalyGlobalConfigServiceConfig defaultConfig;

  @BeforeEach
  public void setup() {
    converter = new ScopedGlobalConfigStatusChangeConverter();
    defaultConfig = mock(AnomalyGlobalConfigServiceConfig.class);

    when(defaultConfig.getNewWebAppStableVersion())
        .thenReturn(getRuleVersion("1.2.0", RuleVersionType.RULE_VERSION_TYPE_STABLE));
    when(defaultConfig.getOldWebAppStableVersion())
        .thenReturn(getRuleVersion("1.1.0", RuleVersionType.RULE_VERSION_TYPE_STABLE));
    when(defaultConfig.getMinConfidenceLevel())
        .thenReturn(AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH);
    when(defaultConfig.getModsecDefaultConfigsType())
        .thenReturn(ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD_MONITORING);
    when(defaultConfig.getApiDefaultConfigsType())
        .thenReturn(ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED);
  }

  @Test
  public void testConvertScopedConfig() {
    ScopedAnomalyConfigStatusChange config =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigScope(getSampleScopes().get(0))
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .build();

    ScopedAnomalyConfigStatus result =
        converter.convertScopedConfig(
            config, defaultConfig, AnomalyConfigStatus.getDefaultInstance());

    GlobalModsecConfig modSecGlobalConfig = result.getGlobalModsecConfig();
    assertNotNull(modSecGlobalConfig);
    assertEquals(
        defaultConfig.getModsecDefaultConfigsType(), modSecGlobalConfig.getDefaultConfigsType());
    assertEquals(defaultConfig.getMinConfidenceLevel(), modSecGlobalConfig.getMinConfidenceLevel());
    assertEquals("1.2.0", modSecGlobalConfig.getRuleVersionData().getCurrentVersion().getVersion());
    assertEquals(
        RuleVersionType.RULE_VERSION_TYPE_STABLE,
        modSecGlobalConfig.getRuleVersionData().getCurrentVersion().getVersionType());
  }

  @Test
  public void testConvertScopedConfigWithCustomConfig() {
    GlobalModsecConfigChange modsecConfigChange =
        GlobalModsecConfigChange.newBuilder()
            .setDisabled(true)
            .setBlockingAvailableForRegularRules(true)
            .setUseTestRules(true)
            .setMinConfidenceLevel(AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_LOW)
            .setDefaultConfigsType(
                ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD_MONITORING)
            .setRuleVersionDataChange(
                RuleVersionDataChange.newBuilder()
                    .setOverrideVersion(RuleVersion.getDefaultInstance()))
            .build();

    ScopedAnomalyConfigStatusChange config =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigScope(getSampleScopes().get(0))
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setGlobalModsecConfigChange(modsecConfigChange)
            .build();

    ScopedAnomalyConfigStatus result =
        converter.convertScopedConfig(
            config, defaultConfig, AnomalyConfigStatus.getDefaultInstance());

    GlobalModsecConfig modSecGlobalConfig = result.getGlobalModsecConfig();
    assertNotNull(modSecGlobalConfig);
    assertTrue(modSecGlobalConfig.getDisabled());
    assertTrue(modSecGlobalConfig.getBlockingAvailableForRegularRules());
    assertTrue(modSecGlobalConfig.getUseTestRules());
    assertEquals(
        AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_LOW,
        modSecGlobalConfig.getMinConfidenceLevel());
    assertEquals(
        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD_MONITORING,
        modSecGlobalConfig.getDefaultConfigsType());
    assertEquals("1.2.0", modSecGlobalConfig.getRuleVersionData().getCurrentVersion().getVersion());
    assertEquals(
        RuleVersionType.RULE_VERSION_TYPE_STABLE,
        modSecGlobalConfig.getRuleVersionData().getCurrentVersion().getVersionType());
  }

  @Test
  public void testIsNullOrDefaultHelpers()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method isNullOrDefaultModsec =
        ScopedGlobalConfigStatusChangeConverter.class.getDeclaredMethod(
            "isNullOrDefault", GlobalModsecConfigChange.class);
    isNullOrDefaultModsec.setAccessible(true);
    assertEquals(true, isNullOrDefaultModsec.invoke(null, (GlobalModsecConfigChange) null));
    assertEquals(
        true, isNullOrDefaultModsec.invoke(null, GlobalModsecConfigChange.getDefaultInstance()));
    assertEquals(
        false,
        isNullOrDefaultModsec.invoke(
            null, GlobalModsecConfigChange.newBuilder().setDisabled(true).build()));
  }

  private List<AnomalyConfigScope> getSampleScopes() {
    return List.of(
        AnomalyConfigScope.newBuilder()
            .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
            .build(),
        AnomalyConfigScope.newBuilder()
            .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
            .build(),
        AnomalyConfigScope.newBuilder()
            .setApiScope(
                AnomalyApiScope.newBuilder()
                    .setId("api1")
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service1").build())
                    .build())
            .build());
  }

  private RuleVersion getRuleVersion(String version, RuleVersionType type) {
    return RuleVersion.newBuilder()
        .setVersion(version)
        .setVersionType(type)
        .setPublishedDate("2023-10-01")
        .build();
  }
}
