package ai.traceable.anomaly.config.service.global;

import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ApiDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

public class AnomalyGlobalConfigServiceConfig {

  private static final String DISABLED_CONFIG_PATH = "disabled";
  private static final String INTERNAL_CONFIG_PATH = "internal";
  private static final String CONFIDENCE_CONFIG_PATH = "minConfidenceLevel";
  private static final String LICENSE_TIERS_CONFIG_PATH = "licenseTiers";
  private static final String TIER_CONFIG_PATH = "tier";
  private static final String MODSEC_DEFAULT_CONFIG_TYPE = "modsecGlobalConfig.defaultConfigsType";
  private static final String ENVIRONMENT_SCOPE_MODSEC_DEFAULT_CONFIG_TYPE =
      "modsecGlobalConfig.environmentDefaultConfigType";
  private static final String API_DEFAULT_CONFIG_TYPE = "apiGlobalConfig.defaultConfigsType";

  private final boolean disabled;
  private final boolean internal;
  private final AnomalyConfidenceLevel minConfidenceLevel;
  private final Map<LicenseInfo.Tier, Boolean> licenseTiersConfigStatusMap;
  @Getter private final ModsecDefaultConfigsType modsecDefaultConfigsType;
  @Getter private final ModsecDefaultConfigsType envScopeModsecDefaultConfigsType;
  @Getter private final ApiDefaultConfigsType apiDefaultConfigsType;

  public AnomalyGlobalConfigServiceConfig(Config config) {
    this.disabled = config.getBoolean(DISABLED_CONFIG_PATH);
    this.internal = config.getBoolean(INTERNAL_CONFIG_PATH);
    this.modsecDefaultConfigsType =
        config.hasPath(MODSEC_DEFAULT_CONFIG_TYPE)
            ? config.getEnum(ModsecDefaultConfigsType.class, MODSEC_DEFAULT_CONFIG_TYPE)
            : ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD_MONITORING;
    this.envScopeModsecDefaultConfigsType =
        config.hasPath(ENVIRONMENT_SCOPE_MODSEC_DEFAULT_CONFIG_TYPE)
            ? config.getEnum(
                ModsecDefaultConfigsType.class, ENVIRONMENT_SCOPE_MODSEC_DEFAULT_CONFIG_TYPE)
            : ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_ENVIRONMENT;
    this.apiDefaultConfigsType =
        config.hasPath(API_DEFAULT_CONFIG_TYPE)
            ? config.getEnum(ApiDefaultConfigsType.class, API_DEFAULT_CONFIG_TYPE)
            : ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED;
    if (config.hasPath(CONFIDENCE_CONFIG_PATH)) {
      this.minConfidenceLevel =
          config.getEnum(AnomalyConfidenceLevel.class, CONFIDENCE_CONFIG_PATH);
    } else {
      this.minConfidenceLevel = AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH;
    }
    if (config.hasPath(LICENSE_TIERS_CONFIG_PATH)) {
      licenseTiersConfigStatusMap =
          config.getConfigList(LICENSE_TIERS_CONFIG_PATH).stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      tierConfig -> tierConfig.getEnum(LicenseInfo.Tier.class, TIER_CONFIG_PATH),
                      tierConfig -> tierConfig.getBoolean(DISABLED_CONFIG_PATH)));
    } else {
      licenseTiersConfigStatusMap = Collections.emptyMap();
    }
  }

  public AnomalyConfigStatus getConfigStatus(LicenseInfo.Tier tier) {
    return AnomalyConfigStatus.newBuilder()
        .setDisabled(
            licenseTiersConfigStatusMap.containsKey(tier)
                ? licenseTiersConfigStatusMap.get(tier)
                : disabled)
        .setInternal(internal)
        .build();
  }

  public AnomalyConfidenceLevel getMinConfidenceLevel() {
    return minConfidenceLevel;
  }
}
