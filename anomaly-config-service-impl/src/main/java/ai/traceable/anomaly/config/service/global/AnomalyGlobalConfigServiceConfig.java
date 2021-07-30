package ai.traceable.anomaly.config.service.global;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class AnomalyGlobalConfigServiceConfig {

  private static final String DISABLED_CONFIG_PATH = "disabled";
  private static final String INTERNAL_CONFIG_PATH = "internal";
  private static final String LICENSE_TIERS_CONFIG_PATH = "licenseTiers";
  private static final String TIER_CONFIG_PATH = "tier";

  private final boolean disabled;
  private final boolean internal;
  private final Map<LicenseInfo.Tier, Boolean> licenseTiersConfigStatusMap;

  public AnomalyGlobalConfigServiceConfig(Config config) {
    this.disabled = config.getBoolean(DISABLED_CONFIG_PATH);
    this.internal = config.getBoolean(INTERNAL_CONFIG_PATH);
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
}
