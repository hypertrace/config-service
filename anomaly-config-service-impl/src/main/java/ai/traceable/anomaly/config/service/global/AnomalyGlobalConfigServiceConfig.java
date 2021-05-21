package ai.traceable.anomaly.config.service.global;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import com.typesafe.config.Config;

public class AnomalyGlobalConfigServiceConfig {

  private static final String DISABLED_CONFIG_PATH = "disabled";
  private static final String INTERNAL_CONFIG_PATH = "internal";

  private final boolean disabled;
  private final boolean internal;

  public AnomalyGlobalConfigServiceConfig(Config config) {
    this.disabled = config.getBoolean(DISABLED_CONFIG_PATH);
    this.internal = config.getBoolean(INTERNAL_CONFIG_PATH);
  }

  public AnomalyConfigStatus getConfigStatus() {
    return AnomalyConfigStatus.newBuilder().setDisabled(disabled).setInternal(internal).build();
  }
}
