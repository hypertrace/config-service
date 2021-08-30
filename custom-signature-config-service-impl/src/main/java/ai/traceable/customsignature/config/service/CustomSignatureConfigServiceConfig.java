package ai.traceable.customsignature.config.service;

import com.typesafe.config.Config;

public class CustomSignatureConfigServiceConfig {
  private final Config config;
  private static final String CUSTOM_SIGNATURE_CONFIG_SERVICE = "custom.signature.config.service";
  private static final String SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG = "shouldPublishActivityEvents";

  public CustomSignatureConfigServiceConfig(Config config) {
    this.config = config.getConfig(CUSTOM_SIGNATURE_CONFIG_SERVICE);
  }

  public boolean shouldPublishActivityEvents() {
    return this.config.getBoolean(SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG);
  }
}
