package ai.traceable.customsignature.config.service;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class CustomSignatureConfigServiceConfig {
  private final Config config;
  private static final String CUSTOM_SIGNATURE_CONFIG_SERVICE = "custom.signature.config.service";
  private static final String SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG = "shouldPublishActivityEvents";
  private static final String MODSEC_RULE_VERSION_CONFIG = "modsecurity.rule.version";

  public CustomSignatureConfigServiceConfig(Config config) {
    this.config = config.getConfig(CUSTOM_SIGNATURE_CONFIG_SERVICE);
  }

  public boolean shouldPublishActivityEvents() {
    return this.config.getBoolean(SHOULD_PUBLISH_ACTIVITY_EVENTS_CONFIG);
  }

  public ModsecRuleVersion getModsecRuleVersion() {
    try {
      return this.config.getEnum(ModsecRuleVersion.class, MODSEC_RULE_VERSION_CONFIG);
    } catch (ConfigException e) {
      return ModsecRuleVersion.MODSEC_RULE_VERSION_V3;
    }
  }
}
