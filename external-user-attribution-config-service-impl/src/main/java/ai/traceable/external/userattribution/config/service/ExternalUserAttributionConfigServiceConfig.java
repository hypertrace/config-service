package ai.traceable.external.userattribution.config.service;

import com.typesafe.config.Config;

public class ExternalUserAttributionConfigServiceConfig {
  private final Config config;
  private static final String EXTERNAL_USER_ATTRIBUTION_CONFIG_SERVICE_CONFIG =
      "external.user.attribution.config.service.config";
  private static final String PARSING_RULES_CONFIG = "parsing.rules";

  public ExternalUserAttributionConfigServiceConfig(Config config) {
    this.config = config.getConfig(EXTERNAL_USER_ATTRIBUTION_CONFIG_SERVICE_CONFIG);
  }

  public Config getParsingRulesConfig() {
    return this.config.getConfig(PARSING_RULES_CONFIG);
  }
}
