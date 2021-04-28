package ai.traceable.customsignature.config.service;

import com.typesafe.config.Config;

public class CustomSignatureConfigServiceConfig {
  private final Config config;

  public CustomSignatureConfigServiceConfig(Config config) {
    this.config = config;
  }

  public String getModsecDirectivesDataPath() {
    return this.config.getString("modsecurity.directives.data.path");
  }
}
