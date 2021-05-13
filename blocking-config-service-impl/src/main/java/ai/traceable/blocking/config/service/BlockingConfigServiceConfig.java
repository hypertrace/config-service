package ai.traceable.blocking.config.service;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingConfigServiceConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingConfigServiceConfig.class);

  private static final String SAFECRS_MODSECURITY_RULES_PATH =
      "safecrs.modsecurity.rules.data.path";
  private final Config config;

  public BlockingConfigServiceConfig(Config config) {
    this.config = config;
  }

  public String getSafeCrsModsecRulesDataPath() {
    return this.config.getString(SAFECRS_MODSECURITY_RULES_PATH);
  }
}
