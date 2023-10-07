package ai.traceable.ast.config.service.configs;

import com.google.protobuf.Duration;
import com.typesafe.config.Config;

public class AstConfigServiceConfig {
  private final Config config;
  private static final String AST_CONFIG_SERVICE = "ast.config.service";
  private static final String DEFAULT_PURGE_DURATION_KEY = "defaultPurgeDuration";

  public AstConfigServiceConfig(Config config) {
    this.config = config.getConfig(AST_CONFIG_SERVICE);
  }

  public Duration getDefaultPurgeDuration() {
    java.time.Duration defaultPurgeDuration = this.config.getDuration(DEFAULT_PURGE_DURATION_KEY);
    return Duration.newBuilder().setSeconds(defaultPurgeDuration.getSeconds()).build();
  }
}
