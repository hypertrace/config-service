package ai.traceable.ast.config.service.configs;

import ai.traceable.ast.config.service.v1.AstEnabledConfig;
import ai.traceable.ast.config.service.v1.AstReplayConfig;
import com.google.protobuf.Duration;
import com.typesafe.config.Config;

public class AstConfigServiceConfig {
  private final Config config;
  private static final String AST_CONFIG_SERVICE = "ast.config.service";
  private static final String DEFAULT_PURGE_DURATION_KEY = "defaultPurgeDuration";
  private static final String DEFAULT_SCAN_RETENTION_LIMIT_PER_SUITE =
      "defaultScanRetentionLimitPerSuite";
  private static final String DEFAULT_AST_FEATURE_CONFIG_KEY = "defaultAstFeatureConfig";
  private static final String AST_ENABLED_KEY = "astEnabled";
  private static final String AST_REPLAY_CONFIG_KEY = "astReplayConfig";
  private static final String ENABLED_KEY = "enabled";
  private static final String DOT = ".";
  private static final String API_INACTIVITY_DURATION_KEY = "apiInactivityDuration";
  private static final String MAX_API_LIMIT_KEY = "maxApiLimit";
  private static final String SKIP_NON_LEARNT_APIS_KEY = "skipNonLearntApis";
  private static final String SKIP_UNDER_DISCOVERY_APIS_KEY = "skipUnderDiscoveryApis";

  public AstConfigServiceConfig(Config config) {
    this.config = config.getConfig(AST_CONFIG_SERVICE);
  }

  public Duration getDefaultPurgeDuration() {
    java.time.Duration defaultPurgeDuration = this.config.getDuration(DEFAULT_PURGE_DURATION_KEY);
    return Duration.newBuilder().setSeconds(defaultPurgeDuration.getSeconds()).build();
  }

  public int getDefaultScanRetentionLimitPerSuite() {
    return this.config.getInt(DEFAULT_SCAN_RETENTION_LIMIT_PER_SUITE);
  }

  public boolean defaultIsAstEnabled() {
    Config defaultAstFeatureConfig = config.getConfig(DEFAULT_AST_FEATURE_CONFIG_KEY);
    return defaultAstFeatureConfig.hasPath(AST_ENABLED_KEY);
  }

  public AstEnabledConfig getDefaultAstEnabledConfig() {
    Config defaultAstFeatureConfig = config.getConfig(DEFAULT_AST_FEATURE_CONFIG_KEY);
    java.time.Duration defaultApiInactivityDuration =
        defaultAstFeatureConfig.getDuration(
            AST_REPLAY_CONFIG_KEY + DOT + API_INACTIVITY_DURATION_KEY);

    return AstEnabledConfig.newBuilder()
        .setReplayConfig(
            AstReplayConfig.newBuilder()
                .setReplayEnabled(
                    defaultAstFeatureConfig.getBoolean(AST_REPLAY_CONFIG_KEY + DOT + ENABLED_KEY))
                .setApiInactivityDuration(
                    Duration.newBuilder()
                        .setSeconds(defaultApiInactivityDuration.getSeconds())
                        .build())
                .setMaxApiLimit(
                    defaultAstFeatureConfig.getInt(AST_REPLAY_CONFIG_KEY + DOT + MAX_API_LIMIT_KEY))
                .setSkipNonLearntApis(
                    defaultAstFeatureConfig.getBoolean(
                        AST_REPLAY_CONFIG_KEY + DOT + SKIP_NON_LEARNT_APIS_KEY))
                .setSkipUnderDiscoveryApis(
                    defaultAstFeatureConfig.getBoolean(
                        AST_REPLAY_CONFIG_KEY + DOT + SKIP_UNDER_DISCOVERY_APIS_KEY))
                .build())
        .build();
  }
}
