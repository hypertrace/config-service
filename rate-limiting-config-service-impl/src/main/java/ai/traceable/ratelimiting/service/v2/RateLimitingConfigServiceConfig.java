package ai.traceable.ratelimiting.service.v2;

import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;

public class RateLimitingConfigServiceConfig {
  private final Config config;
  private static final String RATE_LIMITING_CONFIG_SERVICE = "rate.limiting.config.service";
  private static final String DEFAULT_ENUMERATION_RULES_FILE_PATH =
      "default-enumeration-rules.conf";
  private static final String DEFAULT_DATA_ACCESS_RULES_FILE_PATH =
      "default-data-access-rules.conf";
  private static final String DEFAULT_RATE_LIMITING_RULES_FILE_PATH =
      "default-rate-limiting-rules.conf";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String ENUMERATION_RULES_PATH = "enumerationRules";
  private static final String DATA_ACCESS_RULES_PATH = "dataAccessRules";
  private static final String RATE_LIMITING_RULES_PATH = "rateLimitingRules";
  private static final String MIGRATION_DISABLED_KEY = "migrationDisabled";
  private static final String CHANGE_LOG_1_MIGRATION_DISABLED_KEY =
      "changeLog1." + MIGRATION_DISABLED_KEY;
  private final List<RateLimitingRule> defaultRateLimitingRules;
  private final boolean changeLog1MigrationDisabled;

  public RateLimitingConfigServiceConfig(Config config) {
    this.config = config.getConfig(RATE_LIMITING_CONFIG_SERVICE);
    defaultRateLimitingRules =
        Stream.of(
                this.convert(
                    ConfigFactory.parseResources(DEFAULT_ENUMERATION_RULES_FILE_PATH)
                        .getConfigList(ENUMERATION_RULES_PATH)),
                this.convert(
                    ConfigFactory.parseResources(DEFAULT_DATA_ACCESS_RULES_FILE_PATH)
                        .getConfigList(DATA_ACCESS_RULES_PATH)),
                this.convert(
                    ConfigFactory.parseResources(DEFAULT_RATE_LIMITING_RULES_FILE_PATH)
                        .getConfigList(RATE_LIMITING_RULES_PATH)))
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableList());
    changeLog1MigrationDisabled =
        config.hasPath(CHANGE_LOG_1_MIGRATION_DISABLED_KEY)
            && config.getBoolean(CHANGE_LOG_1_MIGRATION_DISABLED_KEY);
  }

  public List<RateLimitingRule> getDefaultRateLimitingRules() {
    return defaultRateLimitingRules;
  }

  public boolean isChangeLog1MigrationDisabled() {
    return changeLog1MigrationDisabled;
  }

  private List<RateLimitingRule> convert(List<? extends Config> configList) {
    return configList.stream()
        .map(
            config -> {
              RateLimitingRule.Builder builder = RateLimitingRule.newBuilder();
              mergeFromConfig(config, builder);
              return builder.build();
            })
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private void mergeFromConfig(Config config, Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
