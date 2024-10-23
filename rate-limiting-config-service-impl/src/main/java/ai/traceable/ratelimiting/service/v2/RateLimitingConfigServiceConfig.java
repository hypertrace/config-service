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
  private static final String SHOULD_PUBLISH_ACTIVITY_EVENTS = "shouldPublishActivityEvents";
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
  private final List<RateLimitingRule> defaultRateLimitingRules;

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
  }

  public boolean shouldPublishActivityEvents() {
    return this.config.hasPath(SHOULD_PUBLISH_ACTIVITY_EVENTS)
        && this.config.getBoolean(SHOULD_PUBLISH_ACTIVITY_EVENTS);
  }

  public List<RateLimitingRule> getDefaultRateLimitingRules() {
    return defaultRateLimitingRules;
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
