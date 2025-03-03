package ai.traceable.bot.categorized.config.service.v1;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfig.Builder;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class CategorizedBotDetailsConfig {

  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String DEFAULT_TRACEABLE_CATEGORIZED_BOTS_FILE_PATH =
      "default-traceable-categorized-bots.conf";
  private static final String TRACEABLE_CATEGORIZED_BOTS_PATH = "traceableCategorizedBots";
  private final Collection<CategorizedBotConfig> categorizedBotConfigs;

  public CategorizedBotDetailsConfig() {
    this.categorizedBotConfigs =
        ConfigFactory.parseResources(DEFAULT_TRACEABLE_CATEGORIZED_BOTS_FILE_PATH)
            .getConfigList(TRACEABLE_CATEGORIZED_BOTS_PATH)
            .stream()
            .map(
                config -> {
                  final Builder categorizedBotConfigBuilder = CategorizedBotConfig.newBuilder();
                  mergeFromConfig(config, categorizedBotConfigBuilder);
                  return categorizedBotConfigBuilder.build();
                })
            .collect(Collectors.toUnmodifiableList());
  }

  public Collection<CategorizedBotConfig> getAllTraceableCategorizedBots() {
    return categorizedBotConfigs;
  }

  @SneakyThrows
  private void mergeFromConfig(final Config config, final Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
