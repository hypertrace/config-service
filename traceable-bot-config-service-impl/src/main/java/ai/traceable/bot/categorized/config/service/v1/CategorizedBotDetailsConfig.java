package ai.traceable.bot.categorized.config.service.v1;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfig.Builder;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CategorizedBotDetailsConfig {
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String DEFAULT_TRACEABLE_CATEGORIZED_BOTS_FILE_PATH =
      "default-traceable-categorized-bots.conf";
  private static final String TRACEABLE_CATEGORIZED_BOTS_PATH = "traceableCategorizedBots";
  public static final CategorizedBotDetailsConfig INSTANCE = new CategorizedBotDetailsConfig();

  private final Collection<CategorizedBotConfig> categorizedBotConfigs;

  private CategorizedBotDetailsConfig() {
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
            .collect(Collectors.toList());

    // Validate that each bot's sub-category belongs to its category
    validateBotConfigurations(categorizedBotConfigs);
  }

  /**
   * Validates that each bot configuration has a valid sub-category for its category. Throws an
   * exception if any bot configuration is invalid.
   *
   * @param botConfigs Collection of bot configurations to validate
   * @throws IllegalStateException if any bot has an invalid category or subcategory configuration
   */
  private void validateBotConfigurations(Collection<CategorizedBotConfig> botConfigs) {
    // Validate each bot configuration
    for (CategorizedBotConfig botConfig : botConfigs) {
      final String categoryId = botConfig.getCategorizedBotDetails().getBotCategoryId();
      final String subCategoryId = botConfig.getCategorizedBotDetails().getBotSubCategoryId();
      // Check if category exists
      final Set<String> botSubCategoryIdsForCategory =
          CategorizedBotCategoriesConfig.INSTANCE.getBotSubCategoryIdsForCategory(categoryId);
      if (Objects.isNull(botSubCategoryIdsForCategory)
          || botSubCategoryIdsForCategory.isEmpty()
          || !botSubCategoryIdsForCategory.contains(subCategoryId)) {
        log.error(
            "Invalid bot configuration: Category ID {} does not exist for bot '%s'%n",
            categoryId, botConfig.getId());
        throw new IllegalStateException("Bot configuration validation failed");
      }
    }
  }

  public Collection<CategorizedBotConfig> getAllTraceableCategorizedBots() {
    return categorizedBotConfigs;
  }

  @SneakyThrows
  private void mergeFromConfig(final Config config, final Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
