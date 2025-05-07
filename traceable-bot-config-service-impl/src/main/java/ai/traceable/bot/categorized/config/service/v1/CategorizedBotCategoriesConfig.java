package ai.traceable.bot.categorized.config.service.v1;

import ai.traceable.bot.categorized.config.service.v1.BotCategory.Builder;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class CategorizedBotCategoriesConfig {
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String TRACEABLE_CATEGORIZED_BOT_CATEGORIES_FILE_PATH =
      "bot_categories.conf";
  private static final String TRACEABLE_CATEGORIZED_BOT_CATEGORIES_PATH = "botCategories";
  public static final CategorizedBotCategoriesConfig INSTANCE =
      new CategorizedBotCategoriesConfig();

  private final Collection<BotCategory> categorizedBotConfigs;
  private final Map<String, BotCategory> botCategoryMap;
  private final Map<String, BotSubCategory> botSubCategoryMap;
  private final Map<String, Set<String>> botCategorySubCategoryMap;

  private CategorizedBotCategoriesConfig() {
    this.categorizedBotConfigs =
        ConfigFactory.parseResources(TRACEABLE_CATEGORIZED_BOT_CATEGORIES_FILE_PATH)
            .getConfigList(TRACEABLE_CATEGORIZED_BOT_CATEGORIES_PATH)
            .stream()
            .map(
                config -> {
                  final Builder categorizedBotCategoryBuilder = BotCategory.newBuilder();
                  mergeFromConfig(config, categorizedBotCategoryBuilder);
                  return categorizedBotCategoryBuilder.build();
                })
            .collect(Collectors.toUnmodifiableList());
    this.botCategoryMap =
        categorizedBotConfigs.stream()
            .collect(Collectors.toUnmodifiableMap(BotCategory::getId, Function.identity()));
    this.botSubCategoryMap =
        categorizedBotConfigs.stream()
            .flatMap(botCategory -> botCategory.getBotSubCategoriesList().stream())
            .collect(Collectors.toUnmodifiableMap(BotSubCategory::getId, Function.identity()));
    this.botCategorySubCategoryMap =
        categorizedBotConfigs.stream()
            .collect(
                Collectors.toMap(
                    BotCategory::getId,
                    botCategory ->
                        botCategory.getBotSubCategoriesList().stream()
                            .map(BotSubCategory::getId)
                            .collect(Collectors.toUnmodifiableSet())));
  }

  public Collection<BotCategory> getAllTraceableCategorizedBots() {
    return categorizedBotConfigs;
  }

  public BotCategory getBotCategory(final String id) {
    return botCategoryMap.get(id);
  }

  public Collection<BotCategory> getBotCategories() {
    return botCategoryMap.values();
  }

  public BotSubCategory getBotSubCategory(final String id) {
    return botSubCategoryMap.get(id);
  }

  public Collection<BotSubCategory> getAllBotSubCategories() {
    return botSubCategoryMap.values();
  }

  public Collection<BotSubCategory> getBotSubCategoriesForCategory(final String categoryId) {
    return botCategoryMap.get(categoryId).getBotSubCategoriesList();
  }

  public Set<String> getBotSubCategoryIdsForCategory(final String categoryId) {
    return botCategorySubCategoryMap.get(categoryId);
  }

  @SneakyThrows
  private void mergeFromConfig(final Config config, final Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
