package ai.traceable.detection.exclusion.config.service.v1;

import ai.traceable.audit.utils.UserVisibleEmailConfig;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;

public class DetectionExclusionConfigServiceConfig {
  private static final String DEFAULT_EXCLUSION_RULES_FILE_PATH = "default-exclusion-rules.conf";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String DETECTION_EXCLUSION_RULES_PATH = "detectionExclusionRules";
  private static final String DETECTION_NEW_EXCLUSION_RULES_PATH = "detectionExclusionNewRules";

  private static final String DETECTION_EXCLUSION_CONFIG_KEY = "detection.exclusion.config.service";
  private static final String DEFAULT_DETECTION_EXCLUSION_RULES_CONFIG_PATH =
      "defaultDetectionExclusionRules";
  private static final String MIGRATION_DISABLED_KEY = "migrationDisabled";
  private static final String CHANGE_LOG_2_MIGRATION_DISABLED_KEY =
      "changeLog2." + MIGRATION_DISABLED_KEY;
  private static final String CHANGE_LOG_3_MIGRATION_DISABLED_KEY =
      "changeLog3." + MIGRATION_DISABLED_KEY;
  private static final String CHANGE_LOG_4_MIGRATION_DISABLED_KEY =
      "changeLog4." + MIGRATION_DISABLED_KEY;
  private static final String RULE_EVALUATION_POINTS_MIGRATION_DISABLED_KEY =
      "ruleEvaluationPoints." + MIGRATION_DISABLED_KEY;
  private static final String ALLOW_ONLY_PLATFORM_REMOVAL_MIGRATION_DISABLED_KEY =
      "allowOnlyPlatformRemoval." + MIGRATION_DISABLED_KEY;

  private final Config config;
  @Getter private final boolean migrationDisabled;
  @Getter private final boolean changeLog2MigrationDisabled;
  @Getter private final boolean changeLog3MigrationDisabled;
  @Getter private final boolean changeLog4MigrationDisabled;
  @Getter private final boolean ruleEvaluationPointsMigrationDisabled;
  @Getter private final boolean allowOnlyPlatformRemovalMigrationDisabled;

  @Getter private final List<DetectionExclusionRule> defaultDetectionExclusionRules;
  @Getter private final List<DetectionExclusionRule> defaultNewDetectionExclusionRules;
  @Getter private final UserVisibleEmailConfig userVisibleEmailConfig;

  public DetectionExclusionConfigServiceConfig(Config config) {
    this.config =
        config.hasPath(DETECTION_EXCLUSION_CONFIG_KEY)
            ? config.getConfig(DETECTION_EXCLUSION_CONFIG_KEY)
            : ConfigFactory.empty();
    migrationDisabled =
        this.config.hasPath(MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(MIGRATION_DISABLED_KEY);
    changeLog2MigrationDisabled =
        this.config.hasPath(CHANGE_LOG_2_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(CHANGE_LOG_2_MIGRATION_DISABLED_KEY);
    changeLog3MigrationDisabled =
        this.config.hasPath(CHANGE_LOG_3_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(CHANGE_LOG_3_MIGRATION_DISABLED_KEY);
    changeLog4MigrationDisabled =
        this.config.hasPath(CHANGE_LOG_4_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(CHANGE_LOG_4_MIGRATION_DISABLED_KEY);
    this.defaultDetectionExclusionRules = loadDefaultDetectionExclusionRules(false);
    this.defaultNewDetectionExclusionRules = loadDefaultDetectionExclusionRules(true);
    this.ruleEvaluationPointsMigrationDisabled =
        this.config.hasPath(RULE_EVALUATION_POINTS_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(RULE_EVALUATION_POINTS_MIGRATION_DISABLED_KEY);
    this.allowOnlyPlatformRemovalMigrationDisabled =
        this.config.hasPath(ALLOW_ONLY_PLATFORM_REMOVAL_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(ALLOW_ONLY_PLATFORM_REMOVAL_MIGRATION_DISABLED_KEY);
    this.userVisibleEmailConfig = new UserVisibleEmailConfig(config);
  }

  private List<DetectionExclusionRule> loadDefaultDetectionExclusionRules(boolean loadNewRules) {
    List<DetectionExclusionRule> defaultDetectionExclusionRules = new ArrayList<>();
    if (config.hasPath(DEFAULT_DETECTION_EXCLUSION_RULES_CONFIG_PATH)) {
      defaultDetectionExclusionRules.addAll(
          convertToDetectionExclusionRules(
              config.getConfigList(DEFAULT_DETECTION_EXCLUSION_RULES_CONFIG_PATH)));
    }
    if (loadNewRules) {
      defaultDetectionExclusionRules.addAll(
          convertToDetectionExclusionRules(
              ConfigFactory.parseResources(DEFAULT_EXCLUSION_RULES_FILE_PATH)
                  .getConfigList(DETECTION_NEW_EXCLUSION_RULES_PATH)));
      return defaultDetectionExclusionRules;
    }
    defaultDetectionExclusionRules.addAll(
        convertToDetectionExclusionRules(
            ConfigFactory.parseResources(DEFAULT_EXCLUSION_RULES_FILE_PATH)
                .getConfigList(DETECTION_EXCLUSION_RULES_PATH)));
    return defaultDetectionExclusionRules;
  }

  private List<DetectionExclusionRule> convertToDetectionExclusionRules(
      List<? extends Config> configList) {
    return configList.stream()
        .map(
            config -> {
              DetectionExclusionRule.Builder builder = DetectionExclusionRule.newBuilder();
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
