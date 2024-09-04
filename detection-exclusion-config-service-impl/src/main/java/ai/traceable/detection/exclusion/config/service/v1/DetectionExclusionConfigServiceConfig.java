package ai.traceable.detection.exclusion.config.service.v1;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class DetectionExclusionConfigServiceConfig {
  private static final String DEFAULT_EXCLUSION_RULES_FILE_PATH = "default-exclusion-rules.conf";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String DETECTION_EXCLUSION_RULES_PATH = "detectionExclusionRules";

  private static final String DETECTION_EXCLUSION_CONFIG_KEY = "detection.exclusion.config.service";
  private static final String MIGRATION_DISABLED_KEY = "migrationDisabled";
  private static final String CHANGE_LOG_2_MIGRATION_DISABLED_KEY =
      "changeLog2." + MIGRATION_DISABLED_KEY;
  private static final String CHANGE_LOG_3_MIGRATION_DISABLED_KEY =
      "changeLog3." + MIGRATION_DISABLED_KEY;
  private static final String CHANGE_LOG_4_MIGRATION_DISABLED_KEY =
      "changeLog4." + MIGRATION_DISABLED_KEY;

  private final Config config;
  private final boolean migrationDisabled;
  private final boolean changeLog2MigrationDisabled;
  private final boolean changeLog3MigrationDisabled;
  private final boolean changeLog4MigrationDisabled;

  public DetectionExclusionConfigServiceConfig(Config config) {
    this.config =
        config.hasPath(DETECTION_EXCLUSION_CONFIG_KEY)
            ? config.getConfig(DETECTION_EXCLUSION_CONFIG_KEY)
            : ConfigFactory.empty();
    migrationDisabled =
        config.hasPath(MIGRATION_DISABLED_KEY) && config.getBoolean(MIGRATION_DISABLED_KEY);
    changeLog2MigrationDisabled =
        config.hasPath(CHANGE_LOG_2_MIGRATION_DISABLED_KEY)
            && config.getBoolean(CHANGE_LOG_2_MIGRATION_DISABLED_KEY);
    changeLog3MigrationDisabled =
        config.hasPath(CHANGE_LOG_3_MIGRATION_DISABLED_KEY)
            && config.getBoolean(CHANGE_LOG_3_MIGRATION_DISABLED_KEY);
    changeLog4MigrationDisabled =
        config.hasPath(CHANGE_LOG_4_MIGRATION_DISABLED_KEY)
            && config.getBoolean(CHANGE_LOG_4_MIGRATION_DISABLED_KEY);
  }

  public List<DetectionExclusionRule> getDefaultDetectionExclusionRules() {
    return this.convertToDetectionExclusionRules(
        ConfigFactory.parseResources(DEFAULT_EXCLUSION_RULES_FILE_PATH)
            .getConfigList(DETECTION_EXCLUSION_RULES_PATH));
  }

  public boolean isMigrationDisabled() {
    return migrationDisabled;
  }

  public boolean isChangeLog2MigrationDisabled() {
    return changeLog2MigrationDisabled;
  }

  public boolean isChangeLog3MigrationDisabled() {
    return changeLog3MigrationDisabled;
  }

  public boolean isChangeLog4MigrationDisabled() {
    return changeLog4MigrationDisabled;
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
