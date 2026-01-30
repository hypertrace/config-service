package ai.traceable.customsignature.config.service;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.config.service.commons.utils.UserVisibleEmailConfig;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;

public class CustomSignatureConfigServiceConfig {
  private final Config config;
  @Getter private final List<CustomSignatureRule> defaultCustomSignatureRules;
  private static final String CUSTOM_SIGNATURE_CONFIG_SERVICE = "custom.signature.config.service";
  private static final String DEFAULT_CUSTOM_SIGNATURE_RULES_CONFIG_PATH =
      "defaultCustomSignatureRules";
  private static final String DEFAULT_CUSTOM_SIGNATURE_RULES_FILE_PATH =
      "default-custom-signature-rules.conf";
  private static final String DEFAULT_AI_APP_PROTECTION_RULES_CONFIG_PATH =
      "defaultAiAppProtectionRules";
  private static final String DEFAULT_AI_APP_PROTECTION_RULES_FILE_PATH =
      "default-ai-app-protection-rules.conf";
  private static final String MODSEC_RULE_VERSION_CONFIG = "modsecurity.rule.version";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String MIGRATION_DISABLED_KEY = "migrationDisabled";
  private static final String RULE_EVALUATION_POINTS_MIGRATION_DISABLED_KEY =
      "ruleEvaluationPoints." + MIGRATION_DISABLED_KEY;
  private static final String RULE_CATEGORY_MIGRATION_DISABLED_KEY =
      "ruleCategory." + MIGRATION_DISABLED_KEY;
  private static final String ALLOW_RULES_PLATFORM_EXCLUSION_MIGRATION_DISABLED_KEY =
      "allowRulesPlatformExclusion." + MIGRATION_DISABLED_KEY;
  private static final String MARK_FOR_TESTING_INLINE_AGENT_MIGRATION_DISABLED_KEY =
      "markForTestingInlineAgent." + MIGRATION_DISABLED_KEY;

  @Getter private final boolean ruleEvaluationPointsMigrationDisabled;
  @Getter private final boolean ruleCategoryMigrationDisabled;
  @Getter private final boolean allowRulesPlatformExclusionMigrationDisabled;
  @Getter private final boolean markForTestingInlineAgentMigrationDisabled;
  @Getter private final UserVisibleEmailConfig userVisibleEmailConfig;

  public CustomSignatureConfigServiceConfig(Config config) {
    this.config =
        config.hasPath(CUSTOM_SIGNATURE_CONFIG_SERVICE)
            ? config.getConfig(CUSTOM_SIGNATURE_CONFIG_SERVICE)
            : ConfigFactory.empty();
    this.defaultCustomSignatureRules = loadDefaultCustomSignatureRules();
    this.ruleEvaluationPointsMigrationDisabled =
        this.config.hasPath(RULE_EVALUATION_POINTS_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(RULE_EVALUATION_POINTS_MIGRATION_DISABLED_KEY);
    this.ruleCategoryMigrationDisabled =
        this.config.hasPath(RULE_CATEGORY_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(RULE_CATEGORY_MIGRATION_DISABLED_KEY);
    this.allowRulesPlatformExclusionMigrationDisabled =
        this.config.hasPath(ALLOW_RULES_PLATFORM_EXCLUSION_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(ALLOW_RULES_PLATFORM_EXCLUSION_MIGRATION_DISABLED_KEY);
    this.markForTestingInlineAgentMigrationDisabled =
        this.config.hasPath(MARK_FOR_TESTING_INLINE_AGENT_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(MARK_FOR_TESTING_INLINE_AGENT_MIGRATION_DISABLED_KEY);
    this.userVisibleEmailConfig = new UserVisibleEmailConfig(config);
  }

  private List<CustomSignatureRule> loadDefaultCustomSignatureRules() {
    List<CustomSignatureRule> defaultCustomSignatureRules = new ArrayList<>();
    if (config.hasPath(DEFAULT_CUSTOM_SIGNATURE_RULES_CONFIG_PATH)) {
      defaultCustomSignatureRules.addAll(
          convertToCustomSignatureRules(
              config.getConfigList(DEFAULT_CUSTOM_SIGNATURE_RULES_CONFIG_PATH)));
    }
    defaultCustomSignatureRules.addAll(
        convertToCustomSignatureRules(
            ConfigFactory.parseResources(DEFAULT_CUSTOM_SIGNATURE_RULES_FILE_PATH)
                .getConfigList(DEFAULT_CUSTOM_SIGNATURE_RULES_CONFIG_PATH)));
    defaultCustomSignatureRules.addAll(
        convertToCustomSignatureRules(
            ConfigFactory.parseResources(DEFAULT_AI_APP_PROTECTION_RULES_FILE_PATH)
                .getConfigList(DEFAULT_AI_APP_PROTECTION_RULES_CONFIG_PATH)));
    return defaultCustomSignatureRules;
  }

  public ModsecRuleVersion getModsecRuleVersion() {
    try {
      return this.config.getEnum(ModsecRuleVersion.class, MODSEC_RULE_VERSION_CONFIG);
    } catch (ConfigException e) {
      return ModsecRuleVersion.MODSEC_RULE_VERSION_V3;
    }
  }

  private List<CustomSignatureRule> convertToCustomSignatureRules(
      List<? extends Config> configList) {
    return configList.stream()
        .map(
            config -> {
              CustomSignatureRule.Builder builder = CustomSignatureRule.newBuilder();
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
