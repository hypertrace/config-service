package ai.traceable.customsignature.config.service;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
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
  private static final String MODSEC_RULE_VERSION_CONFIG = "modsecurity.rule.version";
  private static final String EDS_CONVERSION_ENABLED_CONFIG_PATH = "edsConversionEnabled";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String MIGRATION_DISABLED_KEY = "migrationDisabled";
  private static final String RULE_EVALUATION_POINTS_MIGRATION_DISABLED_KEY =
      "ruleEvaluationPoints." + MIGRATION_DISABLED_KEY;

  @Getter private final boolean ruleEvaluationPointsMigrationDisabled;

  public CustomSignatureConfigServiceConfig(Config config) {
    this.config =
        config.hasPath(CUSTOM_SIGNATURE_CONFIG_SERVICE)
            ? config.getConfig(CUSTOM_SIGNATURE_CONFIG_SERVICE)
            : ConfigFactory.empty();
    this.defaultCustomSignatureRules = loadDefaultCustomSignatureRules();
    this.ruleEvaluationPointsMigrationDisabled =
        this.config.hasPath(RULE_EVALUATION_POINTS_MIGRATION_DISABLED_KEY)
            && this.config.getBoolean(RULE_EVALUATION_POINTS_MIGRATION_DISABLED_KEY);
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
    return defaultCustomSignatureRules;
  }

  public ModsecRuleVersion getModsecRuleVersion() {
    try {
      return this.config.getEnum(ModsecRuleVersion.class, MODSEC_RULE_VERSION_CONFIG);
    } catch (ConfigException e) {
      return ModsecRuleVersion.MODSEC_RULE_VERSION_V3;
    }
  }

  public boolean isEdsConversionEnabled() {
    return this.config.getBoolean(EDS_CONVERSION_ENABLED_CONFIG_PATH);
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
