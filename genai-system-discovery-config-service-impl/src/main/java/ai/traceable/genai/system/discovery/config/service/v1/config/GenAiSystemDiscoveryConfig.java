package ai.traceable.genai.system.discovery.config.service.v1.config;

import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.validation.GenAiSystemDiscoveryRulesValidator;
import com.google.inject.Inject;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.grpc.Status;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenAiSystemDiscoveryConfig {

  private static final String DEFAULT_GENAI_SYSTEM_DISCOVERY_RULES_FILE_PATH =
      "default-genai-system-discovery-rules.conf";
  private static final String GENAI_SYSTEM_DISCOVERY_RULES_PATH = "genAiSystemDiscoveryRules";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  private final GenAiSystemDiscoveryRulesValidator rulesValidator;
  @Getter private final Map<String, GenAiSystemDiscoveryRule> defaultGenAiSystemDiscoveryRuleMap;

  @Inject
  public GenAiSystemDiscoveryConfig(GenAiSystemDiscoveryRulesValidator rulesValidator) {
    this.rulesValidator = rulesValidator;
    this.defaultGenAiSystemDiscoveryRuleMap = buildDefaultGenAiSystemDiscoveryRuleMap();
  }

  private Map<String, GenAiSystemDiscoveryRule> buildDefaultGenAiSystemDiscoveryRuleMap() {
    return ConfigFactory.parseResources(DEFAULT_GENAI_SYSTEM_DISCOVERY_RULES_FILE_PATH)
        .getConfigList(GENAI_SYSTEM_DISCOVERY_RULES_PATH)
        .stream()
        .map(this::convert)
        .collect(Collectors.toUnmodifiableMap(GenAiSystemDiscoveryRule::getRuleId, entry -> entry));
  }

  private GenAiSystemDiscoveryRule convert(Config config) {
    GenAiSystemDiscoveryRule.Builder builder = GenAiSystemDiscoveryRule.newBuilder();
    mergeFromConfig(config, builder);
    GenAiSystemDiscoveryRule rule = builder.build();
    try {
      rulesValidator.validateGenAiSystemDiscoveryRule(rule);
      return rule;
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid genai system discovery rule : %s", rule))
          .asRuntimeException();
    }
  }

  private void mergeFromConfig(Config config, Message.Builder builder) {
    try {
      JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Failed to parse configuration for config: %s", config))
          .asRuntimeException();
    }
  }
}
