package ai.traceable.genai.system.discovery.config.service.v1.config;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.validation.GenAiSystemDiscoveryRulesValidator;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.grpc.Status;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
public class GenAiSystemDiscoveryConfig {

  private static final String DEFAULT_GENAI_SYSTEM_DISCOVERY_RULES_FILE =
      "default-genai-system-discovery-rules.conf";
  private static final String DEFAULT_GENAI_SYSTEM_DISCOVERY_SERVER_SPAN_RULES_FILE =
      "default-genai-system-discovery-server-span-rules.conf";
  private static final String GENAI_SYSTEM_DISCOVERY_RULES_PATH = "genAiSystemDiscoveryRules";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  private final GenAiSystemDiscoveryRulesValidator rulesValidator;
  private final FeatureCachingClient featureCachingClient;
  private final Map<String, GenAiSystemDiscoveryRule> defaultGenAiSystemDiscoveryRuleMap;

  private volatile Map<String, GenAiSystemDiscoveryRule> mergedDefaultGenAiSystemDiscoveryRuleMap;

  @Inject
  public GenAiSystemDiscoveryConfig(
      GenAiSystemDiscoveryRulesValidator rulesValidator,
      FeatureCachingClient featureCachingClient) {
    this.rulesValidator = rulesValidator;
    this.featureCachingClient = featureCachingClient;
    this.defaultGenAiSystemDiscoveryRuleMap =
        buildRuleMap(DEFAULT_GENAI_SYSTEM_DISCOVERY_RULES_FILE);
  }

  public Map<String, GenAiSystemDiscoveryRule> getDefaultGenAiSystemDiscoveryRuleMap(
      RequestContext requestContext) {
    if (!featureCachingClient.isGenAiServerSpanAiClassificationEnabled(requestContext)) {
      return defaultGenAiSystemDiscoveryRuleMap;
    }
    return getMergedDefaultGenAiSystemDiscoveryRuleMap();
  }

  private Map<String, GenAiSystemDiscoveryRule> getMergedDefaultGenAiSystemDiscoveryRuleMap() {
    if (mergedDefaultGenAiSystemDiscoveryRuleMap == null) {
      synchronized (this) {
        if (mergedDefaultGenAiSystemDiscoveryRuleMap == null) {
          Map<String, GenAiSystemDiscoveryRule> merged = new LinkedHashMap<>();
          merged.putAll(defaultGenAiSystemDiscoveryRuleMap);
          merged.putAll(buildRuleMap(DEFAULT_GENAI_SYSTEM_DISCOVERY_SERVER_SPAN_RULES_FILE));
          mergedDefaultGenAiSystemDiscoveryRuleMap = Collections.unmodifiableMap(merged);
        }
      }
    }
    return mergedDefaultGenAiSystemDiscoveryRuleMap;
  }

  private Map<String, GenAiSystemDiscoveryRule> buildRuleMap(String filePath) {
    Map<String, GenAiSystemDiscoveryRule> map = new LinkedHashMap<>();
    ConfigFactory.parseResources(filePath).getConfigList(GENAI_SYSTEM_DISCOVERY_RULES_PATH).stream()
        .map(this::convert)
        .forEach(rule -> map.putIfAbsent(rule.getRuleId(), rule));
    return Collections.unmodifiableMap(map);
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
