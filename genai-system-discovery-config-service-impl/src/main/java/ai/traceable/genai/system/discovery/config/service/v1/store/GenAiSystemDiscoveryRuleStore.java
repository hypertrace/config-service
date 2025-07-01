package ai.traceable.genai.system.discovery.config.service.v1.store;

import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesFilter;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class GenAiSystemDiscoveryRuleStore
    extends IdentifiedObjectStoreWithFilter<
        GenAiSystemDiscoveryRule, GetGenAiSystemDiscoveryRulesFilter> {

  private static final String GEN_AI_SYSTEM_DISCOVERY_RULE_CONFIG_NAMESPACE =
      "gen-ai-system-discovery-rule";
  private static final String GEN_AI_SYSTEM_DISCOVERY_RULE_CONFIG_RESOURCE_NAME =
      "gen-ai-system-discovery-rule-config";

  @Inject
  public GenAiSystemDiscoveryRuleStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        GEN_AI_SYSTEM_DISCOVERY_RULE_CONFIG_NAMESPACE,
        GEN_AI_SYSTEM_DISCOVERY_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<GenAiSystemDiscoveryRule> filterConfigData(
      GenAiSystemDiscoveryRule data, GetGenAiSystemDiscoveryRulesFilter filter) {
    // ToDo : In future to support filter on getGenAiSystemDiscoveryRules
    return Optional.of(data);
  }

  @Override
  protected Optional<GenAiSystemDiscoveryRule> buildDataFromValue(Value value) {
    try {
      GenAiSystemDiscoveryRule.Builder builder = GenAiSystemDiscoveryRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into GenAiSystemDiscoveryRule failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(GenAiSystemDiscoveryRule data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(GenAiSystemDiscoveryRule data) {
    return data.getRuleId();
  }
}
