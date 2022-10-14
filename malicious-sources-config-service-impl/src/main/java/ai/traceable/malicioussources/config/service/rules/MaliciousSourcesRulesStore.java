package ai.traceable.malicioussources.config.service.rules;

import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class MaliciousSourcesRulesStore
    extends IdentifiedObjectStoreWithFilter<MaliciousSourcesRule, GetRulesFilter> {
  public static final String MALICIOUS_SOURCES_RULE_CONFIG_NAMESPACE = "maliciousSourcesRule";
  public static final String MALICIOUS_SOURCES_RULE_CONFIG_RESOURCE_NAME =
      "maliciousSourcesRuleConfig";

  @Inject
  public MaliciousSourcesRulesStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        MALICIOUS_SOURCES_RULE_CONFIG_NAMESPACE,
        MALICIOUS_SOURCES_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<MaliciousSourcesRule> buildDataFromValue(Value value) {
    try {
      MaliciousSourcesRule.Builder builder = MaliciousSourcesRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into MaliciousSourcesRule failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(MaliciousSourcesRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(MaliciousSourcesRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<MaliciousSourcesRule> filterConfigData(
      MaliciousSourcesRule ruleData, GetRulesFilter filter) {
    return Optional.empty();
  }
}
