package ai.traceable.iprange.config.service.rules;

import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_CONFIG_NAMESPACE;
import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.iprange.config.service.v1.IpRangeRule;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class IpRangeRulesStore extends IdentifiedObjectStore<IpRangeRule> {

  @Inject
  public IpRangeRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        IPRANGE_RULE_CONFIG_NAMESPACE,
        IPRANGE_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<IpRangeRule> buildDataFromValue(Value value) {
    try {
      IpRangeRule.Builder builder = IpRangeRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error("Parsing the value {} into IpRangeRule failed with an exception", value, exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(IpRangeRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(IpRangeRule rule) {
    return rule.getId();
  }
}
