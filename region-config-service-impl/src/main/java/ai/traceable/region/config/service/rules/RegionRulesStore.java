package ai.traceable.region.config.service.rules;

import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_NAMESPACE;
import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.region.config.service.v1.RegionRule;
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
public class RegionRulesStore extends IdentifiedObjectStore<RegionRule> {

  @Inject
  public RegionRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        REGION_RULE_CONFIG_NAMESPACE,
        REGION_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<RegionRule> buildDataFromValue(Value value) {
    try {
      RegionRule.Builder builder = RegionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error("Parsing the value {} into RegionRule failed with an exception", value, exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(RegionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(RegionRule rule) {
    return rule.getId();
  }
}
