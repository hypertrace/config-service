package ai.traceable.localprocessing.config.service.coordinator;

import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAME;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE;

import ai.traceable.localprocessing.config.service.v1.LocalProcessingRule;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class LocalProcessingRulesConfigStore
    extends IdentifiedObjectStore<LocalProcessingRuleConfig> {
  @Inject
  public LocalProcessingRulesConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE,
        LOCAL_PROCESSING_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Value buildValueFromData(LocalProcessingRuleConfig localProcessingRuleConfig) {
    return localProcessingRuleConfig.toValue();
  }

  @Override
  protected Optional<LocalProcessingRuleConfig> buildDataFromValue(Value value) {
    return Optional.of(LocalProcessingRuleConfig.fromValue(value));
  }

  @Override
  @SneakyThrows
  protected Value buildValueForChangeEvent(LocalProcessingRuleConfig localProcessingRuleConfig) {
    return ConfigProtoConverter.convertToValue(localProcessingRuleConfig.getLocalProcessingRule());
  }

  @Override
  protected String buildClassNameForChangeEvent(
      LocalProcessingRuleConfig localProcessingRuleConfig) {
    return LocalProcessingRule.class.getName();
  }

  @Override
  protected String getContextFromData(LocalProcessingRuleConfig localProcessingRuleConfig) {
    return localProcessingRuleConfig.getLocalProcessingRule().getId();
  }
}
