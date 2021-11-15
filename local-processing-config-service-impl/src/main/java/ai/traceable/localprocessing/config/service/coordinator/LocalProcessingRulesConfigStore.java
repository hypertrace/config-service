package ai.traceable.localprocessing.config.service.coordinator;

import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAME;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE;

import ai.traceable.localprocessing.config.service.v1.LocalProcessingRule;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class LocalProcessingRulesConfigStore extends IdentifiedObjectStore<LocalProcessingRule> {
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
  @SneakyThrows
  protected Value buildValueFromData(LocalProcessingRule localProcessingRule) {
    return ConfigProtoConverter.convertToValue(localProcessingRule);
  }

  @Override
  protected Optional<LocalProcessingRule> buildDataFromValue(Value value) {
    try {
      LocalProcessingRule.Builder localProcessingRuleBuilder = LocalProcessingRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, localProcessingRuleBuilder);
      return Optional.of(localProcessingRuleBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Local processing rule from value ->{}", e);
      return Optional.empty();
    }
  }

  @Override
  protected String getContextFromData(LocalProcessingRule localProcessingRule) {
    return localProcessingRule.getId();
  }
}
