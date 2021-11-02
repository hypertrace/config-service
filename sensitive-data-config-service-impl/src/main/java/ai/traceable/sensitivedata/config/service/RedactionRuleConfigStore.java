package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.REDACTION_RULES_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class RedactionRuleConfigStore extends IdentifiedObjectStore<RedactionRuleConfig> {

  @Inject
  RedactionRuleConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SENSITIVE_DATA_CONFIGURATION,
        REDACTION_RULES_CONFIG,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<RedactionRuleConfig> buildDataFromValue(Value value) {
    return Optional.of(RedactionRuleConfig.fromValue(value));
  }

  @Override
  protected Value buildValueFromData(RedactionRuleConfig redactionRuleConfig) {
    return redactionRuleConfig.toValue();
  }

  @Override
  protected String getContextFromData(RedactionRuleConfig redactionRuleConfig) {
    return redactionRuleConfig.getRedactionRule().getId();
  }
}
