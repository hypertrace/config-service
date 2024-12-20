package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.DEFAULT_RULE_POPULATION_STATUS;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class DefaultRedactionRulePersistenceStatusStore
    extends DefaultObjectStore<DefaultRedactionRulePersistenceStatus> {

  @Inject
  DefaultRedactionRulePersistenceStatusStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SENSITIVE_DATA_CONFIGURATION,
        DEFAULT_RULE_POPULATION_STATUS,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DefaultRedactionRulePersistenceStatus> buildDataFromValue(Value value) {
    return Optional.of(DefaultRedactionRulePersistenceStatus.fromValue(value));
  }

  @Override
  protected Value buildValueFromData(
      DefaultRedactionRulePersistenceStatus defaultRedactionRulePopulationStatus) {
    return defaultRedactionRulePopulationStatus.toValue();
  }
}
