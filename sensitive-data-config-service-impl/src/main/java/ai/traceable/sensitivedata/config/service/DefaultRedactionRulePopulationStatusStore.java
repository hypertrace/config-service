package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.DEFAULT_RULE_POPULATION_STATUS;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class DefaultRedactionRulePopulationStatusStore
    extends DefaultObjectStore<DefaultRedactionRulePopulationStatus> {

  @Inject
  DefaultRedactionRulePopulationStatusStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SENSITIVE_DATA_CONFIGURATION,
        DEFAULT_RULE_POPULATION_STATUS,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DefaultRedactionRulePopulationStatus> buildObjectFromValue(Value value) {
    return Optional.of(DefaultRedactionRulePopulationStatus.fromValue(value));
  }

  @Override
  protected Value buildValueFromObject(
      DefaultRedactionRulePopulationStatus defaultRedactionRulePopulationStatus) {
    return defaultRedactionRulePopulationStatus.toValue();
  }
}
