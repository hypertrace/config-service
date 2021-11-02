package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class AutomaticSecretRedactionStrategyConfigStore
    extends DefaultObjectStore<AutomaticSecretRedactionStrategyConfig> {

  @Inject
  AutomaticSecretRedactionStrategyConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SENSITIVE_DATA_CONFIGURATION,
        AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<AutomaticSecretRedactionStrategyConfig> buildDataFromValue(Value value) {
    return AutomaticSecretRedactionStrategyConfig.fromValue(value);
  }

  @Override
  protected Value buildValueFromData(
      AutomaticSecretRedactionStrategyConfig automaticSecretRedactionStrategyConfig) {
    return automaticSecretRedactionStrategyConfig.toValue();
  }
}
