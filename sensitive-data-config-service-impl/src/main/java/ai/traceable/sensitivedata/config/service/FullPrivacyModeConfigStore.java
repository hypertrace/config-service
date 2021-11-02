package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.FULL_PRIVACY_MODE_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class FullPrivacyModeConfigStore extends DefaultObjectStore<FullPrivacyModeConfig> {

  @Inject
  FullPrivacyModeConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SENSITIVE_DATA_CONFIGURATION,
        FULL_PRIVACY_MODE_CONFIG,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<FullPrivacyModeConfig> buildDataFromValue(Value value) {
    return FullPrivacyModeConfig.fromValue(value);
  }

  @Override
  protected Value buildValueFromData(FullPrivacyModeConfig fullPrivacyModeConfig) {
    return fullPrivacyModeConfig.toValue();
  }
}
