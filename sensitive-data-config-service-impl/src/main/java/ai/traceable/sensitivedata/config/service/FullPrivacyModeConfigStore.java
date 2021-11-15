package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.FULL_PRIVACY_MODE_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import ai.traceable.sensitivedata.config.service.v1.FullPrivacyModeConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
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
    try {
      FullPrivacyModeConfig.Builder fullPrivacyModeBuilder = FullPrivacyModeConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, fullPrivacyModeBuilder);
      return Optional.of(fullPrivacyModeBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Full privacy mode config from value -> {}", e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(FullPrivacyModeConfig fullPrivacyModeConfig) {
    return ConfigProtoConverter.convertToValue(fullPrivacyModeConfig);
  }
}
