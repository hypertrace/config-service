package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.INVALID_JSON_POLICY_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import ai.traceable.sensitivedata.config.service.v1.InvalidJsonPolicy;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
class InvalidJsonPolicyConfigStore extends DefaultObjectStore<InvalidJsonPolicy> {

  @Inject
  InvalidJsonPolicyConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SENSITIVE_DATA_CONFIGURATION,
        INVALID_JSON_POLICY_CONFIG,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<InvalidJsonPolicy> buildDataFromValue(Value value) {
    try {
      InvalidJsonPolicy.Builder builder = InvalidJsonPolicy.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Invalid Json Policy config from value -> {}", e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(InvalidJsonPolicy invalidJsonPolicy) {
    return ConfigProtoConverter.convertToValue(invalidJsonPolicy);
  }
}
