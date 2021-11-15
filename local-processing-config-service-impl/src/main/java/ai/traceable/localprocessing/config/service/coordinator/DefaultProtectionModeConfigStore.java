package ai.traceable.localprocessing.config.service.coordinator;

import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.DEFAULT_PROTECTION_MODE_CONFIG;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE;

import ai.traceable.localprocessing.config.service.v1.DefaultProtectionModeConfig;
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
public class DefaultProtectionModeConfigStore
    extends DefaultObjectStore<DefaultProtectionModeConfig> {

  @Inject
  public DefaultProtectionModeConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE,
        DEFAULT_PROTECTION_MODE_CONFIG,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DefaultProtectionModeConfig> buildDataFromValue(Value value) {
    try {
      DefaultProtectionModeConfig.Builder defaultProtectionModeConfigBuilder =
          DefaultProtectionModeConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, defaultProtectionModeConfigBuilder);
      return Optional.of(defaultProtectionModeConfigBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Default Protection Mode from value -> {}", e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(DefaultProtectionModeConfig defaultProtectionModeConfig) {
    return ConfigProtoConverter.convertToValue(defaultProtectionModeConfig);
  }
}
