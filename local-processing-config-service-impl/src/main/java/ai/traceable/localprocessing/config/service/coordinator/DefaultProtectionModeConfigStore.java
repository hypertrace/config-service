package ai.traceable.localprocessing.config.service.coordinator;

import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.DEFAULT_PROTECTION_MODE_CONFIG;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE;

import ai.traceable.localprocessing.config.service.v1.DefaultProtectionModeConfig;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class DefaultProtectionModeConfigStore extends DefaultObjectStore<ProtectionMode> {

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
  protected Optional<ProtectionMode> buildDataFromValue(Value value) {
    return DefaultProtectionModeConfigConverter.fromValue(value);
  }

  @Override
  @SneakyThrows
  protected Value buildValueForChangeEvent(ProtectionMode protectionMode) {
    return ConfigProtoConverter.convertToValue(
        DefaultProtectionModeConfig.newBuilder().setDefaultProtectionMode(protectionMode).build());
  }

  @Override
  protected String buildClassNameForChangeEvent(ProtectionMode protectionMode) {
    return DefaultProtectionModeConfig.class.getName();
  }

  @Override
  protected Value buildValueFromData(ProtectionMode protectionMode) {
    return DefaultProtectionModeConfigConverter.toValue(protectionMode);
  }
}
