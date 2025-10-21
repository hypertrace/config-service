package ai.traceable.customsignature.config.service.migration;

import static ai.traceable.customsignature.config.service.CustomSignatureConstants.CUSTOM_SIGNATURE_MIGRATION_CONFIG_RESOURCE_NAME;
import static ai.traceable.customsignature.config.service.CustomSignatureConstants.CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE;

import ai.traceable.customsignature.config.service.v1.CustomSignatureMigrationConfig;
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
public class CustomSignatureMigrationConfigStore
    extends DefaultObjectStore<CustomSignatureMigrationConfig> {

  @Inject
  public CustomSignatureMigrationConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE,
        CUSTOM_SIGNATURE_MIGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<CustomSignatureMigrationConfig> buildDataFromValue(Value value) {
    try {
      CustomSignatureMigrationConfig.Builder builder = CustomSignatureMigrationConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize CustomSignatureMigrationConfig from value : {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(CustomSignatureMigrationConfig migrationConfig) {
    return ConfigProtoConverter.convertToValue(migrationConfig);
  }
}
