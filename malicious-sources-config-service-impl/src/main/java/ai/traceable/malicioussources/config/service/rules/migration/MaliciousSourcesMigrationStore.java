package ai.traceable.malicioussources.config.service.rules.migration;

import static ai.traceable.malicioussources.config.service.constants.MaliciousSourcesConfigConstants.MALICIOUS_SOURCES_RULE_CONFIG_NAMESPACE;
import static ai.traceable.malicioussources.config.service.constants.MaliciousSourcesConfigConstants.MALICIOUS_SOURCES_RULE_MIGRATION_CONFIG_RESOURCE_NAME;

import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesMigrationConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class MaliciousSourcesMigrationStore
    extends DefaultObjectStore<MaliciousSourcesMigrationConfig> {

  @Inject
  public MaliciousSourcesMigrationStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        MALICIOUS_SOURCES_RULE_CONFIG_NAMESPACE,
        MALICIOUS_SOURCES_RULE_MIGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<MaliciousSourcesMigrationConfig> buildDataFromValue(Value value) {
    try {
      MaliciousSourcesMigrationConfig.Builder builder =
          MaliciousSourcesMigrationConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize MaliciousSourcesMigrationConfig from value : {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(
      MaliciousSourcesMigrationConfig maliciousSourcesMigrationConfig) {
    return ConfigProtoConverter.convertToValue(maliciousSourcesMigrationConfig);
  }
}
