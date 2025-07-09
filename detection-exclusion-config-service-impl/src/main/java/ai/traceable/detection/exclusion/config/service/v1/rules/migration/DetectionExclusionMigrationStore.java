package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import static ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesUtils.DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionMigrationConfig;
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
public class DetectionExclusionMigrationStore
    extends DefaultObjectStore<DetectionExclusionMigrationConfig> {

  public static final String DETECTION_EXCLUSION_MIGRATION_CONFIG_RESOURCE_NAME =
      "detectionExclusionMigrationConfig";

  @Inject
  public DetectionExclusionMigrationStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE,
        DETECTION_EXCLUSION_MIGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DetectionExclusionMigrationConfig> buildDataFromValue(Value value) {
    try {
      DetectionExclusionMigrationConfig.Builder detectionExclusionMigrationConfigBuilder =
          DetectionExclusionMigrationConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, detectionExclusionMigrationConfigBuilder);
      return Optional.of(detectionExclusionMigrationConfigBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Detection Exclusion Migration Config from value -> ", e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(
      DetectionExclusionMigrationConfig detectionExclusionMigrationConfig) {
    return ConfigProtoConverter.convertToValue(detectionExclusionMigrationConfig);
  }
}
