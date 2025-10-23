package ai.traceable.anomaly.config.service.detector.migration;

import static ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigConstants.ANOMALY_DETECTION_CONFIG_NAMESPACE;

import ai.traceable.anomaly.config.service.v1.detector.migration.AnomalyDetectionMigrationConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
@Singleton
public class AnomalyDetectionMigrationStore
    extends DefaultObjectStore<AnomalyDetectionMigrationConfig> {

  public static final String ANOMALY_DETECTION_MIGRATION_CONFIG_RESOURCE_NAME =
      "anomalyDetectionMigrationConfig";

  @Inject
  public AnomalyDetectionMigrationStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        ANOMALY_DETECTION_CONFIG_NAMESPACE,
        ANOMALY_DETECTION_MIGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<AnomalyDetectionMigrationConfig> buildDataFromValue(Value value) {
    try {
      AnomalyDetectionMigrationConfig.Builder migrationConfigBuilder =
          AnomalyDetectionMigrationConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, migrationConfigBuilder);
      return Optional.of(migrationConfigBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Anomaly Detection Migration Config from value -> ", e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(
      AnomalyDetectionMigrationConfig anomalyDetectionMigrationConfig) {
    return ConfigProtoConverter.convertToValue(anomalyDetectionMigrationConfig);
  }
}
