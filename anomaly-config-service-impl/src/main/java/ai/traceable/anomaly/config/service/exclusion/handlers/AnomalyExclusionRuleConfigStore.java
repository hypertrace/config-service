package ai.traceable.anomaly.config.service.exclusion.handlers;

import static ai.traceable.anomaly.config.service.exclusion.AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.exclusion.AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class AnomalyExclusionRuleConfigStore
    extends IdentifiedObjectStore<AnomalyExclusionRuleConfig> {
  @Inject
  public AnomalyExclusionRuleConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        ANOMALY_EXCLUSION_CONFIG_NAMESPACE,
        ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<AnomalyExclusionRuleConfig> buildDataFromValue(Value value) {
    try {
      AnomalyExclusionRuleConfig.Builder anomalyExclusionRuleConfigBuilder =
          AnomalyExclusionRuleConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, anomalyExclusionRuleConfigBuilder);
      return Optional.of(anomalyExclusionRuleConfigBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Anomaly Exclusion Rule config from value -> {}", e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(AnomalyExclusionRuleConfig anomalyExclusionRuleConfig) {
    return ConfigProtoConverter.convertToValue(anomalyExclusionRuleConfig);
  }

  @Override
  protected String getContextFromData(AnomalyExclusionRuleConfig anomalyExclusionRuleConfig) {
    return anomalyExclusionRuleConfig.getId();
  }
}
