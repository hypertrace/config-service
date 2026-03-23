package ai.traceable.localprocessing.config.service.coordinator;

import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.DETECTION_RULES_CONFIG;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE;

import ai.traceable.localprocessing.config.service.v1.DetectionRulesConfig;
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
public class DetectionRulesConfigStore extends DefaultObjectStore<DetectionRulesConfig> {

  @Inject
  public DetectionRulesConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE,
        DETECTION_RULES_CONFIG,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DetectionRulesConfig> buildDataFromValue(Value value) {
    try {
      DetectionRulesConfig.Builder detectionRulesConfigBuilder = DetectionRulesConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, detectionRulesConfigBuilder);
      return Optional.of(detectionRulesConfigBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Detection Rules Config from value -> {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(DetectionRulesConfig detectionRulesConfig) {
    return ConfigProtoConverter.convertToValue(detectionRulesConfig);
  }
}
