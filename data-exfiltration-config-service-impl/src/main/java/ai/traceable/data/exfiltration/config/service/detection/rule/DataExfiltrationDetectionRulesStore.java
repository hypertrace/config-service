package ai.traceable.data.exfiltration.config.service.detection.rule;

import ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

@Slf4j
public class DataExfiltrationDetectionRulesStore
    extends IdentifiedObjectStore<DataExfiltrationDetectionRuleConfig> {

  private static final String DATA_EXFILTRATION_DETECTION_RULE_CONFIG =
      "dataExfiltrationDetectionRuleConfig";
  private static final String DATA_EXFILTRATION_DETECTION_RULE_NAMESPACE =
      "data-exfiltration-config-v1";

  @Inject
  public DataExfiltrationDetectionRulesStore(
      Channel configChannel, ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get()),
        DATA_EXFILTRATION_DETECTION_RULE_NAMESPACE,
        DATA_EXFILTRATION_DETECTION_RULE_CONFIG,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DataExfiltrationDetectionRuleConfig> buildDataFromValue(Value value) {
    DataExfiltrationDetectionRuleConfig.Builder builder =
        DataExfiltrationDetectionRuleConfig.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Conversion failed. value {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(DataExfiltrationDetectionRuleConfig data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(DataExfiltrationDetectionRuleConfig data) {
    return data.getId();
  }
}
