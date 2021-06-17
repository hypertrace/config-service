package ai.traceable.anomaly.config.service.exclusion.converters;

import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import lombok.SneakyThrows;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class AnomalyExclusionRuleConfigConverter
    implements ConfigConverter<AnomalyExclusionRuleConfig> {

  @SneakyThrows
  public Value convert(AnomalyExclusionRuleConfig config) {
    return ConfigProtoConverter.convertToValue(config);
  }

  @SneakyThrows
  public AnomalyExclusionRuleConfig convert(Value configValue) {
    AnomalyExclusionRuleConfig.Builder configBuilder = AnomalyExclusionRuleConfig.newBuilder();
    if (configValue != null && configValue.getKindCase() != KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(configValue, configBuilder);
    }
    return configBuilder.build();
  }
}
