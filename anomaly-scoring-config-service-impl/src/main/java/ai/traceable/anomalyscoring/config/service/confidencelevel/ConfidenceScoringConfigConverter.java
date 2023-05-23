package ai.traceable.anomalyscoring.config.service.confidencelevel;

import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoringConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import java.util.Optional;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class ConfidenceScoringConfigConverter {

  public Optional<ConfidenceScoringConfig> convert(Value ruleConfig)
      throws InvalidProtocolBufferException {
    if (ruleConfig == null
        || ruleConfig.getKindCase() == KindCase.NULL_VALUE
        || ruleConfig.getKindCase() == KindCase.KIND_NOT_SET) {
      return Optional.empty();
    }

    ConfidenceScoringConfig.Builder builder = ConfidenceScoringConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    return Optional.of(builder.build());
  }

  public Value convert(ConfidenceScoringConfig confidenceScoringConfig)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(confidenceScoringConfig);
  }
}
