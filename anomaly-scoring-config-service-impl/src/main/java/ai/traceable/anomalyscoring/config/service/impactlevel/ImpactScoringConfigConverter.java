package ai.traceable.anomalyscoring.config.service.impactlevel;

import ai.traceable.anomalyscoring.config.service.v1.ImpactScoringConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import java.util.Optional;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class ImpactScoringConfigConverter {

  public Optional<ImpactScoringConfig> convert(Value ruleConfig)
      throws InvalidProtocolBufferException {
    if (ruleConfig == null
        || ruleConfig.getKindCase() == KindCase.NULL_VALUE
        || ruleConfig.getKindCase() == KindCase.KIND_NOT_SET) {
      return Optional.empty();
    }

    ImpactScoringConfig.Builder builder = ImpactScoringConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    return Optional.of(builder.build());
  }

  public Value convert(ImpactScoringConfig impactScoringConfig)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(impactScoringConfig);
  }
}
