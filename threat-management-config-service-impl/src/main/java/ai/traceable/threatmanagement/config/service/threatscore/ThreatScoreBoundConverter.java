package ai.traceable.threatmanagement.config.service.threatscore;

import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import java.util.Optional;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class ThreatScoreBoundConverter {

  public Optional<ThreatScoreBound> convert(Value ruleConfig)
      throws InvalidProtocolBufferException {
    if (ruleConfig == null
        || ruleConfig.getKindCase() == Value.KindCase.NULL_VALUE
        || ruleConfig.getKindCase() == KindCase.KIND_NOT_SET) {
      return Optional.empty();
    }

    ThreatScoreBound.Builder builder = ThreatScoreBound.newBuilder();
    ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    return Optional.of(builder.build());
  }

  public Value convert(ThreatScoreBound threatScoreBound) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(threatScoreBound);
  }
}
