package ai.traceable.threatmanagement.config.service.threatscore;

import ai.traceable.threatmanagement.config.service.v1.ThreatScoreDecay;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.Optional;

class ThreatScoreDecayConverter {
  public Optional<ThreatScoreDecay> convert(Value ruleConfig) throws IOException {
    if (ruleConfig == null) {
      return Optional.empty();
    }
    ThreatScoreDecay.Builder builder = ThreatScoreDecay.newBuilder();
    JsonFormat.parser().merge(JsonFormat.printer().print(ruleConfig), builder);
    return Optional.of(builder.build());
  }

  public Value convert(ThreatScoreDecay threatScoreDecay) throws IOException {
    String json = JsonFormat.printer().print(threatScoreDecay);
    Value.Builder valueBuilder = Value.newBuilder();
    JsonFormat.parser().merge(json, valueBuilder);
    return valueBuilder.build();
  }
}
