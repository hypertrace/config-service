package ai.traceable.anomaly.config.service.exclusion.utils;

import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import com.github.f4b6a3.uuid.UuidCreator;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.UUID;

public class UuidGenerator {
  // A randomly generated UUID required for the UUID(type 5) generation
  private static final UUID NAMESPACE_UUID =
      UUID.fromString("5088c92d-5e9c-43f4-a35b-2589474d5642");

  private final String emptyValueUuid = generateId(new byte[0]);

  public String generateId(AnomalyExclusionRuleData exclusionRuleData) {
    return List.of(
            exclusionRuleData.getAnomalyConfigScope(),
            exclusionRuleData.getEventExclusionInfo(),
            exclusionRuleData.getAnomalyActorExclusionInfo())
        .stream()
        .map(AbstractMessageLite::toByteString)
        .reduce(ByteString::concat)
        .map(ByteString::toByteArray)
        .map(this::generateId)
        .orElse(emptyValueUuid);
  }

  private String generateId(byte[] value) {
    return UuidCreator.getNameBasedSha1(NAMESPACE_UUID, value).toString();
  }
}
