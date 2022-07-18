package ai.traceable.config.utils;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class UuidGenerator {
  // A randomly generated UUID required for the UUID(type 5) generation
  private static final UUID NAMESPACE_UUID =
      UUID.fromString("5088c92d-5e9c-43f4-a35b-2589474d5642");
  private final String emptyValueUuid = generateId(new byte[0]);

  public String generateId(GeneratedMessageV3 protoMessage) {
    return generateId(protoMessage.toByteString().toByteArray());
  }

  public String generateRandomId() {
    return UuidCreator.getRandomBased().toString();
  }

  private String generateId(byte[] value) {
    return UuidCreator.getNameBasedSha1(NAMESPACE_UUID, value).toString();
  }

  public String generateId(List<? extends GeneratedMessageV3> protoMessages) {
    return protoMessages.stream()
        .map(GeneratedMessageV3::toByteString)
        .reduce(ByteString::concat)
        .map(ByteString::toByteArray)
        .map(this::generateId)
        .orElse(emptyValueUuid);
  }

  public String generateId(String value) {
    return this.generateId(value.getBytes(StandardCharsets.UTF_8));
  }
}
