package ai.traceable.config.utils;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.protobuf.GeneratedMessageV3;
import java.util.UUID;

public class UuidGenerator {
  // A randomly generated UUID required for the UUID(type 5) generation
  private static final UUID NAMESPACE_UUID =
      UUID.fromString("5088c92d-5e9c-43f4-a35b-2589474d5642");

  public String generateId(GeneratedMessageV3 protoMessage) {
    return generateId(protoMessage.toByteString().toByteArray());
  }

  public String generateRandomId() {
    return UuidCreator.getRandomBased().toString();
  }

  private String generateId(byte[] value) {
    return UuidCreator.getNameBasedSha1(NAMESPACE_UUID, value).toString();
  }
}
