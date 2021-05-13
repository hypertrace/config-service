package ai.traceable.blocking.config.service;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class UuidGenerator {
  // A randomly generated UUID required for the UUID(type 5) generation
  private static final UUID NAMESPACE_UUID =
      UUID.fromString("5088c92d-5e9c-43f4-a35b-2589474d5642");

  private final String emptyValueUuid = generateId(new byte[0]);

  public String generateId(String value) {
    return UuidCreator.getNameBasedSha1(NAMESPACE_UUID, value).toString();
  }

  public String generateId(Optional<String> value) {
    return value.isEmpty()
        ? emptyValueUuid
        : UuidCreator.getNameBasedSha1(NAMESPACE_UUID, value.get()).toString();
  }

  public String generateId(List<? extends GeneratedMessageV3> protoMessages) {
    return protoMessages.stream()
        .map(message -> message.toByteString())
        .reduce((str1, str2) -> str1.concat(str2))
        .map(ByteString::toByteArray)
        .map(byteArray -> generateId(byteArray))
        .orElse(emptyValueUuid);
  }

  private String generateId(byte[] value) {
    return UuidCreator.getNameBasedSha1(NAMESPACE_UUID, value).toString();
  }
}
