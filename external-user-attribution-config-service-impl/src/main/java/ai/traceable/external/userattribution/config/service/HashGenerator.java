package ai.traceable.external.userattribution.config.service;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.protobuf.Message;
import java.util.UUID;

class HashGenerator {
  // A randomly generated UUID required for the UUID(type 5) generation
  private static final UUID NAMESPACE_UUID =
      UUID.fromString("58614ad4-4290-44d6-939f-a3f43be4b4df");

  public String generateHash(Message message) {
    return UuidCreator.getNameBasedSha1(NAMESPACE_UUID, message.toByteArray()).toString();
  }
}
