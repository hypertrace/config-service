package ai.traceable.region.config.service.utils;

import com.github.f4b6a3.uuid.UuidCreator;
import java.util.UUID;

public class UuidGenerator {
  // A randomly generated UUID required for the UUID(type 5) generation
  private static final UUID NAMESPACE_UUID =
      UUID.fromString("5088c92d-5e9c-43f4-a35b-2589474d5642");

  public String generateId(String value) {
    return UuidCreator.getNameBasedSha1(NAMESPACE_UUID, value).toString();
  }
}
