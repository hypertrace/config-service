package ai.traceable.iprange.config.service.utils;

import java.util.UUID;

public class UuidGenerator {
  public String generateId() {
    return UUID.randomUUID().toString();
  }
}
