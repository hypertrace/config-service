package org.hypertrace.config.objectstore;

import java.time.Duration;
import lombok.Value;

@Value
public class ClientConfig {
  Duration timeout;

  public static ClientConfig DEFAULT = new ClientConfig(Duration.ofSeconds(5));
}
