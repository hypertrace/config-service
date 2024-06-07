package org.hypertrace.config.objectstore;

import java.time.Duration;
import lombok.Value;

@Value
public class ClientConfig {
  Duration timeout;
  // TODO: Explore unobtrusive ways of setting this default from config
  public static ClientConfig DEFAULT = new ClientConfig(Duration.ofSeconds(10));
}
