package ai.traceable.saved.filter.caching.client.config;

import java.time.Duration;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class SavedFilterClientConfig {
  @NonNull Duration deadline;
  String host;
  int port;
}
