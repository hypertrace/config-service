package ai.traceable.saved.filter.caching.client.config;

import java.time.Duration;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class SavedFilterCacheConfig {
  Duration expiryDurationAfterWrite;
  long maxCacheSize;
}
