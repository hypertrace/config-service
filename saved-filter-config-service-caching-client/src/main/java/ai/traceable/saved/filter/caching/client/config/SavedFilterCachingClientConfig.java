package ai.traceable.saved.filter.caching.client.config;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class SavedFilterCachingClientConfig {
  SavedFilterCacheConfig savedFilterCacheConfig;
  SavedFilterClientConfig savedFilterClientConfig;
}
