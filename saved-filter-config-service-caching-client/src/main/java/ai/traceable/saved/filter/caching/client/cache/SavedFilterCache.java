package ai.traceable.saved.filter.caching.client.cache;

import ai.traceable.saved.filter.caching.client.SavedFilterCachingClient.SavedFilterKey;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import java.util.Collection;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface SavedFilterCache {
  Map<SavedFilterKey, SavedFilter> get(
      final RequestContext context, final Collection<SavedFilterKey> keys);
}
