package ai.traceable.saved.filter.caching.client;

import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import com.google.inject.ImplementedBy;
import java.util.Collection;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.grpcutils.context.RequestContext;

@ImplementedBy(SavedFilterCachingClientImpl.class)
public interface SavedFilterCachingClient {

  Map<SavedFilterKey, SavedFilter> get(SavedFilterContext savedFilterContext);

  @Value
  @Builder(toBuilder = true)
  @Accessors(fluent = true, chain = true)
  class SavedFilterContext {
    @NonNull RequestContext requestContext;
    @NonNull Collection<SavedFilterKey> requests;
  }

  @Value
  @Builder
  @Accessors(fluent = true, chain = true)
  class SavedFilterKey {
    String id;
  }
}
