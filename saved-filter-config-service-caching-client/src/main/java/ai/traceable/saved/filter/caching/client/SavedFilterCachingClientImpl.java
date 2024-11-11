package ai.traceable.saved.filter.caching.client;

import ai.traceable.saved.filter.caching.client.cache.SavedFilterCache;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import io.grpc.Status;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class SavedFilterCachingClientImpl implements SavedFilterCachingClient {

  private final SavedFilterCache savedFilterCache;

  public SavedFilterCachingClientImpl(SavedFilterCache savedFilterCache) {
    this.savedFilterCache = savedFilterCache;
  }

  @Override
  public Map<SavedFilterKey, SavedFilter> get(SavedFilterContext savedFilterContext) {
    validateRequestContext(savedFilterContext.requestContext());
    return savedFilterCache.get(savedFilterContext.requestContext(), savedFilterContext.requests());
  }

  private void validateRequestContext(RequestContext requestContext) {
    requestContext
        .getTenantId()
        .orElseThrow(
            () ->
                Status.INVALID_ARGUMENT
                    .withDescription("No tenantId is present in the request")
                    .asRuntimeException(requestContext.buildTrailers()));
  }
}
