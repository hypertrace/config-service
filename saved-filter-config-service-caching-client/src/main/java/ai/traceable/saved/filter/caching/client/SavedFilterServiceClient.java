package ai.traceable.saved.filter.caching.client;

import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.SECONDS;

import ai.traceable.saved.filter.caching.client.SavedFilterCachingClient.SavedFilterKey;
import ai.traceable.saved.filter.caching.client.config.SavedFilterClientConfig;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersResponse;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import ai.traceable.saved.filter.config.service.v1.SavedFilterServiceGrpc.SavedFilterServiceBlockingStub;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class SavedFilterServiceClient {
  private final SavedFilterServiceBlockingStub savedFilterServiceBlockingStub;
  private final SavedFilterClientConfig savedFilterClientConfig;

  public SavedFilterServiceClient(
      SavedFilterServiceBlockingStub savedFilterServiceBlockingStub,
      SavedFilterClientConfig savedFilterClientConfig) {
    this.savedFilterServiceBlockingStub = savedFilterServiceBlockingStub;
    this.savedFilterClientConfig = savedFilterClientConfig;
  }

  public List<SavedFilter> getSavedFilter(final RequestContext requestContext, SavedFilterKey key) {
    GetSavedFiltersRequest request = GetSavedFiltersRequest.newBuilder().setId(key.id()).build();

    final GetSavedFiltersResponse response;
    try {
      response =
          requestContext.call(
              () ->
                  savedFilterServiceBlockingStub
                      .withDeadlineAfter(
                          savedFilterClientConfig.getDeadline().getSeconds(), SECONDS)
                      .getSavedFilters(request));

    } catch (Exception e) {
      log.warn(
          String.format(
              "Error fetching savedFilter with request %s and context %s", request, requestContext),
          e);
      throw e;
    }

    log.debug(
        "Fetch Query: Request = {} Response has items {} Context = {}",
        request,
        response.getSavedFiltersList(),
        requestContext);

    return unmodifiableList(response.getSavedFiltersList());
  }
}
