import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.saved.filter.caching.client.SavedFilterCachingClient;
import ai.traceable.saved.filter.caching.client.SavedFilterCachingClient.SavedFilterKey;
import ai.traceable.saved.filter.caching.client.SavedFilterCachingClientImpl;
import ai.traceable.saved.filter.caching.client.SavedFilterServiceClient;
import ai.traceable.saved.filter.caching.client.cache.SavedFilterLoadingCache;
import ai.traceable.saved.filter.caching.client.config.SavedFilterCacheConfig;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SavedFilterCachingClientImplTest {
  private final String SAVED_FILTER_ID = "id";
  private final String TENANT_ID = "tenantId";

  @Mock KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> mockKafkaEventListener;

  @Mock SavedFilterServiceClient savedFilterServiceClient;

  private SavedFilterCachingClient cachingClient;
  private RequestContext requestContext;
  private SavedFilterLoadingCache loadingCache;

  @BeforeEach
  void setUp() {

    requestContext = RequestContext.forTenantId(TENANT_ID);
    loadingCache =
        new SavedFilterLoadingCache(
            mockKafkaEventListener,
            SavedFilterCacheConfig.builder()
                .expiryDurationAfterWrite(Duration.ofMinutes(30))
                .maxCacheSize(1000)
                .build(),
            savedFilterServiceClient);
    cachingClient = new SavedFilterCachingClientImpl(loadingCache);
  }

  @Test
  void testFetchingSavedFilter() {

    SavedFilter savedFilter = SavedFilter.newBuilder().setId(SAVED_FILTER_ID).build();

    final SavedFilterKey savedFilterKey = SavedFilterKey.builder().id(SAVED_FILTER_ID).build();

    SavedFilterCachingClient.SavedFilterContext savedFilterContext =
        SavedFilterCachingClient.SavedFilterContext.builder()
            .requests(List.of(savedFilterKey))
            .requestContext(requestContext)
            .build();

    Map<SavedFilterKey, SavedFilter> expectedMap = Map.of(savedFilterKey, savedFilter);

    when(savedFilterServiceClient.getSavedFilter(requestContext, savedFilterKey))
        .thenReturn(List.of(savedFilter));

    Map<SavedFilterKey, SavedFilter> actualMap = cachingClient.get(savedFilterContext);
    assertEquals(expectedMap, actualMap);

    actualMap = cachingClient.get(savedFilterContext);
    assertEquals(expectedMap, actualMap);

    verify(savedFilterServiceClient, times(1)).getSavedFilter(requestContext, savedFilterKey);
  }
}
