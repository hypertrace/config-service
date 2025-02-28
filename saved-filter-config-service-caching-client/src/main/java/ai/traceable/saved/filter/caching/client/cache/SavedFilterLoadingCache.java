package ai.traceable.saved.filter.caching.client.cache;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.SECONDS;

import ai.traceable.saved.filter.caching.client.SavedFilterCachingClient.SavedFilterKey;
import ai.traceable.saved.filter.caching.client.SavedFilterServiceClient;
import ai.traceable.saved.filter.caching.client.config.SavedFilterCacheConfig;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.change.event.v1.ConfigCreateEvent;
import org.hypertrace.config.change.event.v1.ConfigDeleteEvent;
import org.hypertrace.config.change.event.v1.ConfigUpdateEvent;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class SavedFilterLoadingCache implements SavedFilterCache {

  private static final String SAVED_FILTER = "SavedFilter";
  private static final String SAVED_FILTER_CACHE_NAME = "savedFilterCache";

  private final LoadingCache<ContextualKey<SavedFilterKey>, SavedFilter> savedFilterCache;
  private final SavedFilterServiceClient savedFilterServiceClient;

  public SavedFilterLoadingCache(
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener,
      SavedFilterCacheConfig savedFilterCacheConfig,
      SavedFilterServiceClient savedFilterServiceClient) {
    this(savedFilterCacheConfig, savedFilterServiceClient);
    kafkaLiveEventListener.registerCallback(this::updateBasedOnChangeEvent);
  }

  public SavedFilterLoadingCache(
      SavedFilterCacheConfig savedFilterCacheConfig,
      SavedFilterServiceClient savedFilterServiceClient) {
    this.savedFilterServiceClient = savedFilterServiceClient;
    this.savedFilterCache = buildSavedFilterCache(savedFilterCacheConfig);
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        SAVED_FILTER_CACHE_NAME,
        this.savedFilterCache,
        emptyMap(),
        savedFilterCacheConfig.getMaxCacheSize());
  }

  @Override
  public Map<SavedFilterKey, SavedFilter> get(
      final RequestContext context, final Collection<SavedFilterKey> keys) {
    final Map<SavedFilterKey, SavedFilter> savedFilterMap = new HashMap<>();

    for (final SavedFilterKey key : keys) {
      try {
        savedFilterMap.put(key, savedFilterCache.get(context.buildInternalContextualKey(key)));
      } catch (ExecutionException e) {
        log.error("Error while fetching saved filters for key {}", key, e);
      }
    }

    return unmodifiableMap(savedFilterMap);
  }

  public void updateBasedOnChangeEvent(
      ConfigChangeEventKey eventKey, ConfigChangeEventValue eventValue) {
    if (!eventKey.getConfigType().equals(SAVED_FILTER)) {
      return;
    }
    log.debug("Config change event is {}, {} ", eventKey, eventValue);
    switch (eventValue.getEventCase()) {
      case CREATE_EVENT:
        updateCacheValues(eventKey.getTenantId(), eventValue.getCreateEvent());
        break;
      case UPDATE_EVENT:
        updateCacheValues(eventKey.getTenantId(), eventValue.getUpdateEvent());
        break;
      case DELETE_EVENT:
        removeFromCache(eventKey.getTenantId(), eventValue.getDeleteEvent());
        break;
      default:
        log.warn(
            "Config change event value has invalid event type -> {}", eventValue.getEventCase());
    }
  }

  @SuppressWarnings("Convert2Diamond")
  @NotNull
  private LoadingCache<ContextualKey<SavedFilterKey>, SavedFilter> buildSavedFilterCache(
      SavedFilterCacheConfig savedFilterCacheConfig) {
    return CacheBuilder.newBuilder()
        .expireAfterAccess(
            savedFilterCacheConfig.getExpiryDurationAfterWrite().getSeconds(), SECONDS)
        .maximumSize(savedFilterCacheConfig.getMaxCacheSize())
        .recordStats()
        .build(
            new CacheLoader<ContextualKey<SavedFilterKey>, SavedFilter>() {
              @Nonnull
              @Override
              public SavedFilter load(@Nonnull ContextualKey<SavedFilterKey> key) {
                return loadSavedFilterFromSource(key);
              }
            });
  }

  private SavedFilter loadSavedFilterFromSource(ContextualKey<SavedFilterKey> contextualKey) {

    List<SavedFilter> savedFilterSet =
        savedFilterServiceClient.getSavedFilter(
            contextualKey.getContext(), contextualKey.getData());

    if (savedFilterSet.isEmpty()) {
      throw Status.NOT_FOUND
          .withDescription(String.format("No saved filters found for key %s", contextualKey))
          .asRuntimeException();
    }
    if (savedFilterSet.size() > 1) {
      throw new IllegalStateException(
          String.format(
              "Identifying attributes must produce only one saved filter but for key %s we have %s saved filters in total",
              contextualKey, savedFilterSet.size()));
    }

    return savedFilterSet.get(0);
  }

  private void updateCacheValues(String tenantId, ConfigCreateEvent createdConfig) {

    SavedFilter savedFilter = getSavedFilterFromJsonString(createdConfig.getCreatedConfigJson());
    SavedFilterKey savedFilterKey = SavedFilterKey.builder().id(savedFilter.getId()).build();

    ContextualKey<SavedFilterKey> contextualKey =
        RequestContext.forTenantId(tenantId).buildInternalContextualKey(savedFilterKey);
    savedFilterCache.put(contextualKey, savedFilter);
  }

  private void updateCacheValues(String tenantId, ConfigUpdateEvent updatedConfig) {

    SavedFilter latestSavedFilter =
        getSavedFilterFromJsonString(updatedConfig.getLatestConfigJson());
    SavedFilterKey savedFilterKey = SavedFilterKey.builder().id(latestSavedFilter.getId()).build();

    ContextualKey<SavedFilterKey> contextualKey =
        RequestContext.forTenantId(tenantId).buildInternalContextualKey(savedFilterKey);
    if (savedFilterCache.getIfPresent(contextualKey) != null) {
      savedFilterCache.put(contextualKey, latestSavedFilter);
    }
  }

  private void removeFromCache(String tenantId, ConfigDeleteEvent configDeleteEvent) {

    SavedFilter deletedSavedFilter =
        getSavedFilterFromJsonString(configDeleteEvent.getDeletedConfigJson());
    SavedFilterKey savedFilterKey = SavedFilterKey.builder().id(deletedSavedFilter.getId()).build();

    ContextualKey<SavedFilterKey> contextualKey =
        RequestContext.forTenantId(tenantId).buildInternalContextualKey(savedFilterKey);
    savedFilterCache.invalidate(contextualKey);
  }

  @NotNull
  private SavedFilter getSavedFilterFromJsonString(String configString) {
    SavedFilter.Builder savedFilterBuilder = SavedFilter.newBuilder();
    try {
      ConfigProtoConverter.mergeFromJsonString(configString, savedFilterBuilder);
    } catch (InvalidProtocolBufferException e) {
      log.error("Error while converting json string to saved filter config");
      throw new RuntimeException(e);
    }
    return savedFilterBuilder.build();
  }
}
