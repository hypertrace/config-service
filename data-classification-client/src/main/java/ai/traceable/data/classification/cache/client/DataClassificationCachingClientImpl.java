package ai.traceable.data.classification.cache.client;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import ai.traceable.data.classification.cache.config.DataClassificationInfoCachingClientConfig;
import ai.traceable.data.classification.cache.info.DataClassificationInfo;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest.DataTypeFilter;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class DataClassificationCachingClientImpl implements DataClassificationClient {
  private final DataClassificationInfoCachingClientConfig dataClassificationInfoCachingClientConfig;
  private final LoadingCache<ContextualKey<Optional<DataTypeFilter>>, DataClassificationInfo>
      dataClassificationInfoCache;
  private final DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub
      dataClassificationConfigServiceBlockingStub;

  public DataClassificationCachingClientImpl(
      DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub
          dataClassificationConfigServiceBlockingStub,
      DataClassificationInfoCachingClientConfig dataClassificationInfoCachingClientConfig) {
    this.dataClassificationInfoCachingClientConfig = dataClassificationInfoCachingClientConfig;
    this.dataClassificationConfigServiceBlockingStub = dataClassificationConfigServiceBlockingStub;
    this.dataClassificationInfoCache = buildCache();
    registerCacheMetrics();
  }

  public DataClassificationCachingClientImpl(
      KafkaLiveEventListener.Builder<ConfigChangeEventKey, ConfigChangeEventValue>
          kafkaLiveEventListenerBuilder,
      DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub
          dataClassificationConfigServiceBlockingStub,
      DataClassificationInfoCachingClientConfig dataClassificationInfoCachingClientConfig) {
    this(dataClassificationConfigServiceBlockingStub, dataClassificationInfoCachingClientConfig);
    kafkaLiveEventListenerBuilder.registerCallback(this::updateCacheBasedOnEvent);
  }

  private LoadingCache<ContextualKey<Optional<DataTypeFilter>>, DataClassificationInfo>
      buildCache() {
    return CacheBuilder.newBuilder()
        .maximumSize(this.dataClassificationInfoCachingClientConfig.getMaxSize())
        .refreshAfterWrite(this.dataClassificationInfoCachingClientConfig.getRefreshDuration())
        .expireAfterAccess(this.dataClassificationInfoCachingClientConfig.getExpirationDuration())
        .recordStats()
        .build(
            CacheLoader.asyncReloading(
                CacheLoader.from(this::fetchDataClassificationInfo),
                Executors.newFixedThreadPool(
                    this.dataClassificationInfoCachingClientConfig.getMaxThreadPoolSize(),
                    this.buildDataClassificationThreadFactory())));
  }

  private void registerCacheMetrics() {
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        this.dataClassificationInfoCachingClientConfig.getDataClassificationInfoCacheName(),
        this.dataClassificationInfoCache,
        Collections.emptyMap(),
        this.dataClassificationInfoCachingClientConfig.getMaxSize());
  }

  @Override
  public DataClassificationInfo getDataClassificationInfo(RequestContext requestContext) {

    return this.dataClassificationInfoCache.getUnchecked(
        requestContext.buildInternalContextualKey(Optional.empty()));
  }

  @Override
  public DataClassificationInfo getDataClassificationInfo(
      RequestContext requestContext, @Nonnull DataTypeFilter dataTypeFilter) {
    return this.dataClassificationInfoCache.getUnchecked(
        requestContext.buildInternalContextualKey(Optional.of(dataTypeFilter)));
  }

  private DataClassificationInfo fetchDataClassificationInfo(
      ContextualKey<Optional<DataTypeFilter>> contextualKey) {
    GetDataTypesRequest.Builder requestBuilder =
        GetDataTypesRequest.newBuilder().setResolveInheritedDetails(true);
    contextualKey.getData().ifPresent(requestBuilder::setFilter);
    GetDataTypesResponse dataTypesResponse =
        contextualKey.callInContext(
            () ->
                dataClassificationConfigServiceBlockingStub
                    .withDeadlineAfter(
                        this.dataClassificationInfoCachingClientConfig
                            .getTimeoutDuration()
                            .toMillis(),
                        MILLISECONDS)
                    .getDataTypes(requestBuilder.build()));
    Map<String, DataType> dataTypeIdToDataTypeMap =
        Maps.uniqueIndex(dataTypesResponse.getDataTypesList(), DataType::getId);
    return new DataClassificationInfo(
        dataTypeIdToDataTypeMap, Map.copyOf(dataTypesResponse.getReferencedDataSetsByIdMap()));
  }

  private ThreadFactory buildDataClassificationThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(
            this.dataClassificationInfoCachingClientConfig
                .getDataClassificationInfoCacheThreadFactoryName())
        .build();
  }

  private void updateCacheBasedOnEvent(ConfigChangeEventKey key, ConfigChangeEventValue value) {
    if (!key.getConfigType().equals(DataType.class.getName())
        && !key.getConfigType().equals(DataSet.class.getName())) {
      return;
    }
    RequestContext requestContext = RequestContext.forTenantId(key.getTenantId());
    switch (value.getEventCase()) {
      case CREATE_EVENT:
      case UPDATE_EVENT:
      case DELETE_EVENT:
        this.dataClassificationInfoCache.invalidate(requestContext.buildInternalContextualKey());
        break;
      default:
    }
  }
}
