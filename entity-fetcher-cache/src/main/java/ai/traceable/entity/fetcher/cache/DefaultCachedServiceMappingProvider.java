package ai.traceable.entity.fetcher.cache;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.platform.cache.TimedCacheConfig;
import ai.traceable.platform.cache.TimedCacheValueProvider;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.time.Clock;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Singleton
@Slf4j
class DefaultCachedServiceMappingProvider
    extends TimedCacheValueProvider<ContextualKey<String>, Optional<ServiceIdentifierEntity>>
    implements CachedServiceMappingProvider {
  private final EntityQueryServiceClient entityQueryServiceClient;

  @Inject
  DefaultCachedServiceMappingProvider(
      @Named(CachedServiceMappingProviderModule.SERVICE_MAPPING_CACHE_NAME) String cacheName,
      EntityQueryServiceClient entityQueryServiceClient,
      Clock clock,
      TimedCacheConfig cacheConfig,
      UuidGenerator uuidGenerator) {
    super(cacheName + "-" + uuidGenerator.generateRandomId(), clock, cacheConfig);
    this.entityQueryServiceClient = entityQueryServiceClient;
  }

  @Override
  protected Optional<ServiceIdentifierEntity> loadValue(
      @NonNull ContextualKey<String> serviceIdContextualKey) {
    return entityQueryServiceClient.getServiceEntity(serviceIdContextualKey);
  }

  @Override
  protected boolean isValueNotLoaded(Optional<ServiceIdentifierEntity> serviceIdentifierEntity) {
    return serviceIdentifierEntity.isEmpty();
  }

  @Override
  public Optional<ServiceIdentifierEntity> getServiceIdentifierEntity(
      RequestContext requestContext, @NonNull String serviceId) {
    return this.getValue(requestContext.buildInternalContextualKey(serviceId));
  }
}
