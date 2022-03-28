package ai.traceable.localprocessing.config.service.apinaming.http;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.ServiceIdentifier;
import ai.traceable.localprocessing.config.service.client.EntityDataServiceClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;

@Slf4j
public class EntityFetcher {
  private static final String CACHE_REFRESH_DURATION =
      "api.naming.config.entity.fetcher.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRATION_DURATION =
      "api.naming.config.entity.fetcher.cache.expireAfterWriteDuration";
  private static final String MAXIMUM_CACHE_SIZE =
      "api.naming.config.entity.fetcher.cache.maximumCacheSize";
  private static final String CACHE_NAME = "serviceEntityCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofHours(12);
  private static final Duration CACHE_EXPIRATION_DURATION_DEFAULT = Duration.ofHours(24);
  private static final long MAXIMUM_CACHE_SIZE_DEFAULT = 1000;
  private static final String ENVIRONMENT_IDENTIFYING_ATTRIBUTE = "ENVIRONMENT";

  private final LoadingCache<ContextualKey<ServiceIdentifier>, Optional<Entity>> serviceEntityCache;
  private final EntityDataServiceClient entityDataServiceClient;

  @Inject
  public EntityFetcher(Config config, EntityDataServiceClient entityDataServiceClient) {
    Duration cacheRefreshDuration =
        config.hasPath(CACHE_REFRESH_DURATION)
            ? config.getDuration(CACHE_REFRESH_DURATION)
            : CACHE_REFRESH_DURATION_DEFAULT;
    Duration cacheExpiryDuration =
        config.hasPath(CACHE_EXPIRATION_DURATION)
            ? config.getDuration(CACHE_EXPIRATION_DURATION)
            : CACHE_EXPIRATION_DURATION_DEFAULT;
    long maximumCacheSize =
        config.hasPath(MAXIMUM_CACHE_SIZE)
            ? config.getLong(MAXIMUM_CACHE_SIZE)
            : MAXIMUM_CACHE_SIZE_DEFAULT;

    this.entityDataServiceClient = entityDataServiceClient;
    this.serviceEntityCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheRefreshDuration.toMillis(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheExpiryDuration.toMillis(), TimeUnit.MILLISECONDS)
            .maximumSize(maximumCacheSize)
            .recordStats()
            .build(CacheLoader.from(this::loadEntity));
    PlatformMetricsRegistry.registerCache(CACHE_NAME, serviceEntityCache, Collections.emptyMap());
  }

  private Optional<Entity> loadEntity(
      @Nonnull ContextualKey<ServiceIdentifier> serviceIdentifierContextualKey) {
    try {
      Entity entity =
          serviceIdentifierContextualKey.callInContext(
              serviceIdentifier ->
                  entityDataServiceClient.getByTypeAndIdentifyingProperties(
                      serviceIdentifierContextualKey.getContext(),
                      buildGetEntityByTypeAndIdentifyingAttributesRequest(
                          serviceIdentifier.getServiceName(), serviceIdentifier.getEnvironment())));
      if (entity == null) {
        return Optional.empty();
      }
      return Optional.of(entity);
    } catch (Exception e) {
      ServiceIdentifier serviceIdentifier = serviceIdentifierContextualKey.getData();
      log.error(
          "Could not fetch entity for tenant id:{}, service name: {} and environment : {}",
          serviceIdentifierContextualKey.getContext().getTenantId(),
          serviceIdentifier.getServiceName(),
          serviceIdentifier.getEnvironment(),
          e);
      return Optional.empty();
    }
  }

  private Optional<Entity> get(ContextualKey<ServiceIdentifier> serviceIdentifierContextualKey) {
    try {
      return serviceEntityCache.get(serviceIdentifierContextualKey);
    } catch (Exception e) {
      log.error(
          "Could not get entity for serviceIdentifierKey:{} with exception:{}",
          serviceIdentifierContextualKey,
          e);
      return Optional.empty();
    }
  }

  public Optional<Entity> getEntity(
      RequestContext requestContext, String serviceName, Optional<String> environment) {
    return get(
        requestContext.buildInternalContextualKey(new ServiceIdentifier(serviceName, environment)));
  }

  private ByTypeAndIdentifyingAttributes buildGetEntityByTypeAndIdentifyingAttributesRequest(
      String serviceName, Optional<String> environmentMaybe) {
    ByTypeAndIdentifyingAttributes.Builder byTypeAndIdentifyingAttributesBuilder =
        ByTypeAndIdentifyingAttributes.newBuilder()
            .setEntityType(EntityType.SERVICE.name())
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                AttributeValue.newBuilder()
                    .setValue(
                        org.hypertrace.entity.data.service.v1.Value.newBuilder()
                            .setString(serviceName)
                            .build())
                    .build());
    environmentMaybe.ifPresent(
        environment ->
            byTypeAndIdentifyingAttributesBuilder.putIdentifyingAttributes(
                ENVIRONMENT_IDENTIFYING_ATTRIBUTE,
                AttributeValue.newBuilder()
                    .setValue(
                        org.hypertrace.entity.data.service.v1.Value.newBuilder()
                            .setString(environment)
                            .build())
                    .build()));
    return byTypeAndIdentifyingAttributesBuilder.build();
  }
}
