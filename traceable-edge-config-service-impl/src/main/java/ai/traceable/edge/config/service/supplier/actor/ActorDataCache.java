package ai.traceable.edge.config.service.supplier.actor;

import static ai.traceable.datamodel.data.transformation.config.DataModelEntityConstants.USER_ENTITY_TYPE;
import static ai.traceable.datamodel.data.transformation.config.DataModelEntityConstants.USER_IP_ADDRESSES_METADATA_KEY;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataModelEntity;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.edge.config.service.config.ActorServiceConfig;
import ai.traceable.edge.config.service.config.CacheConfig;
import ai.traceable.edge.decision.config.service.v1.EntityRuleData;
import ai.traceable.platform.actor.v1.Status;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ActorDataCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(ActorDataCache.class);

  private static final String ACTOR_DATA_CACHE = "ActorDataCache";
  private static final Set<Status> BLOCKED_ACTOR_STATUSES =
      Set.of(Status.STATUS_ALWAYS_DENIED, Status.STATUS_SUSPENDED);
  private static final Set<Status> ALLOWED_ACTOR_STATUSES =
      Set.of(Status.STATUS_ALWAYS_ALLOWED, Status.STATUS_SNOOZED);

  private final LoadingCache<ContextualKey<Optional<String>>, List<ActorData>> userCache;
  private final ActorStore actorStore;
  private final Clock clock;

  @Inject
  public ActorDataCache(ActorServiceConfig actorServiceConfig, ActorStore actorStore, Clock clock) {
    this.actorStore = actorStore;
    this.clock = clock;
    CacheConfig cacheConfig = actorServiceConfig.getCacheConfig();
    userCache =
        CacheBuilder.newBuilder()
            .maximumSize(cacheConfig.getMaxCacheSize())
            .expireAfterWrite(cacheConfig.getWriteExpirationDuration())
            .refreshAfterWrite(cacheConfig.getRefreshExpirationDuration())
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadValue), Executors.newSingleThreadExecutor()));
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        ACTOR_DATA_CACHE, userCache, Collections.emptyMap(), cacheConfig.getMaxCacheSize());
  }

  @SneakyThrows
  public List<EntityRuleData> getUserDataForBlockedActors(
      ContextualKey<Optional<String>> contextualKey) {
    return Optional.ofNullable(userCache.get(contextualKey))
        .map(
            actorData ->
                actorData.stream()
                    .filter(actor -> BLOCKED_ACTOR_STATUSES.contains(actor.getStatus()))
                    .filter(actor -> actor.isActive(clock))
                    .map(this::getEntityRuleData)
                    .collect(Collectors.toUnmodifiableList()))
        .orElse(Collections.emptyList());
  }

  @SneakyThrows
  public List<EntityRuleData> getEntityRuleDataForAllowedActors(
      ContextualKey<Optional<String>> contextualKey) {
    return Optional.ofNullable(userCache.get(contextualKey))
        .map(
            actorData ->
                actorData.stream()
                    .filter(actor -> ALLOWED_ACTOR_STATUSES.contains(actor.getStatus()))
                    .filter(actor -> actor.isActive(clock))
                    .map(this::getEntityRuleData)
                    .collect(Collectors.toUnmodifiableList()))
        .orElse(Collections.emptyList());
  }

  private List<ActorData> loadValue(ContextualKey<Optional<String>> contextualKey) {
    RequestContext requestContext = contextualKey.getContext();
    Optional<String> environmentId = contextualKey.getData();
    return actorStore.getActiveThreatActorsWithUserId(requestContext, environmentId);
  }

  private EntityRuleData getEntityRuleData(ActorData actorData) {
    return EntityRuleData.newBuilder()
        .setEntity(
            DataModelEntity.newBuilder()
                .setId(actorData.getEntityId())
                .setType(USER_ENTITY_TYPE)
                .setName(actorData.getActorId())
                .addMetadataAttributes(
                    AttributeDerivationMapping.newBuilder()
                        .setName(USER_IP_ADDRESSES_METADATA_KEY)
                        .setType(FieldType.FIELD_TYPE_STR)
                        .addRules(
                            DerivationRule.newBuilder()
                                .setTransformationConfig(
                                    DataTransformationConfig.newBuilder()
                                        .setStaticValue(
                                            Value.newBuilder()
                                                .setListValue(
                                                    ListValue.newBuilder()
                                                        .addAllValues(
                                                            actorData.getIpAddresses().stream()
                                                                .map(
                                                                    ip ->
                                                                        Value.newBuilder()
                                                                            .setStringValue(ip)
                                                                            .build())
                                                                .collect(Collectors.toList()))))))))
        .build();
  }
}
