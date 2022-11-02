package ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_ALLOWED;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_DENIED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SNOOZED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SUSPENDED;

import ai.traceable.blocking.config.service.BlockingDataCacheConfig;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.ActorDetails;
import ai.traceable.blocking.config.service.v1.BlockingCategory;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.blocking.config.service.v1.IpDetails;
import ai.traceable.platform.actor.v1.StatusChangeSource;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.platform.validator.ExternalIpAddressValidator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.protobuf.StatusProto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActorBasedRulesCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(ActorBasedRulesCache.class);
  private static final String ACTOR_BASED_RULES_CACHE = "ActorBasedRulesCache";
  private static final ExternalIpAddressValidator externalIpAddressValidator =
      ExternalIpAddressValidator.getInstance();

  private final LoadingCache<ContextualKey<Optional<String>>, ActorBasedRulesCollection> actorCache;
  private final BlockingRulesUtils blockingRulesUtils;
  private final ActorStore actorStore;

  @Inject
  public ActorBasedRulesCache(
      ActorServiceConfig actorServiceConfig,
      BlockingRulesUtils blockingRulesUtils,
      ActorStore actorStore) {
    this.blockingRulesUtils = blockingRulesUtils;
    this.actorStore = actorStore;
    BlockingDataCacheConfig blockingDataCacheConfig = actorServiceConfig.getCacheConfig();
    actorCache =
        CacheBuilder.newBuilder()
            .maximumSize(blockingDataCacheConfig.getMaxCacheSize())
            .expireAfterWrite(blockingDataCacheConfig.getWriteExpirationDuration())
            .refreshAfterWrite(blockingDataCacheConfig.getRefreshExpirationDuration())
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadValue), Executors.newSingleThreadExecutor()));
    PlatformMetricsRegistry.registerCache(
        ACTOR_BASED_RULES_CACHE, actorCache, Collections.emptyMap());
  }

  public ActorBasedRulesCollection getActorBasedRules(
      ContextualKey<Optional<String>> contextualKey) {
    try {
      return actorCache.get(contextualKey);
    } catch (Exception e) {
      LOGGER.error(
          String.format(
              "Error retrieving active actors from cache for context - %s: %s", contextualKey, e),
          e);
      Status status =
          Status.newBuilder()
              .setCode(Code.INTERNAL.getNumber())
              .setMessage("Error retrieving active actors from cache")
              .build();
      throw StatusProto.toStatusRuntimeException(status);
    }
  }

  private ActorBasedRulesCollection loadValue(ContextualKey<Optional<String>> contextualKey) {
    RequestContext requestContext = contextualKey.getContext();
    Optional<String> environmentId = contextualKey.getData();

    List<ActorStatusDetails> actorStatusDetailsList =
        actorStore.getActiveThreatActors(requestContext, environmentId);

    List<BlockingDetails> threatActorBasedIpViolations = new ArrayList<>();
    List<BlockingDetails> threatActorBasedIpExemptions = new ArrayList<>();
    List<BlockingDetails> rateLimitBasedIpViolations = new ArrayList<>();

    actorStatusDetailsList.stream()
        .filter(
            actorStatusDetails -> !parseIpAddresses(actorStatusDetails.getIpAddresses()).isEmpty())
        .forEach(
            actor -> {
              if (actor.getStatusChangeSource()
                  == StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT) {
                // Rate limit error
                rateLimitBasedIpViolations.addAll(
                    this.generateBlockingDetails(
                        BLOCKING_CATEGORY_RATE_LIMIT,
                        BLOCKING_RULE_TYPE_BLOCK,
                        ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
                            actor.getEntityId(),
                            actor.getStatusChangeDetails().getRateLimitDetails().getRuleId(),
                            actor.getStatusChangeDetails().getRateLimitDetails().getRuleName()),
                        actor));
              } else {
                if (actor.getStatus() == STATUS_ALWAYS_ALLOWED
                    || actor.getStatus() == STATUS_SNOOZED) {
                  // Exemption
                  threatActorBasedIpExemptions.addAll(
                      this.generateBlockingDetails(
                          BLOCKING_CATEGORY_THREAT_ACTOR,
                          BLOCKING_RULE_TYPE_ALLOW,
                          ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo(
                              actor.getEntityId()),
                          actor));
                } else if (actor.getStatus() == STATUS_ALWAYS_DENIED
                    || actor.getStatus() == STATUS_SUSPENDED) {
                  // Violation
                  threatActorBasedIpViolations.addAll(
                      this.generateBlockingDetails(
                          BLOCKING_CATEGORY_THREAT_ACTOR,
                          BLOCKING_RULE_TYPE_BLOCK,
                          ViolationInfoEncoder.getEncodedThreatActorViolationInfo(
                              actor.getEntityId()),
                          actor));
                }
              }
            });

    ActorBasedRulesCollection response =
        new ActorBasedRulesCollection(
            threatActorBasedIpViolations, threatActorBasedIpExemptions, rateLimitBasedIpViolations);

    LOGGER.debug(
        String.format(
            "For tenant - %s, parsed actors response is - %s",
            requestContext.getTenantId(), response));

    return response;
  }

  private List<BlockingDetails> generateBlockingDetails(
      BlockingCategory blockingCategory,
      BlockingRuleType blockingRuleType,
      String info,
      ActorStatusDetails actorStatusDetails) {
    BlockingDetails actorDetails =
        BlockingDetails.newBuilder()
            .setCategory(blockingCategory)
            .setBlockingRuleType(blockingRuleType)
            .setInfo(info)
            .setExpirationTimestamp(actorStatusDetails.getExpirationTimestampMillis())
            .setStatus(
                blockingRulesUtils.generateBlockingStatus(
                    actorStatusDetails.getExpirationTimestampMillis(), blockingRuleType))
            .setActorDetails(
                ActorDetails.newBuilder()
                    .addAllIpAddresses(parseIpAddresses(actorStatusDetails.getIpAddresses()))
                    .setUserId(actorStatusDetails.getActorId()))
            .build();

    // for backward compatibility
    BlockingDetails ipDetails =
        actorDetails.toBuilder()
            .clearActorDetails()
            .setIpDetails(
                IpDetails.newBuilder()
                    .addAllIpAddresses(parseIpAddresses(actorStatusDetails.getIpAddresses())))
            .build();

    return List.of(actorDetails, ipDetails);
  }

  private static List<String> parseIpAddresses(List<String> ipAddresses) {
    return ipAddresses.stream()
        .filter(externalIpAddressValidator::validate)
        .collect(Collectors.toUnmodifiableList());
  }
}
