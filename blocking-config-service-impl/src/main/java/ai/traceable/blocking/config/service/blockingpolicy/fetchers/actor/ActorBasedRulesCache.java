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
import ai.traceable.platform.actor.v1.Actor;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.platform.actor.v1.GetActorsByStatusRequest;
import ai.traceable.platform.actor.v1.GetActorsByStatusResponse;
import ai.traceable.platform.actor.v1.StatusChangeSource;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.platform.validator.ExternalIpAddressValidator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.protobuf.StatusProto;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActorBasedRulesCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(ActorBasedRulesCache.class);
  private static final ExternalIpAddressValidator externalIpAddressValidator =
      ExternalIpAddressValidator.getInstance();

  private final ActorServiceBlockingStub actorServiceStub;
  private final LoadingCache<ContextualKey<Void>, ActorBasedRulesCollection> actorCache;
  private final Duration callTimeout;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public ActorBasedRulesCache(
      ActorServiceBlockingStub actorServiceStub,
      ActorServiceConfig actorServiceConfig,
      BlockingRulesUtils blockingRulesUtils) {
    this.actorServiceStub = actorServiceStub;
    this.callTimeout = actorServiceConfig.getCallTimeoutDuration();
    this.blockingRulesUtils = blockingRulesUtils;
    BlockingDataCacheConfig blockingDataCacheConfig = actorServiceConfig.getCacheConfig();
    actorCache =
        CacheBuilder.newBuilder()
            .maximumSize(blockingDataCacheConfig.getMaxCacheSize())
            .expireAfterWrite(blockingDataCacheConfig.getWriteExpirationDuration())
            .refreshAfterWrite(blockingDataCacheConfig.getRefreshExpirationDuration())
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadValue), Executors.newSingleThreadExecutor()));
  }

  public ActorBasedRulesCollection getActorBasedRules(ContextualKey<Void> contextualKey) {
    try {
      return actorCache.get(contextualKey);
    } catch (Exception e) {
      Status status =
          Status.newBuilder()
              .setCode(Code.INTERNAL.getNumber())
              .setMessage("Error retrieving active actors from cache " + e.getMessage())
              .build();
      throw StatusProto.toStatusRuntimeException(status);
    }
  }

  private ActorBasedRulesCollection loadValue(ContextualKey<Void> contextualKey) {
    RequestContext requestContext = contextualKey.getContext();

    GetActorsByStatusResponse actorsByStatusResponse =
        requestContext.call(
            () ->
                actorServiceStub
                    .withDeadlineAfter(callTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .getActorsByStatus(
                        GetActorsByStatusRequest.newBuilder()
                            .addAllStatus(
                                ImmutableList.of(
                                    STATUS_ALWAYS_DENIED,
                                    STATUS_SUSPENDED,
                                    STATUS_ALWAYS_ALLOWED,
                                    STATUS_SNOOZED))
                            .build()));

    List<BlockingDetails> threatActorBasedIpViolations = new ArrayList<>();
    List<BlockingDetails> threatActorBasedIpExemptions = new ArrayList<>();
    List<BlockingDetails> rateLimitBasedIpViolations = new ArrayList<>();

    actorsByStatusResponse.getActorsList().stream()
        .filter(this::filterActor)
        .forEach(
            actor -> {
              if (actor.getStatusChangeSource()
                  == StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT) {
                // Rate limit error
                rateLimitBasedIpViolations.addAll(
                    this.generateBlockingDetails(
                        actor,
                        BLOCKING_CATEGORY_RATE_LIMIT,
                        BLOCKING_RULE_TYPE_BLOCK,
                        ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
                            actor.getEntityId(),
                            actor.getStatusChangeDetails().getRateLimitDetails().getRuleId(),
                            actor.getStatusChangeDetails().getRateLimitDetails().getRuleName())));
              } else {
                switch (actor.getStatus()) {
                  case STATUS_NORMAL:
                  case STATUS_THREAT_ACTOR:
                  case STATUS_RESOLVED:
                    break;
                  case STATUS_ALWAYS_ALLOWED:
                  case STATUS_SNOOZED:
                    threatActorBasedIpExemptions.addAll(
                        this.generateBlockingDetails(
                            actor,
                            BLOCKING_CATEGORY_THREAT_ACTOR,
                            BLOCKING_RULE_TYPE_ALLOW,
                            ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo(
                                actor.getEntityId())));
                    break;
                  case STATUS_ALWAYS_DENIED:
                  case STATUS_SUSPENDED:
                    threatActorBasedIpViolations.addAll(
                        this.generateBlockingDetails(
                            actor,
                            BLOCKING_CATEGORY_THREAT_ACTOR,
                            BLOCKING_RULE_TYPE_BLOCK,
                            ViolationInfoEncoder.getEncodedThreatActorViolationInfo(
                                actor.getEntityId())));
                    break;
                  default:
                    LOGGER.warn("Unsupported actor status type {}", actor.getStatus());
                }
              }
            });

    return new ActorBasedRulesCollection(
        threatActorBasedIpViolations, threatActorBasedIpExemptions, rateLimitBasedIpViolations);
  }

  private boolean filterActor(Actor actor) {
    return !parseIpAddresses(actor.getIpAddressesList()).isEmpty()
        && blockingRulesUtils.isRuleActive(actor.getStatusExpiryTimestamp());
  }

  private List<BlockingDetails> generateBlockingDetails(
      Actor actor,
      BlockingCategory blockingCategory,
      BlockingRuleType blockingRuleType,
      String info) {
    BlockingDetails actorDetails =
        BlockingDetails.newBuilder()
            .setCategory(blockingCategory)
            .setBlockingRuleType(blockingRuleType)
            .setInfo(info)
            .setExpirationTimestamp(actor.getStatusExpiryTimestamp())
            .setStatus(
                blockingRulesUtils.generateBlockingStatus(
                    actor.getStatusExpiryTimestamp(), blockingRuleType))
            .setActorDetails(
                ActorDetails.newBuilder()
                    .addAllIpAddresses(parseIpAddresses(actor.getIpAddressesList()))
                    .setUserId(actor.getActorId()))
            .build();

    // for backward compatibility
    BlockingDetails ipDetails =
        actorDetails.toBuilder()
            .clearActorDetails()
            .setIpDetails(
                IpDetails.newBuilder()
                    .addAllIpAddresses(parseIpAddresses(actor.getIpAddressesList())))
            .build();

    return List.of(actorDetails, ipDetails);
  }

  private List<String> parseIpAddresses(List<String> ipAddresses) {
    return ipAddresses.stream()
        .filter(externalIpAddressValidator::validate)
        .collect(Collectors.toUnmodifiableList());
  }
}
