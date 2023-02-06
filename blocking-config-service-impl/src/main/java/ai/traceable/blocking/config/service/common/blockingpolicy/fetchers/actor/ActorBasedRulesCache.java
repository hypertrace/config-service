package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor;

import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_ALLOWED;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_DENIED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SNOOZED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SUSPENDED;

import ai.traceable.blocking.config.service.BlockingDataCacheConfig;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.platform.actor.v1.RateLimitCategory;
import ai.traceable.platform.actor.v1.StatusChangeSource;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.platform.utils.ip.IpValidationUtils;
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

    List<BlockingPolicyData> threatActorBasedIpViolations = new ArrayList<>();
    List<BlockingPolicyData> threatActorBasedIpExemptions = new ArrayList<>();
    List<BlockingPolicyData> rateLimitBasedIpViolations = new ArrayList<>();
    List<BlockingPolicyData> emailDomainBasedExemptions = new ArrayList<>();
    List<BlockingPolicyData> emailDomainBasedViolations = new ArrayList<>();

    actorStatusDetailsList.stream()
        .filter(
            actorStatusDetails -> !parseIpAddresses(actorStatusDetails.getIpAddresses()).isEmpty())
        .forEach(
            actor -> {
              if (actor.getStatus() == STATUS_ALWAYS_ALLOWED
                  || actor.getStatus() == STATUS_SNOOZED) {
                // Exemptions
                if (actor.getStatusChangeSource()
                    == StatusChangeSource.STATUS_CHANGE_SOURCE_MALICIOUS_SOURCES) {
                  emailDomainBasedExemptions.add(
                      this.generateBlockingDetails(
                          Category.EMAIL_DOMAIN_RULE,
                          RuleType.ALLOW,
                          ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
                              actor
                                  .getStatusChangeDetails()
                                  .getMaliciousSourcesDetails()
                                  .getRuleId(),
                              actor
                                  .getStatusChangeDetails()
                                  .getMaliciousSourcesDetails()
                                  .getRuleName(),
                              "",
                              Optional.of(actor.getEntityId()),
                              List.of(
                                  MaliciousSourcesRuleCondition.ConditionCase
                                      .EMAIL_DOMAIN_CONDITION)),
                          actor));
                } else {
                  threatActorBasedIpExemptions.add(
                      this.generateBlockingDetails(
                          Category.THREAT_ACTOR,
                          RuleType.ALLOW,
                          ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo(
                              actor.getEntityId()),
                          actor));
                }
              } else if (actor.getStatus() == STATUS_ALWAYS_DENIED
                  || actor.getStatus() == STATUS_SUSPENDED) {
                // Violations
                switch (actor.getStatusChangeSource()) {
                  case STATUS_CHANGE_SOURCE_RATE_LIMIT:
                    rateLimitBasedIpViolations.add(
                        this.generateBlockingDetails(
                            getBlockingCategory(
                                actor
                                    .getStatusChangeDetails()
                                    .getRateLimitDetails()
                                    .getRuleCategory()),
                            RuleType.BLOCK,
                            ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
                                actor.getEntityId(),
                                actor.getStatusChangeDetails().getRateLimitDetails().getRuleId(),
                                actor.getStatusChangeDetails().getRateLimitDetails().getRuleName(),
                                actor
                                    .getStatusChangeDetails()
                                    .getRateLimitDetails()
                                    .getRuleCategory()),
                            actor));
                    break;
                  case STATUS_CHANGE_SOURCE_MALICIOUS_SOURCES:
                    emailDomainBasedViolations.add(
                        this.generateBlockingDetails(
                            Category.EMAIL_DOMAIN_RULE,
                            RuleType.BLOCK,
                            ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
                                actor
                                    .getStatusChangeDetails()
                                    .getMaliciousSourcesDetails()
                                    .getRuleId(),
                                actor
                                    .getStatusChangeDetails()
                                    .getMaliciousSourcesDetails()
                                    .getRuleName(),
                                "",
                                Optional.of(actor.getEntityId()),
                                List.of(
                                    MaliciousSourcesRuleCondition.ConditionCase
                                        .EMAIL_DOMAIN_CONDITION)),
                            actor));
                    break;
                  default:
                    threatActorBasedIpViolations.add(
                        this.generateBlockingDetails(
                            Category.THREAT_ACTOR,
                            RuleType.BLOCK,
                            ViolationInfoEncoder.getEncodedThreatActorViolationInfo(
                                actor.getEntityId()),
                            actor));
                }
              }
            });

    ActorBasedRulesCollection response =
        new ActorBasedRulesCollection(
            threatActorBasedIpViolations,
            threatActorBasedIpExemptions,
            rateLimitBasedIpViolations,
            emailDomainBasedExemptions,
            emailDomainBasedViolations);

    LOGGER.debug(
        String.format(
            "For tenant - %s, parsed actors response is - %s",
            requestContext.getTenantId(), response));

    return response;
  }

  private Category getBlockingCategory(RateLimitCategory rateLimitCategory) {
    switch (rateLimitCategory) {
      case RATE_LIMIT_CATEGORY_DATA_EXFILTRATION:
        return Category.DATA_EXFILTRATION;
      case RATE_LIMIT_CATEGORY_ENUMERATION:
        return Category.ENUMERATION;
      default:
        return Category.RATE_LIMIT;
    }
  }

  private BlockingPolicyData generateBlockingDetails(
      Category category, RuleType ruleType, String info, ActorStatusDetails actorStatusDetails) {
    return BlockingPolicyData.builder()
        .category(category)
        .ruleType(ruleType)
        .info(info)
        .timestamp(actorStatusDetails.getExpirationTimestampMillis())
        .status(
            blockingRulesUtils.generateBlockingStatus(
                actorStatusDetails.getExpirationTimestampMillis(), ruleType))
        .ipAddresses(parseIpAddresses(actorStatusDetails.getIpAddresses()))
        .userId(actorStatusDetails.getActorId())
        .build();
  }

  private static List<String> parseIpAddresses(List<String> ipAddresses) {
    return ipAddresses.stream()
        .filter(ip -> IpValidationUtils.getIpValidationResult(ip).isExternalIp())
        .collect(Collectors.toUnmodifiableList());
  }
}
