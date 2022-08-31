package ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_ALLOWED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_ALLOWED;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_DENIED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SNOOZED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SUSPENDED;
import static ai.traceable.platform.actor.v1.StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT;
import static ai.traceable.platform.actor.v1.StatusChangeSource.STATUS_CHANGE_SOURCE_SYSTEM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.BlockingDataCacheConfig;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.platform.actor.v1.Actor;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.platform.actor.v1.GetActorsByStatusRequest;
import ai.traceable.platform.actor.v1.GetActorsByStatusResponse;
import ai.traceable.platform.actor.v1.RateLimitDetails;
import ai.traceable.platform.actor.v1.StatusChangeDetails;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorBasedRulesCacheTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  private ActorServiceBlockingStub actorServiceBlockingStub;
  private ActorBasedRulesCache actorBasedRulesCache;

  @BeforeEach
  void setUp() {
    actorServiceBlockingStub = mock(ActorServiceBlockingStub.class, RETURNS_DEEP_STUBS);

    BlockingRulesUtils blockingRulesUtils = mock(BlockingRulesUtils.class);
    doReturn(true).when(blockingRulesUtils).isRuleActive(activeTimestamp);
    doReturn(false).when(blockingRulesUtils).isRuleActive(inactiveTimestamp);
    doReturn(BLOCKING_STATUS_ALLOWED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_ALLOW);
    doReturn(BLOCKING_STATUS_DENIED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_BLOCK);

    ActorServiceConfig actorServiceConfig = mock(ActorServiceConfig.class);
    doReturn(Duration.ofSeconds(30)).when(actorServiceConfig).getCallTimeoutDuration();
    doReturn(
            new BlockingDataCacheConfig(
                ConfigFactory.parseMap(
                    Map.of(
                        "cache",
                        Map.of(
                            "maxCacheSize",
                            10,
                            "expireAfterWriteDuration",
                            "2m",
                            "refreshAfterWriteDuration",
                            "10m")))))
        .when(actorServiceConfig)
        .getCacheConfig();

    actorBasedRulesCache =
        new ActorBasedRulesCache(actorServiceBlockingStub, actorServiceConfig, blockingRulesUtils);
  }

  @Test
  void getActorBasedRulesEmpty() {
    when(actorServiceBlockingStub
            .withDeadlineAfter(30000L, TimeUnit.MILLISECONDS)
            .getActorsByStatus(
                GetActorsByStatusRequest.newBuilder()
                    .addAllStatus(
                        ImmutableList.of(
                            STATUS_ALWAYS_DENIED,
                            STATUS_SUSPENDED,
                            STATUS_ALWAYS_ALLOWED,
                            STATUS_SNOOZED))
                    .build()))
        .thenReturn(GetActorsByStatusResponse.getDefaultInstance());

    ActorBasedRulesCollection response =
        actorBasedRulesCache.getActorBasedRules(REQUEST_CONTEXT.buildInternalContextualKey());
    assertEquals(0, response.getRateLimitBasedIpViolations().size());
    assertEquals(0, response.getThreatActorBasedIpExemptions().size());
    assertEquals(0, response.getRateLimitBasedIpViolations().size());
  }

  @Test
  void getActorBasedRules() {
    when(actorServiceBlockingStub
            .withDeadlineAfter(30000L, TimeUnit.MILLISECONDS)
            .getActorsByStatus(
                GetActorsByStatusRequest.newBuilder()
                    .addAllStatus(
                        ImmutableList.of(
                            STATUS_ALWAYS_DENIED,
                            STATUS_SUSPENDED,
                            STATUS_ALWAYS_ALLOWED,
                            STATUS_SNOOZED))
                    .build()))
        .thenReturn(
            GetActorsByStatusResponse.newBuilder().addAllActors(generateSampleResponse()).build());

    ActorBasedRulesCollection response =
        actorBasedRulesCache.getActorBasedRules(REQUEST_CONTEXT.buildInternalContextualKey());

    List<BlockingDetails> violations = response.getThreatActorBasedIpViolations();
    assertEquals(1, violations.size());
    assertEquals(
        List.of("1.1.1.1", "2.2.2.2"), violations.get(0).getIpDetails().getIpAddressesList());
    assertEquals(BLOCKING_CATEGORY_THREAT_ACTOR, violations.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedThreatActorViolationInfo("entity-2"),
        violations.get(0).getInfo());

    List<BlockingDetails> exemptions = response.getThreatActorBasedIpExemptions();
    assertEquals(1, exemptions.size());
    assertEquals(
        List.of("1.1.1.1", "2.2.2.2"), exemptions.get(0).getIpDetails().getIpAddressesList());
    assertEquals(BLOCKING_CATEGORY_THREAT_ACTOR, exemptions.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_ALLOW, exemptions.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_ALLOWED, exemptions.get(0).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo("entity-7"),
        exemptions.get(0).getInfo());

    List<BlockingDetails> rateLimitBasedIpViolation = response.getRateLimitBasedIpViolations();
    assertEquals(1, rateLimitBasedIpViolation.size());
    assertEquals(
        List.of("1.1.1.1"), rateLimitBasedIpViolation.get(0).getIpDetails().getIpAddressesList());
    assertEquals(BLOCKING_CATEGORY_RATE_LIMIT, rateLimitBasedIpViolation.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, rateLimitBasedIpViolation.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, rateLimitBasedIpViolation.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
            "entity-1", "rate-limit-id-1", "rate-limit-name-1"),
        rateLimitBasedIpViolation.get(0).getInfo());
  }

  private static List<Actor> generateSampleResponse() {
    return List.of(
        Actor.newBuilder()
            .setActorId("actor-1")
            .setEntityId("entity-1")
            .addAllIpAddresses(List.of("1.1.1.1", "192.168.65.3"))
            .setStatusChangeSource(STATUS_CHANGE_SOURCE_RATE_LIMIT)
            .setStatusChangeDetails(
                StatusChangeDetails.newBuilder()
                    .setRateLimitDetails(
                        RateLimitDetails.newBuilder()
                            .setRuleId("rate-limit-id-1")
                            .setRuleName("rate-limit-name-1")
                            .build())
                    .build())
            .setStatusExpiryTimestamp(activeTimestamp)
            .build(),
        Actor.newBuilder()
            .setActorId("actor-2")
            .setEntityId("entity-2")
            .setStatus(STATUS_SUSPENDED)
            .addAllIpAddresses(List.of("1.1.1.1", "192.168.65.3", "2.2.2.2"))
            .setStatusChangeSource(STATUS_CHANGE_SOURCE_SYSTEM)
            .setStatusExpiryTimestamp(activeTimestamp)
            .build(),
        Actor.newBuilder()
            .setActorId("actor-3")
            .setEntityId("entity-3")
            .addAllIpAddresses(List.of("1.1.1.1", "2.2.2.2"))
            .setStatusChangeSource(STATUS_CHANGE_SOURCE_RATE_LIMIT)
            .setStatusChangeDetails(
                StatusChangeDetails.newBuilder()
                    .setRateLimitDetails(
                        RateLimitDetails.newBuilder()
                            .setRuleId("rate-limit-id-2")
                            .setRuleName("rate-limit-name-2")
                            .build())
                    .build())
            .setStatusExpiryTimestamp(inactiveTimestamp)
            .build(),
        Actor.newBuilder()
            .setActorId("actor-4")
            .setEntityId("entity-4")
            .setStatus(STATUS_SUSPENDED)
            .addAllIpAddresses(List.of("1.1.1.1", "2.2.2.2"))
            .setStatusChangeSource(STATUS_CHANGE_SOURCE_SYSTEM)
            .setStatusExpiryTimestamp(inactiveTimestamp)
            .build(),
        Actor.newBuilder()
            .setActorId("actor-5")
            .setEntityId("entity-5")
            .setStatusChangeSource(STATUS_CHANGE_SOURCE_RATE_LIMIT)
            .setStatusChangeDetails(
                StatusChangeDetails.newBuilder()
                    .setRateLimitDetails(
                        RateLimitDetails.newBuilder()
                            .setRuleId("rate-limit-id-3")
                            .setRuleName("rate-limit-name-3")
                            .build())
                    .build())
            .setStatusExpiryTimestamp(activeTimestamp)
            .build(),
        Actor.newBuilder()
            .setActorId("actor-6")
            .setEntityId("entity-6")
            .setStatus(STATUS_SNOOZED)
            .setStatusChangeSource(STATUS_CHANGE_SOURCE_SYSTEM)
            .setStatusExpiryTimestamp(activeTimestamp)
            .build(),
        Actor.newBuilder()
            .setActorId("actor-7")
            .setEntityId("entity-7")
            .setStatus(STATUS_SNOOZED)
            .addAllIpAddresses(List.of("1.1.1.1", "192.168.65.3", "2.2.2.2"))
            .setStatusChangeSource(STATUS_CHANGE_SOURCE_SYSTEM)
            .setStatusExpiryTimestamp(activeTimestamp)
            .build());
  }
}
