package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

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

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl.ActorBasedDataFetcherImpl;
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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorBasedDataFetcherTest {

  private ActorServiceBlockingStub actorServiceStub;
  private ActorBasedDataFetcher actorBasedDataFetcher;

  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    actorServiceStub = mock(ActorServiceBlockingStub.class, RETURNS_DEEP_STUBS);

    ActorServiceConfig actorServiceConfig = mock(ActorServiceConfig.class);
    doReturn(Duration.ofSeconds(2)).when(actorServiceConfig).getCallTimeoutDuration();

    BlockingRulesUtils blockingRulesUtils = mock(BlockingRulesUtils.class);
    doReturn(true).when(blockingRulesUtils).isRuleActive(activeTimestamp);
    doReturn(false).when(blockingRulesUtils).isRuleActive(inactiveTimestamp);
    doReturn(BLOCKING_STATUS_ALLOWED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_ALLOW);
    doReturn(BLOCKING_STATUS_DENIED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_BLOCK);

    actorBasedDataFetcher =
        new ActorBasedDataFetcherImpl(actorServiceStub, actorServiceConfig, blockingRulesUtils);
  }

  @Test
  void getThreatActorBasedIpViolation() {
    when(actorServiceStub
            .withDeadlineAfter(2000, TimeUnit.MILLISECONDS)
            .getActorsByStatus(
                GetActorsByStatusRequest.newBuilder()
                    .addAllStatus(List.of(STATUS_ALWAYS_DENIED, STATUS_SUSPENDED))
                    .build()))
        .thenReturn(generateSampleActorResponse());

    List<BlockingDetails> violations = actorBasedDataFetcher.getThreatActorBasedIpViolation();
    assertEquals(1, violations.size());
    assertEquals(
        List.of("1.1.1.1", "2.2.2.2"), violations.get(0).getIpDetails().getIpAddressesList());
    assertEquals(BLOCKING_CATEGORY_THREAT_ACTOR, violations.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedThreatActorViolationInfo("entity-2"),
        violations.get(0).getInfo());
  }

  @Test
  void getThreatActorBasedIpExemption() {
    when(actorServiceStub
            .withDeadlineAfter(2000, TimeUnit.MILLISECONDS)
            .getActorsByStatus(
                GetActorsByStatusRequest.newBuilder()
                    .addAllStatus(List.of(STATUS_ALWAYS_ALLOWED, STATUS_SNOOZED))
                    .build()))
        .thenReturn(generateSampleActorResponse());

    List<BlockingDetails> exemptions = actorBasedDataFetcher.getThreatActorBasedIpExemption();
    assertEquals(1, exemptions.size());
    assertEquals(
        List.of("1.1.1.1", "2.2.2.2"), exemptions.get(0).getIpDetails().getIpAddressesList());
    assertEquals(BLOCKING_CATEGORY_THREAT_ACTOR, exemptions.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_ALLOW, exemptions.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_ALLOWED, exemptions.get(0).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo("entity-2"),
        exemptions.get(0).getInfo());
  }

  @Test
  void getRateLimitBasedIpViolation() {
    when(actorServiceStub
            .withDeadlineAfter(2000, TimeUnit.MILLISECONDS)
            .getActorsByStatus(
                GetActorsByStatusRequest.newBuilder()
                    .addAllStatus(List.of(STATUS_ALWAYS_DENIED, STATUS_SUSPENDED))
                    .build()))
        .thenReturn(generateSampleActorResponse());

    List<BlockingDetails> violations = actorBasedDataFetcher.getRateLimitBasedIpViolation();
    assertEquals(1, violations.size());
    assertEquals(
        List.of("1.1.1.1", "2.2.2.2"), violations.get(0).getIpDetails().getIpAddressesList());
    assertEquals(BLOCKING_CATEGORY_RATE_LIMIT, violations.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
            "entity-1", "rate-limit-id-1", "rate-limit-name-1"),
        violations.get(0).getInfo());
  }

  private static GetActorsByStatusResponse generateSampleActorResponse() {
    return GetActorsByStatusResponse.newBuilder()
        .addActors(
            Actor.newBuilder()
                .setActorId("actor-1")
                .setEntityId("entity-1")
                .addAllIpAddresses(List.of("1.1.1.1", "2.2.2.2"))
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
                .build())
        .addActors(
            Actor.newBuilder()
                .setActorId("actor-2")
                .setEntityId("entity-2")
                .addAllIpAddresses(List.of("1.1.1.1", "2.2.2.2"))
                .setStatusChangeSource(STATUS_CHANGE_SOURCE_SYSTEM)
                .setStatusExpiryTimestamp(activeTimestamp)
                .build())
        .addActors(
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
                .build())
        .addActors(
            Actor.newBuilder()
                .setActorId("actor-4")
                .setEntityId("entity-4")
                .addAllIpAddresses(List.of("1.1.1.1", "2.2.2.2"))
                .setStatusChangeSource(STATUS_CHANGE_SOURCE_SYSTEM)
                .setStatusExpiryTimestamp(inactiveTimestamp)
                .build())
        .addActors(
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
                .build())
        .addActors(
            Actor.newBuilder()
                .setActorId("actor-6")
                .setEntityId("entity-6")
                .setStatusChangeSource(STATUS_CHANGE_SOURCE_SYSTEM)
                .setStatusExpiryTimestamp(activeTimestamp)
                .build())
        .build();
  }
}
