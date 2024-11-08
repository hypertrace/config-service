package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor;

import static ai.traceable.platform.actor.v1.RateLimitCategory.RATE_LIMIT_CATEGORY_DATA_EXFILTRATION;
import static ai.traceable.platform.actor.v1.RateLimitCategory.RATE_LIMIT_CATEGORY_RATE_LIMITING;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_ALLOWED;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_DENIED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SNOOZED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SUSPENDED;
import static ai.traceable.platform.actor.v1.StatusChangeSource.STATUS_CHANGE_SOURCE_MALICIOUS_SOURCES;
import static ai.traceable.platform.actor.v1.StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT;
import static ai.traceable.platform.actor.v1.StatusChangeSource.STATUS_CHANGE_SOURCE_SYSTEM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.BlockingDataCacheConfig;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.ActorBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Status;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.platform.actor.v1.MaliciousSourcesDetails;
import ai.traceable.platform.actor.v1.RateLimitDetails;
import ai.traceable.platform.actor.v1.StatusChangeDetails;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.typesafe.config.ConfigFactory;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorBasedRulesCacheTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final Long ACTIVE_TIMESTAMP = 10000L;

  private final BlockingRulesUtils blockingRulesUtils = new BlockingRulesUtils(Clock.systemUTC());

  private ActorBasedRulesCache actorBasedRulesCache;
  private ActorStore actorStore;

  @BeforeEach
  void setUp() {
    actorStore = mock(ActorStore.class);

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
        new ActorBasedRulesCache(actorServiceConfig, blockingRulesUtils, actorStore);
  }

  @Test
  void testGetActorBasedRulesWithoutEnvironment() {
    // Request does not have environment filter
    doReturn(generateSampleResponse())
        .when(actorStore)
        .getActiveThreatActors(REQUEST_CONTEXT, Optional.empty());

    List<BlockingPolicyData> response =
        actorBasedRulesCache.getActorBasedRules(
            REQUEST_CONTEXT.buildInternalContextualKey(Optional.empty()));

    assertEquals(6, response.size());

    assertEquals(
        ActorBlockingDetails.builder().ipAddresses(List.of("1.1.1.1")).userId("actor-1").build(),
        response.get(0).getBlockingDetails());
    assertEquals(Category.RATE_LIMIT, response.get(0).getCategory());
    assertEquals(RuleType.BLOCK, response.get(0).getRuleType());
    assertEquals(Status.SUSPENDED, response.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
            "entity-1",
            "rate-limit-id-1",
            "rate-limit-name-1",
            RATE_LIMIT_CATEGORY_RATE_LIMITING,
            Map.of("key", "value")),
        response.get(0).getInfo());
    assertEquals("actor-1", response.get(0).getRuleId());

    assertEquals(
        ActorBlockingDetails.builder().ipAddresses(List.of("2.2.2.2")).userId("actor-2").build(),
        response.get(1).getBlockingDetails());
    assertEquals(Category.THREAT_ACTOR, response.get(1).getCategory());
    assertEquals(RuleType.BLOCK, response.get(1).getRuleType());
    assertEquals(Status.DENIED, response.get(1).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedThreatActorViolationInfo("entity-2"),
        response.get(1).getInfo());
    assertEquals("actor-2", response.get(1).getRuleId());

    assertEquals(
        ActorBlockingDetails.builder().ipAddresses(List.of("3.3.3.3")).userId("actor-3").build(),
        response.get(2).getBlockingDetails());
    assertEquals(Category.THREAT_ACTOR, response.get(2).getCategory());
    assertEquals(RuleType.ALLOW, response.get(2).getRuleType());
    assertEquals(Status.SNOOZED, response.get(2).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo("entity-3"),
        response.get(2).getInfo());
    assertEquals("actor-3", response.get(2).getRuleId());

    assertEquals(
        ActorBlockingDetails.builder().ipAddresses(List.of("5.5.5.5")).userId("actor-5").build(),
        response.get(3).getBlockingDetails());
    assertEquals(Category.EMAIL_DOMAIN_RULE, response.get(3).getCategory());
    assertEquals(RuleType.ALLOW, response.get(3).getRuleType());
    assertEquals(Status.ALLOWED, response.get(3).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            "email-domain-id-1",
            "email-domain-name-1",
            "",
            Optional.of("entity-5"),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.EMAIL_DOMAIN_CONDITION)),
        response.get(3).getInfo());
    assertEquals("actor-5", response.get(3).getRuleId());

    assertEquals(
        ActorBlockingDetails.builder().ipAddresses(List.of("6.6.6.6")).userId("actor-6").build(),
        response.get(4).getBlockingDetails());
    assertEquals(Category.EMAIL_DOMAIN_RULE, response.get(4).getCategory());
    assertEquals(RuleType.BLOCK, response.get(4).getRuleType());
    assertEquals(Status.SUSPENDED, response.get(4).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "email-domain-id-2",
            "email-domain-name-2",
            "",
            Optional.of("entity-6"),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.EMAIL_DOMAIN_CONDITION)),
        response.get(4).getInfo());
    assertEquals("actor-6", response.get(4).getRuleId());

    assertEquals(
        ActorBlockingDetails.builder().ipAddresses(List.of("7.7.7.7")).userId("actor-7").build(),
        response.get(5).getBlockingDetails());
    assertEquals(Category.DATA_EXFILTRATION, response.get(5).getCategory());
  }

  @Test
  void testGetActorBasedRulesWithEnvironment() {
    // Request has an environment filter
    doReturn(generateSampleResponse())
        .when(actorStore)
        .getActiveThreatActors(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    List<BlockingPolicyData> response =
        actorBasedRulesCache.getActorBasedRules(
            REQUEST_CONTEXT.buildInternalContextualKey(Optional.of(ENVIRONMENT_ID)));
    assertEquals(6, response.size());
  }

  private static List<ActorStatusDetails> generateSampleResponse() {
    Map<String, String> blockedLabelsMap = Map.of("key", "value");
    return List.of(
        new ActorStatusDetails(
            "actor-1",
            "entity-1",
            List.of("1.1.1.1", "192.168.65.3"),
            STATUS_SUSPENDED,
            STATUS_CHANGE_SOURCE_RATE_LIMIT,
            StatusChangeDetails.newBuilder()
                .setRateLimitDetails(
                    RateLimitDetails.newBuilder()
                        .setRuleId("rate-limit-id-1")
                        .setRuleName("rate-limit-name-1")
                        .setRuleCategory(RATE_LIMIT_CATEGORY_RATE_LIMITING))
                .build(),
            ACTIVE_TIMESTAMP,
            blockedLabelsMap),
        new ActorStatusDetails(
            "actor-2",
            "entity-2",
            List.of("192.168.65.3", "2.2.2.2"),
            STATUS_ALWAYS_DENIED,
            STATUS_CHANGE_SOURCE_SYSTEM,
            StatusChangeDetails.newBuilder().setStatusChangeReason("Test 2").build(),
            0L,
            blockedLabelsMap),
        new ActorStatusDetails(
            "actor-3",
            "entity-3",
            List.of("3.3.3.3"),
            STATUS_SNOOZED,
            STATUS_CHANGE_SOURCE_SYSTEM,
            StatusChangeDetails.newBuilder().setStatusChangeReason("Test 3").build(),
            ACTIVE_TIMESTAMP,
            blockedLabelsMap),
        new ActorStatusDetails(
            "actor-4",
            "entity-4",
            List.of("192.168.65.3"),
            STATUS_ALWAYS_ALLOWED,
            STATUS_CHANGE_SOURCE_SYSTEM,
            StatusChangeDetails.newBuilder().setStatusChangeReason("Test 4").build(),
            0L,
            blockedLabelsMap),
        new ActorStatusDetails(
            "actor-5",
            "entity-5",
            List.of("5.5.5.5", "192.168.65.3"),
            STATUS_ALWAYS_ALLOWED,
            STATUS_CHANGE_SOURCE_MALICIOUS_SOURCES,
            StatusChangeDetails.newBuilder()
                .setMaliciousSourcesDetails(
                    MaliciousSourcesDetails.newBuilder()
                        .setRuleId("email-domain-id-1")
                        .setRuleName("email-domain-name-1"))
                .build(),
            0L,
            blockedLabelsMap),
        new ActorStatusDetails(
            "actor-6",
            "entity-6",
            List.of("6.6.6.6", "192.168.65.3"),
            STATUS_ALWAYS_DENIED,
            STATUS_CHANGE_SOURCE_MALICIOUS_SOURCES,
            StatusChangeDetails.newBuilder()
                .setMaliciousSourcesDetails(
                    MaliciousSourcesDetails.newBuilder()
                        .setRuleId("email-domain-id-2")
                        .setRuleName("email-domain-name-2"))
                .build(),
            ACTIVE_TIMESTAMP,
            blockedLabelsMap),
        new ActorStatusDetails(
            "actor-7",
            "entity-7",
            List.of("7.7.7.7"),
            STATUS_ALWAYS_DENIED,
            STATUS_CHANGE_SOURCE_RATE_LIMIT,
            StatusChangeDetails.newBuilder()
                .setRateLimitDetails(
                    RateLimitDetails.newBuilder()
                        .setRuleId("rate-limit-id-7")
                        .setRuleName("rate-limit-name-7")
                        .setRuleCategory(RATE_LIMIT_CATEGORY_DATA_EXFILTRATION))
                .build(),
            ACTIVE_TIMESTAMP,
            blockedLabelsMap));
  }
}
