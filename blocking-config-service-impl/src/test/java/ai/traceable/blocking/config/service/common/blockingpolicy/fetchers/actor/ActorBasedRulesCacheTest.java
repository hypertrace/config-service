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

import ai.traceable.blocking.config.service.BlockingDataCacheConfig;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Status;
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

    ActorBasedRulesCollection response =
        actorBasedRulesCache.getActorBasedRules(
            REQUEST_CONTEXT.buildInternalContextualKey(Optional.empty()));
    {
      List<BlockingPolicyData> violations = response.getThreatActorBasedIpViolations();
      assertEquals(1, violations.size());
      assertEquals(List.of("2.2.2.2"), violations.get(0).getIpAddresses());
      assertEquals(Category.THREAT_ACTOR, violations.get(0).getCategory());
      assertEquals("actor-2", violations.get(0).getUserId());
      assertEquals(RuleType.BLOCK, violations.get(0).getRuleType());
      assertEquals(Status.DENIED, violations.get(0).getStatus());
      assertEquals(
          ViolationInfoEncoder.getEncodedThreatActorViolationInfo("entity-2"),
          violations.get(0).getInfo());
    }

    {
      List<BlockingPolicyData> exemptions = response.getThreatActorBasedIpExemptions();
      assertEquals(1, exemptions.size());
      assertEquals(List.of("3.3.3.3"), exemptions.get(0).getIpAddresses());
      assertEquals("actor-3", exemptions.get(0).getUserId());
      assertEquals(Category.THREAT_ACTOR, exemptions.get(0).getCategory());
      assertEquals(RuleType.ALLOW, exemptions.get(0).getRuleType());
      assertEquals(Status.SNOOZED, exemptions.get(0).getStatus());
      assertEquals(
          ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo("entity-3"),
          exemptions.get(0).getInfo());
    }

    {
      List<BlockingPolicyData> rateLimitBasedIpViolation = response.getRateLimitBasedIpViolations();
      assertEquals(2, rateLimitBasedIpViolation.size());
      assertEquals(List.of("1.1.1.1"), rateLimitBasedIpViolation.get(0).getIpAddresses());
      assertEquals("actor-1", rateLimitBasedIpViolation.get(0).getUserId());
      assertEquals(Category.RATE_LIMIT, rateLimitBasedIpViolation.get(0).getCategory());
      assertEquals(RuleType.BLOCK, rateLimitBasedIpViolation.get(0).getRuleType());
      assertEquals(Status.SUSPENDED, rateLimitBasedIpViolation.get(0).getStatus());
      assertEquals(
          ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
              "entity-1",
              "rate-limit-id-1",
              "rate-limit-name-1",
              RATE_LIMIT_CATEGORY_RATE_LIMITING),
          rateLimitBasedIpViolation.get(0).getInfo());

      assertEquals("actor-7", rateLimitBasedIpViolation.get(1).getUserId());
      assertEquals(Category.DATA_EXFILTRATION, rateLimitBasedIpViolation.get(1).getCategory());
    }
    {
      List<BlockingPolicyData> emailDomainBasedExemptions =
          response.getEmailDomainBasedExemptions();
      assertEquals(1, emailDomainBasedExemptions.size());
      assertEquals(List.of("5.5.5.5"), emailDomainBasedExemptions.get(0).getIpAddresses());
      assertEquals("actor-5", emailDomainBasedExemptions.get(0).getUserId());
      assertEquals(Category.EMAIL_DOMAIN_RULE, emailDomainBasedExemptions.get(0).getCategory());
      assertEquals(RuleType.ALLOW, emailDomainBasedExemptions.get(0).getRuleType());
      assertEquals(Status.ALLOWED, emailDomainBasedExemptions.get(0).getStatus());
      assertEquals(
          ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
              "email-domain-id-1",
              "email-domain-name-1",
              "",
              Optional.of("entity-5"),
              List.of(MaliciousSourcesRuleCondition.ConditionCase.EMAIL_DOMAIN_CONDITION)),
          emailDomainBasedExemptions.get(0).getInfo());
    }

    {
      List<BlockingPolicyData> emailDomainBasedViolations =
          response.getEmailDomainBasedViolations();
      assertEquals(1, emailDomainBasedViolations.size());
      assertEquals(List.of("6.6.6.6"), emailDomainBasedViolations.get(0).getIpAddresses());
      assertEquals("actor-6", emailDomainBasedViolations.get(0).getUserId());
      assertEquals(Category.EMAIL_DOMAIN_RULE, emailDomainBasedViolations.get(0).getCategory());
      assertEquals(RuleType.BLOCK, emailDomainBasedViolations.get(0).getRuleType());
      assertEquals(Status.SUSPENDED, emailDomainBasedViolations.get(0).getStatus());
      assertEquals(
          ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
              "email-domain-id-2",
              "email-domain-name-2",
              "",
              Optional.of("entity-6"),
              List.of(MaliciousSourcesRuleCondition.ConditionCase.EMAIL_DOMAIN_CONDITION)),
          emailDomainBasedViolations.get(0).getInfo());
    }
  }

  @Test
  void testGetActorBasedRulesWithEnvironment() {
    // Request has an environment filter
    doReturn(generateSampleResponse())
        .when(actorStore)
        .getActiveThreatActors(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    ActorBasedRulesCollection response =
        actorBasedRulesCache.getActorBasedRules(
            REQUEST_CONTEXT.buildInternalContextualKey(Optional.of(ENVIRONMENT_ID)));

    assertEquals(1, response.getThreatActorBasedIpViolations().size());
    assertEquals(1, response.getThreatActorBasedIpExemptions().size());
    assertEquals(2, response.getRateLimitBasedIpViolations().size());
    assertEquals(1, response.getEmailDomainBasedExemptions().size());
    assertEquals(1, response.getEmailDomainBasedViolations().size());
  }

  private static List<ActorStatusDetails> generateSampleResponse() {
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
            ACTIVE_TIMESTAMP),
        new ActorStatusDetails(
            "actor-2",
            "entity-2",
            List.of("192.168.65.3", "2.2.2.2"),
            STATUS_ALWAYS_DENIED,
            STATUS_CHANGE_SOURCE_SYSTEM,
            StatusChangeDetails.newBuilder().setStatusChangeReason("Test 2").build(),
            0L),
        new ActorStatusDetails(
            "actor-3",
            "entity-3",
            List.of("3.3.3.3"),
            STATUS_SNOOZED,
            STATUS_CHANGE_SOURCE_SYSTEM,
            StatusChangeDetails.newBuilder().setStatusChangeReason("Test 3").build(),
            ACTIVE_TIMESTAMP),
        new ActorStatusDetails(
            "actor-4",
            "entity-4",
            List.of("192.168.65.3"),
            STATUS_ALWAYS_ALLOWED,
            STATUS_CHANGE_SOURCE_SYSTEM,
            StatusChangeDetails.newBuilder().setStatusChangeReason("Test 4").build(),
            0L),
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
            0L),
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
            ACTIVE_TIMESTAMP),
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
            ACTIVE_TIMESTAMP));
  }
}
