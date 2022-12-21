package ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_ALLOWED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_SNOOZED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_SUSPENDED;
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
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
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
      List<BlockingDetails> violations = response.getThreatActorBasedIpViolations();
      assertEquals(2, violations.size());
      assertEquals(List.of("2.2.2.2"), violations.get(0).getActorDetails().getIpAddressesList());
      assertEquals(BLOCKING_CATEGORY_THREAT_ACTOR, violations.get(0).getCategory());
      assertEquals("actor-2", violations.get(0).getActorDetails().getUserId());
      assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
      assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
      assertEquals(
          ViolationInfoEncoder.getEncodedThreatActorViolationInfo("entity-2"),
          violations.get(0).getInfo());
      // Addition ip detail rule
      assertEquals(List.of("2.2.2.2"), violations.get(1).getIpDetails().getIpAddressesList());
      assertEquals(BLOCKING_CATEGORY_THREAT_ACTOR, violations.get(1).getCategory());
      assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(1).getBlockingRuleType());
      assertEquals(BLOCKING_STATUS_DENIED, violations.get(1).getStatus());
      assertEquals(
          ViolationInfoEncoder.getEncodedThreatActorViolationInfo("entity-2"),
          violations.get(1).getInfo());
    }

    {
      List<BlockingDetails> exemptions = response.getThreatActorBasedIpExemptions();
      assertEquals(2, exemptions.size());
      assertEquals(List.of("3.3.3.3"), exemptions.get(0).getActorDetails().getIpAddressesList());
      assertEquals("actor-3", exemptions.get(0).getActorDetails().getUserId());
      assertEquals(BLOCKING_CATEGORY_THREAT_ACTOR, exemptions.get(0).getCategory());
      assertEquals(BLOCKING_RULE_TYPE_ALLOW, exemptions.get(0).getBlockingRuleType());
      assertEquals(BLOCKING_STATUS_SNOOZED, exemptions.get(0).getStatus());
      assertEquals(
          ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo("entity-3"),
          exemptions.get(0).getInfo());
      assertEquals(List.of("3.3.3.3"), exemptions.get(1).getIpDetails().getIpAddressesList());
      // Addition ip detail rule
      assertEquals(List.of("3.3.3.3"), exemptions.get(1).getIpDetails().getIpAddressesList());
      assertEquals(BLOCKING_RULE_TYPE_ALLOW, exemptions.get(1).getBlockingRuleType());
    }

    {
      List<BlockingDetails> rateLimitBasedIpViolation = response.getRateLimitBasedIpViolations();
      assertEquals(2, rateLimitBasedIpViolation.size());
      assertEquals(
          List.of("1.1.1.1"),
          rateLimitBasedIpViolation.get(0).getActorDetails().getIpAddressesList());
      assertEquals("actor-1", rateLimitBasedIpViolation.get(0).getActorDetails().getUserId());
      assertEquals(BLOCKING_CATEGORY_RATE_LIMIT, rateLimitBasedIpViolation.get(0).getCategory());
      assertEquals(
          BLOCKING_RULE_TYPE_BLOCK, rateLimitBasedIpViolation.get(0).getBlockingRuleType());
      assertEquals(BLOCKING_STATUS_SUSPENDED, rateLimitBasedIpViolation.get(0).getStatus());
      assertEquals(
          ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
              "entity-1", "rate-limit-id-1", "rate-limit-name-1"),
          rateLimitBasedIpViolation.get(0).getInfo());
      // Addition ip detail rule
      assertEquals(
          List.of("1.1.1.1"), rateLimitBasedIpViolation.get(1).getIpDetails().getIpAddressesList());
      assertEquals(
          BLOCKING_RULE_TYPE_BLOCK, rateLimitBasedIpViolation.get(1).getBlockingRuleType());
      assertEquals(BLOCKING_CATEGORY_RATE_LIMIT, rateLimitBasedIpViolation.get(1).getCategory());
      assertEquals(BLOCKING_STATUS_SUSPENDED, rateLimitBasedIpViolation.get(1).getStatus());
    }
    {
      List<BlockingDetails> emailDomainBasedExemptions = response.getEmailDomainBasedExemptions();
      assertEquals(2, emailDomainBasedExemptions.size());
      assertEquals(
          List.of("5.5.5.5"),
          emailDomainBasedExemptions.get(0).getActorDetails().getIpAddressesList());
      assertEquals("actor-5", emailDomainBasedExemptions.get(0).getActorDetails().getUserId());
      assertEquals(
          BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
          emailDomainBasedExemptions.get(0).getCategory());
      assertEquals(
          BLOCKING_RULE_TYPE_ALLOW, emailDomainBasedExemptions.get(0).getBlockingRuleType());
      assertEquals(BLOCKING_STATUS_ALLOWED, emailDomainBasedExemptions.get(0).getStatus());
      assertEquals(
          ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
              "email-domain-id-1", "email-domain-name-1", "", Optional.of("entity-5")),
          emailDomainBasedExemptions.get(0).getInfo());
      // Addition ip detail rule
      assertEquals(
          List.of("5.5.5.5"),
          emailDomainBasedExemptions.get(1).getIpDetails().getIpAddressesList());
      assertEquals(
          BLOCKING_RULE_TYPE_ALLOW, emailDomainBasedExemptions.get(1).getBlockingRuleType());
      assertEquals(
          BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
          emailDomainBasedExemptions.get(1).getCategory());
      assertEquals(BLOCKING_STATUS_ALLOWED, emailDomainBasedExemptions.get(1).getStatus());
    }

    {
      List<BlockingDetails> emailDomainBasedViolations = response.getEmailDomainBasedViolations();
      assertEquals(2, emailDomainBasedViolations.size());
      assertEquals(
          List.of("6.6.6.6"),
          emailDomainBasedViolations.get(0).getActorDetails().getIpAddressesList());
      assertEquals("actor-6", emailDomainBasedViolations.get(0).getActorDetails().getUserId());
      assertEquals(
          BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
          emailDomainBasedViolations.get(0).getCategory());
      assertEquals(
          BLOCKING_RULE_TYPE_BLOCK, emailDomainBasedViolations.get(0).getBlockingRuleType());
      assertEquals(BLOCKING_STATUS_SUSPENDED, emailDomainBasedViolations.get(0).getStatus());
      assertEquals(
          ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
              "email-domain-id-2", "email-domain-name-2", "", Optional.of("entity-6")),
          emailDomainBasedViolations.get(0).getInfo());
      // Addition ip detail rule
      assertEquals(
          List.of("6.6.6.6"),
          emailDomainBasedViolations.get(1).getIpDetails().getIpAddressesList());
      assertEquals(
          BLOCKING_RULE_TYPE_BLOCK, emailDomainBasedViolations.get(1).getBlockingRuleType());
      assertEquals(
          BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
          emailDomainBasedViolations.get(1).getCategory());
      assertEquals(BLOCKING_STATUS_SUSPENDED, emailDomainBasedViolations.get(1).getStatus());
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

    assertEquals(2, response.getThreatActorBasedIpViolations().size());
    assertEquals(2, response.getThreatActorBasedIpExemptions().size());
    assertEquals(2, response.getRateLimitBasedIpViolations().size());
    assertEquals(2, response.getEmailDomainBasedExemptions().size());
    assertEquals(2, response.getEmailDomainBasedViolations().size());
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
                        .setRuleName("rate-limit-name-1"))
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
            ACTIVE_TIMESTAMP));
  }
}
