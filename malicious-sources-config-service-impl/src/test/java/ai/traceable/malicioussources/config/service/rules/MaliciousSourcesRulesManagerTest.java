package ai.traceable.malicioussources.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.EnvironmentScope;
import ai.traceable.malicioussources.config.service.v1.EventSeverity;
import ai.traceable.malicioussources.config.service.v1.ExpirationDetails;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.IpReputationCondition;
import ai.traceable.malicioussources.config.service.v1.IpReputationSeverity;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleConditions;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.RegionCondition;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import com.google.protobuf.Timestamp;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class MaliciousSourcesRulesManagerTest {
  private static final MaliciousSourcesRuleScope ruleScope =
      MaliciousSourcesRuleScope.newBuilder()
          .setEnvironmentScope(
              EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("env1", "env2")))
          .build();
  private MockGenericConfigService mockConfigService;
  private UuidGenerator uuidGenerator;
  private MaliciousSourcesRulesManager rulesManager;
  private RequestContext requestContext;
  private MaliciousSourcesRulesStore maliciousSourcesRulesStore;

  @BeforeEach
  void setUp() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    uuidGenerator = mock(UuidGenerator.class);

    maliciousSourcesRulesStore =
        new MaliciousSourcesRulesStore(
            configServiceBlockingStub, mock(ConfigChangeEventGenerator.class));
    this.rulesManager = new MaliciousSourcesRulesManager(maliciousSourcesRulesStore, uuidGenerator);
    requestContext = RequestContext.forTenantId("default tenant");
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Nested
  class createMaliciousSourcesRule {
    @Test
    @DisplayName("should be able to create an Malicious Source Rule if given valid arguments")
    void shouldCreateMaliciousRule() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALERT)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  com.google.protobuf.Duration.newBuilder().setSeconds(10).build())
                              .setExpirationTimestampMillis(
                                  Timestamp.newBuilder().setSeconds(20).build())
                              .build())
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .setConditions(
                  MaliciousSourcesRuleConditions.newBuilder()
                      .setRegionCondition(RegionCondition.newBuilder().addRegions("China").build())
                      .setIpLocationTypeCondition(
                          IpLocationTypeCondition.newBuilder()
                              .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN)
                              .build())
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder()
                              .setMinIpReputationSeverity(
                                  IpReputationSeverity.IP_REPUTATION_SEVERITY_LOW)
                              .setMinIpReputationScore(1)
                              .build())
                      .build())
              .build();

      when(uuidGenerator.generateId(anyString())).thenReturn("First-test");
      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleScope(ruleScope)
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();
      MaliciousSourcesRule maybeCreatedMaliciousSourcesRule =
          rulesManager.createMaliciousSourcesRule(
              requestContext,
              CreateMaliciousSourcesRuleRequest.newBuilder()
                  .setRuleInfo(maliciousSourcesRuleInfo)
                  .setRuleScope(ruleScope)
                  .build());

      assertNotNull(maybeCreatedMaliciousSourcesRule);
      assertEquals(maliciousSourcesRule, maybeCreatedMaliciousSourcesRule);
    }
  }
}
