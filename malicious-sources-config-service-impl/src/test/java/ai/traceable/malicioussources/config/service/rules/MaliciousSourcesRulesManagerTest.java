package ai.traceable.malicioussources.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.EnvironmentScope;
import ai.traceable.malicioussources.config.service.v1.EventSeverity;
import ai.traceable.malicioussources.config.service.v1.ExpirationDetails;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.IpReputationCondition;
import ai.traceable.malicioussources.config.service.v1.IpReputationSeverity;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleConditions;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleStatus;
import ai.traceable.malicioussources.config.service.v1.Region;
import ai.traceable.malicioussources.config.service.v1.RegionCondition;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
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
  class getMaliciousSourcesRules {
    @Test
    @DisplayName("should fetch all Malicious Sources rules for a valid query")
    void shouldGetAllMaliciousSourcesRules() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo1 =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test 1")
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
              .build();

      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo2 =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-2")
              .setDescription("Malicious Sources Rule Test 2")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  com.google.protobuf.Duration.newBuilder().setSeconds(20).build())
                              .build())
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule1 =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleScope(
                  MaliciousSourcesRuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder()
                              .addEnvironmentIds("env1")
                              .addEnvironmentIds("env2")))
              .setRuleInfo(maliciousSourcesRuleInfo1)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setDisabled(true)
                      .setInternal(false)
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule2 =
          MaliciousSourcesRule.newBuilder()
              .setId("Second-test")
              .setRuleInfo(maliciousSourcesRuleInfo2)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setDisabled(false)
                      .setInternal(true)
                      .build())
              .build();

      addMaliciousSourcesRule(maliciousSourcesRule1);
      addMaliciousSourcesRule(maliciousSourcesRule2);

      // No filter used so should return all rules
      List<MaliciousSourcesRule> maliciousSourcesRules =
          rulesManager.getMaliciousSourcesRules(
              requestContext, GetRulesFilter.newBuilder().build());
      assertEquals(List.of(maliciousSourcesRule2, maliciousSourcesRule1), maliciousSourcesRules);

      // Filter by id when id is not present
      assertTrue(
          rulesManager
              .getMaliciousSourcesRules(
                  requestContext, GetRulesFilter.newBuilder().addRuleIds("An absent id").build())
              .isEmpty());

      // Filter by id and id is present
      List<MaliciousSourcesRule> resultMaliciousSourcesRules1 =
          rulesManager.getMaliciousSourcesRules(
              requestContext, GetRulesFilter.newBuilder().addRuleIds("First-test").build());
      assertEquals(List.of(maliciousSourcesRule1), resultMaliciousSourcesRules1);

      // Filter by env
      assertEquals(
          List.of(maliciousSourcesRule2, maliciousSourcesRule1),
          rulesManager.getMaliciousSourcesRules(
              requestContext,
              GetRulesFilter.newBuilder()
                  .setRuleScope(
                      MaliciousSourcesRuleScope.newBuilder()
                          .setEnvironmentScope(
                              EnvironmentScope.newBuilder()
                                  .addEnvironmentIds("env1")
                                  .addEnvironmentIds("env3")))
                  .build()));
      assertEquals(
          List.of(maliciousSourcesRule2),
          rulesManager.getMaliciousSourcesRules(
              requestContext,
              GetRulesFilter.newBuilder()
                  .setRuleScope(
                      MaliciousSourcesRuleScope.newBuilder()
                          .setEnvironmentScope(
                              EnvironmentScope.newBuilder().addEnvironmentIds("env3")))
                  .build()));

      // Filter by rule scope with env scope with no envs should only return rules with no envs
      assertEquals(
          List.of(maliciousSourcesRule2),
          rulesManager.getMaliciousSourcesRules(
              requestContext,
              GetRulesFilter.newBuilder()
                  .setRuleScope(
                      MaliciousSourcesRuleScope.newBuilder()
                          .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                  .build()));

      // Filter by rule scope with no env scope should all available rules
      assertEquals(
          List.of(maliciousSourcesRule2, maliciousSourcesRule1),
          rulesManager.getMaliciousSourcesRules(
              requestContext,
              GetRulesFilter.newBuilder()
                  .setRuleScope(MaliciousSourcesRuleScope.getDefaultInstance())
                  .build()));
    }
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
                      .setRegionCondition(
                          RegionCondition.newBuilder()
                              .addRegions(Region.newBuilder().setCountryIsoCode("0000").build())
                              .build())
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

  @Nested
  class updateMaliciousSourcesRule {
    @Test
    @DisplayName("Should fail when Malicious Sources Rule to be updated is not present")
    void should_notUpdateMaliciousSourcesRule_ifNotPresent() {
      MaliciousSourcesRuleInfo updatedRuleDetails =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Updated-Tester-1")
              .setDescription("Updated-Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
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
                      .setRegionCondition(
                          RegionCondition.newBuilder()
                              .addRegions(Region.newBuilder().setCountryIsoCode("0000").build())
                              .build())
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
      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleScope(ruleScope)
              .setRuleInfo(updatedRuleDetails)
              .build();

      assertThrows(
          StatusRuntimeException.class,
          () ->
              rulesManager.updateMaliciousSourcesRule(
                  requestContext,
                  UpdateMaliciousSourcesRuleRequest.newBuilder()
                      .setRule(maliciousSourcesRule)
                      .build()));
    }

    @Test
    @DisplayName("should be able to update an Malicious Sources Rule if given valid arguments")
    void shouldUpdateMaliciousSourcesRule() {
      MaliciousSourcesRuleInfo updatedRuleDetails =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Updated-Tester-1")
              .setDescription("Updated-Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
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
                      .setRegionCondition(
                          RegionCondition.newBuilder()
                              .addRegions(Region.newBuilder().setCountryIsoCode("0000").build())
                              .build())
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
      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleScope(ruleScope)
              .setRuleInfo(updatedRuleDetails)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(true)
                      .setInternal(false)
                      .build())
              .build();

      addMaliciousSourcesRule(MaliciousSourcesRule.getDefaultInstance());

      MaliciousSourcesRule maybeUpdatedMaliciousStatusRule =
          rulesManager.updateMaliciousSourcesRule(
              requestContext,
              UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build());

      assertNotNull(maybeUpdatedMaliciousStatusRule);
      assertEquals(maliciousSourcesRule, maybeUpdatedMaliciousStatusRule);
    }
  }

  @Nested
  class deleteMaliciousSourcesRule {
    @Test
    @DisplayName("should be able to delete an Malicious Sources Rule")
    void shouldDeleteMaliciousSourcesRule() {
      addMaliciousSourcesRule(MaliciousSourcesRule.newBuilder().setId("id-1").build());
      // Deleting an entity which exists
      assertDoesNotThrow(() -> rulesManager.deleteMaliciousSourcesRule(requestContext, "id-1"));
    }
  }

  private void addMaliciousSourcesRule(MaliciousSourcesRule maliciousSourcesRule) {
    this.maliciousSourcesRulesStore.upsertObject(requestContext, maliciousSourcesRule);
  }
}
