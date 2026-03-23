package ai.traceable.iprange.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.iprange.config.service.IpRangeConfigServiceConfig;
import ai.traceable.iprange.config.service.utils.UuidGenerator;
import ai.traceable.iprange.config.service.v1.AgentModification;
import ai.traceable.iprange.config.service.v1.AgentRuleEffect;
import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.EnvironmentScope;
import ai.traceable.iprange.config.service.v1.EventSeverity;
import ai.traceable.iprange.config.service.v1.ExpirationDetails;
import ai.traceable.iprange.config.service.v1.FieldValue;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.HeaderInjection;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.PredicateLocation;
import ai.traceable.iprange.config.service.v1.RuleAction;
import ai.traceable.iprange.config.service.v1.RuleEffectWithModifications;
import ai.traceable.iprange.config.service.v1.RuleScope;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import com.google.protobuf.Value;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class IpRangeRulesManagerTest {

  private static final RuleScope ruleScope =
      RuleScope.newBuilder()
          .setEnvironmentScope(
              EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("env1", "env2")))
          .build();
  private MockGenericConfigService mockConfigService;
  private UuidGenerator uuidGenerator;
  private IpRangeRulesManager rulesManager;
  private RequestContext requestContext;
  private Clock mockClock;
  private IpRangeRulesStore ipRangeRulesStore;

  @BeforeEach
  void setUp() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    uuidGenerator = mock(UuidGenerator.class);
    mockClock = mock(Clock.class);
    ipRangeRulesStore =
        new IpRangeRulesStore(
            configServiceBlockingStub,
            mock(ConfigChangeEventGenerator.class),
            mock(IpRangeConfigServiceConfig.class));
    this.rulesManager = new IpRangeRulesManager(ipRangeRulesStore, uuidGenerator, mockClock);
    requestContext = RequestContext.forTenantId("default tenant");
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Nested
  class getIpRangeRules {
    @Test
    @DisplayName("should fetch all ip range rules for a valid query")
    void shouldGetAllIpRangeRules() {
      IpRangeRuleDetails ipRangeRuleDetails1 =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("4.3.2.1", "16.16.16.16/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT2H2M34S").build())
              .build();

      IpRangeRule ipRangeRule1 =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(ipRangeRuleDetails1)
              .setDisabled(true)
              .setInternal(false)
              .addAllIpRanges(Arrays.asList("16.16.16.16/16"))
              .addAllIpAddresses(Arrays.asList("4.3.2.1"))
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder()
                              .addEnvironmentIds("env1")
                              .addEnvironmentIds("env2")))
              .build();

      IpRangeRuleDetails ipRangeRuleDetails2 =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-2")
              .setDescription("Range rule test 2")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationTimestampMillis(1000).build())
              .build();

      IpRangeRule ipRangeRule2 =
          IpRangeRule.newBuilder()
              .setId("Second-test")
              .setRuleDetails(ipRangeRuleDetails2)
              .setDisabled(false)
              .setInternal(true)
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .addAllIpAddresses(Arrays.asList("1.2.3.4"))
              .build();

      addIpRangeRule(ipRangeRule1);
      addIpRangeRule(ipRangeRule2);
      // No filter used so should return all rules
      List<IpRangeRule> ipRangeRules =
          rulesManager.getIpRangeRules(requestContext, GetRulesFilter.newBuilder().build());
      assertEquals(List.of(ipRangeRule2, ipRangeRule1), ipRangeRules);

      // Filter by id when id is not present
      assertTrue(
          rulesManager
              .getIpRangeRules(
                  requestContext, GetRulesFilter.newBuilder().addRuleIds("An absent id").build())
              .isEmpty());

      // Filter by id and id is present
      List<IpRangeRule> resultIpRules1 =
          rulesManager.getIpRangeRules(
              requestContext, GetRulesFilter.newBuilder().addRuleIds("First-test").build());
      assertEquals(List.of(ipRangeRule1), resultIpRules1);

      // Filter by action when action is not present
      assertTrue(
          rulesManager
              .getIpRangeRules(
                  requestContext,
                  GetRulesFilter.newBuilder().addRuleActions(RuleAction.RULE_ACTION_ALERT).build())
              .isEmpty());

      // Filter by action and action is present
      resultIpRules1 =
          rulesManager.getIpRangeRules(
              requestContext,
              GetRulesFilter.newBuilder().addRuleActions(RuleAction.RULE_ACTION_BLOCK).build());
      assertEquals(List.of(ipRangeRule1), resultIpRules1);

      // Filter by internal and name is present
      List<IpRangeRule> resultIpRules2 =
          rulesManager.getIpRangeRules(
              requestContext, GetRulesFilter.newBuilder().setInternal(true).build());
      assertEquals(List.of(ipRangeRule2), resultIpRules2);

      // Filter by env
      assertEquals(
          List.of(ipRangeRule2, ipRangeRule1),
          rulesManager.getIpRangeRules(
              requestContext,
              GetRulesFilter.newBuilder()
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(
                              EnvironmentScope.newBuilder()
                                  .addEnvironmentIds("env1")
                                  .addEnvironmentIds("env3")))
                  .build()));
      assertEquals(
          List.of(ipRangeRule2),
          rulesManager.getIpRangeRules(
              requestContext,
              GetRulesFilter.newBuilder()
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(
                              EnvironmentScope.newBuilder().addEnvironmentIds("env3")))
                  .build()));

      // Filter by rule scope with env scope with no envs should only return rules with no envs
      assertEquals(
          List.of(ipRangeRule2),
          rulesManager.getIpRangeRules(
              requestContext,
              GetRulesFilter.newBuilder()
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                  .build()));

      // Filter by rule scope with no env scope should all available rules
      assertEquals(
          List.of(ipRangeRule2, ipRangeRule1),
          rulesManager.getIpRangeRules(
              requestContext,
              GetRulesFilter.newBuilder().setRuleScope(RuleScope.getDefaultInstance()).build()));
    }
  }

  @Nested
  class createIpRangeRule {
    @Test
    @DisplayName("should be able to create an Ip Range Rule if given valid arguments")
    void shouldCreateIpRangeRule() {
      RuleEffectWithModifications ruleEffectWithModifications =
          RuleEffectWithModifications.newBuilder()
              .setAgentRuleEffect(
                  AgentRuleEffect.newBuilder()
                      .addAgentModifications(
                          AgentModification.newBuilder()
                              .setHeaderInjection(
                                  HeaderInjection.newBuilder()
                                      .setHeaderLocation(
                                          PredicateLocation.PREDICATE_LOCATION_REQUEST)
                                      .setHeaderName("name")
                                      .setValue(FieldValue.newBuilder().setStaticValue("value")))))
              .build();
      IpRangeRuleDetails ipRangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester-1")
              .setDescription("Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
              .addEffects(ruleEffectWithModifications)
              .build();

      long now = Clock.systemUTC().millis();

      IpRangeRule ipRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(
                  IpRangeRuleDetails.newBuilder(ipRangeRuleDetails)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  ipRangeRuleDetails.getExpirationDetails().getExpirationDuration())
                              .setExpirationTimestampMillis(
                                  Duration.parse(
                                              ipRangeRuleDetails
                                                  .getExpirationDetails()
                                                  .getExpirationDuration())
                                          .toMillis()
                                      + now)
                              .build())
                      .build())
              .addAllIpAddresses(Arrays.asList("1.2.3.4"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .setRuleScope(ruleScope)
              .build();

      when(uuidGenerator.generateId()).thenReturn("First-test");

      when(mockClock.millis()).thenReturn(now);

      IpRangeRule maybeCreatedIpRangeRule =
          rulesManager.createIpRangeRule(
              requestContext,
              CreateIpRangeRuleRequest.newBuilder()
                  .setRuleDetails(ipRangeRuleDetails)
                  .setRuleScope(ruleScope)
                  .build());

      assertNotNull(maybeCreatedIpRangeRule);
      assertEquals(ipRangeRule, maybeCreatedIpRangeRule);
    }
  }

  @Nested
  class updateIpRangeRule {
    @Test
    @DisplayName("Should fail when Ip Range Rule to be updated is not present")
    void should_notUpdateIpRangeRule_ifNotPresent() {
      IpRangeRuleDetails updatedRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Updated-Tester-1")
              .setDescription("Updated-Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("11.12.13.14", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();
      UpdateIpRangeRuleRequest rangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("First-test")
              .setRuleDetails(updatedRuleDetails)
              .setDisabled(true)
              .setRuleScope(ruleScope)
              .build();
      assertThrows(
          NoSuchElementException.class,
          () -> rulesManager.updateIpRangeRule(requestContext, rangeRuleRequest));
    }

    @Test
    @DisplayName("should be able to update an Ip Range Rule if given valid arguments")
    void shouldUpdateIpRangeRule() {
      RuleEffectWithModifications ruleEffectWithModifications =
          RuleEffectWithModifications.newBuilder()
              .setAgentRuleEffect(
                  AgentRuleEffect.newBuilder()
                      .addAgentModifications(
                          AgentModification.newBuilder()
                              .setHeaderInjection(
                                  HeaderInjection.newBuilder()
                                      .setHeaderLocation(
                                          PredicateLocation.PREDICATE_LOCATION_REQUEST)
                                      .setHeaderName("name")
                                      .setValue(FieldValue.newBuilder().setStaticValue("value")))))
              .build();
      IpRangeRuleDetails updatedRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Updated-Tester-1")
              .setDescription("Updated-Range rule test 1")
              .addAllRawInputIpData(Arrays.asList("11.12.13.14", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
              .addEffects(ruleEffectWithModifications)
              .build();
      long now = Clock.systemUTC().millis();

      IpRangeRule updatedIpRangeRule =
          IpRangeRule.newBuilder()
              .setId("First-test")
              .setRuleDetails(
                  IpRangeRuleDetails.newBuilder(updatedRuleDetails)
                      .setExpirationDetails(
                          ExpirationDetails.newBuilder()
                              .setExpirationDuration(
                                  updatedRuleDetails.getExpirationDetails().getExpirationDuration())
                              .setExpirationTimestampMillis(
                                  Duration.parse(
                                              updatedRuleDetails
                                                  .getExpirationDetails()
                                                  .getExpirationDuration())
                                          .toMillis()
                                      + now)
                              .build())
                      .build())
              .setDisabled(true)
              .addAllIpAddresses(Arrays.asList("11.12.13.14"))
              .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
              .setRuleScope(ruleScope)
              .build();

      addIpRangeRule(IpRangeRule.getDefaultInstance());

      when(mockClock.millis()).thenReturn(now);

      IpRangeRule maybeUpdatedIpRangeRule =
          rulesManager.updateIpRangeRule(
              requestContext,
              UpdateIpRangeRuleRequest.newBuilder()
                  .setId("First-test")
                  .setRuleDetails(updatedRuleDetails)
                  .setDisabled(true)
                  .setRuleScope(ruleScope)
                  .build());

      assertNotNull(maybeUpdatedIpRangeRule);
      assertEquals(updatedIpRangeRule, maybeUpdatedIpRangeRule);
    }
  }

  @Nested
  class deleteIpRangeRule {
    @Test
    @DisplayName("should be able to delete an Ip Range Rule")
    void shouldDeleteIpRangeRule() {
      addIpRangeRule(IpRangeRule.newBuilder().setId("id-1").build());
      // Deleting an entity which exists
      assertDoesNotThrow(() -> rulesManager.deleteIpRangeRule(requestContext, "id-1"));
    }
  }

  @Test
  @DisplayName("AAP-11627: config change events generated for create and update")
  void testConfigChangeEventsGeneratedForCreateAndUpdate_AAP11627() {
    // Regression test for AAP-11627: Verify that config change events are generated
    // for create and update operations.
    MockGenericConfigService localMockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    localMockConfigService.start();
    try {
      ConfigChangeEventGenerator spyEventGenerator = mock(ConfigChangeEventGenerator.class);
      ConfigServiceGrpc.ConfigServiceBlockingStub stub =
          ConfigServiceGrpc.newBlockingStub(localMockConfigService.channel());
      IpRangeRulesStore localStore =
          new IpRangeRulesStore(stub, spyEventGenerator, mock(IpRangeConfigServiceConfig.class));
      UuidGenerator localUuidGenerator = mock(UuidGenerator.class);
      when(localUuidGenerator.generateId()).thenReturn("event-test-id");
      Clock localClock = mock(Clock.class);
      long now = Clock.systemUTC().millis();
      when(localClock.millis()).thenReturn(now);
      IpRangeRulesManager localManager =
          new IpRangeRulesManager(localStore, localUuidGenerator, localClock);
      RequestContext testContext = RequestContext.forTenantId("event-test-tenant");

      IpRangeRuleDetails ruleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("event-test-rule")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .build();
      localManager.createIpRangeRule(
          testContext,
          CreateIpRangeRuleRequest.newBuilder()
              .setRuleDetails(ruleDetails)
              .setRuleScope(ruleScope)
              .build());
      verify(spyEventGenerator, atLeastOnce())
          .sendCreateNotification(
              any(RequestContext.class), any(String.class), any(String.class), any(Value.class));

      IpRangeRuleDetails updateDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("event-test-rule-updated")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .build();
      localManager.updateIpRangeRule(
          testContext,
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("event-test-id")
              .setRuleDetails(updateDetails)
              .setRuleScope(ruleScope)
              .build());
      verify(spyEventGenerator, atLeastOnce())
          .sendUpdateNotification(
              any(RequestContext.class),
              any(String.class),
              any(String.class),
              any(Value.class),
              any(Value.class));
    } finally {
      localMockConfigService.shutdown();
    }
  }

  private void addIpRangeRule(IpRangeRule ipRangeRule) {
    this.ipRangeRulesStore.upsertObject(requestContext, ipRangeRule);
  }
}
