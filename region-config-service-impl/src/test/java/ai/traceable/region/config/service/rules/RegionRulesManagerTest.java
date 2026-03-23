package ai.traceable.region.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.region.config.service.v1.AgentModification;
import ai.traceable.region.config.service.v1.AgentRuleEffect;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.EventSeverity;
import ai.traceable.region.config.service.v1.FieldValue;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.HeaderInjection;
import ai.traceable.region.config.service.v1.IpReputationCondition;
import ai.traceable.region.config.service.v1.IpReputationSeverity;
import ai.traceable.region.config.service.v1.PredicateLocation;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.RegionRuleConditions;
import ai.traceable.region.config.service.v1.RuleEffectWithModifications;
import ai.traceable.region.config.service.v1.RuleScope;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.Value;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RegionRulesManagerTest {
  private static final RuleScope ruleScope =
      RuleScope.newBuilder()
          .setEnvironmentScope(
              EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("env1", "env2")))
          .build();
  private final Clock clock = Clock.systemUTC();

  private MockGenericConfigService mockConfigService;
  private UuidGenerator uuidGenerator;
  private RequestContext requestContext;
  private RegionRulesStore regionRulesStore;
  private RegionRulesManager rulesManager;

  @BeforeEach
  void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    uuidGenerator = mock(UuidGenerator.class);
    requestContext = RequestContext.forTenantId("default tenant");
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    regionRulesStore =
        new RegionRulesStore(
            configServiceBlockingStub,
            configChangeEventGenerator,
            mock(ai.traceable.region.config.service.RegionConfigServiceConfig.class));
    this.rulesManager = new RegionRulesManager(clock, regionRulesStore, uuidGenerator);
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Nested
  class GetAllRegionRules {
    @Test
    void shouldGetAllRegionRules() {
      RegionRule mockRegionRule1 =
          RegionRule.newBuilder()
              .setRuleScope(ruleScope)
              .setId("id-1")
              .setName("name-1")
              .setDescription("desc-1")
              .setInternal(true)
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();
      RegionRule mockRegionRule2 =
          RegionRule.newBuilder()
              .setId("id-2")
              .setName("name-2")
              .setDisabled(true)
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALERT)
              .build();
      addRegionRules(ImmutableSortedMap.of("id-1", mockRegionRule1, "id-2", mockRegionRule2));

      RegionRule regionRule1 =
          RegionRule.newBuilder()
              .setId("id-1")
              .setName("name-1")
              .setRuleScope(ruleScope)
              .setDescription("desc-1")
              .setInternal(true)
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();
      RegionRule regionRule2 =
          RegionRule.newBuilder()
              .setId("id-2")
              .setName("name-2")
              .setDisabled(true)
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALERT)
              .build();
      assertEquals(
          List.of(regionRule2, regionRule1),
          rulesManager.getRegionRules(requestContext, GetRegionRulesFilter.getDefaultInstance()));
      assertEquals(
          List.of(regionRule2, regionRule1),
          rulesManager.getRegionRules(
              requestContext,
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(
                              EnvironmentScope.newBuilder()
                                  .addEnvironmentIds("env1")
                                  .addEnvironmentIds("env3")))
                  .build()));
      assertEquals(
          List.of(regionRule2),
          rulesManager.getRegionRules(
              requestContext,
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(
                              EnvironmentScope.newBuilder().addEnvironmentIds("env3")))
                  .build()));

      // Filter by rule scope with env scope with no envs should only return rules with no envs
      assertEquals(
          List.of(regionRule2),
          rulesManager.getRegionRules(
              requestContext,
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                  .build()));

      // Filter by rule scope with no env scope should all available rules
      assertEquals(
          List.of(regionRule2, regionRule1),
          rulesManager.getRegionRules(
              requestContext,
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(RuleScope.getDefaultInstance())
                  .build()));
      // Filter on rule action type
      assertEquals(
          List.of(regionRule1),
          rulesManager.getRegionRules(
              requestContext,
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(RuleScope.getDefaultInstance())
                  .addRuleActionTypes(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                  .build()));

      // Filter on disabled and internal
      assertEquals(
          List.of(regionRule1),
          rulesManager.getRegionRules(
              requestContext,
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(RuleScope.getDefaultInstance())
                  .setInternal(true)
                  .build()));
      assertEquals(
          List.of(regionRule2),
          rulesManager.getRegionRules(
              requestContext,
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(RuleScope.getDefaultInstance())
                  .setDisabled(true)
                  .build()));
      assertEquals(
          List.of(),
          rulesManager.getRegionRules(
              requestContext,
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(RuleScope.getDefaultInstance())
                  .setDisabled(false)
                  .setInternal(false)
                  .build()));
    }
  }

  @Nested
  class CreateRegionRule {

    @Test
    void shouldCreateRegionRule() {
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
      when(uuidGenerator.generateRandomId()).thenReturn("id-1");
      RegionRuleConditions conditions =
          RegionRuleConditions.newBuilder()
              .setIpReputation(
                  IpReputationCondition.newBuilder()
                      .setMinIpReputationSeverity(IpReputationSeverity.IP_REPUTATION_SEVERITY_HIGH))
              .build();
      RegionRule regionRule =
          RegionRule.newBuilder()
              .setId("id-1")
              .setName("name-1")
              .setDescription("desc-1")
              .setRuleScope(ruleScope)
              .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
              .setConditions(conditions)
              .addEffects(ruleEffectWithModifications)
              .build();
      RegionRule createdRegionRule =
          rulesManager
              .createRegionRule(
                  requestContext,
                  CreateRegionRuleRequest.newBuilder()
                      .setName("name-1")
                      .setDescription("desc-1")
                      .setRuleScope(ruleScope)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .setConditions(conditions)
                      .addEffects(ruleEffectWithModifications)
                      .build())
              .get();
      assertEquals(regionRule, createdRegionRule);
    }
  }

  @Nested
  class UpdateRegionRule {

    @Test
    void shouldUpdateRegionRule() {
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
      RegionRule originalRegionRule =
          RegionRule.newBuilder().setId("id-1").setName("name-1").build();
      RegionRuleConditions conditions =
          RegionRuleConditions.newBuilder()
              .setIpReputation(
                  IpReputationCondition.newBuilder()
                      .setMinIpReputationSeverity(IpReputationSeverity.IP_REPUTATION_SEVERITY_HIGH))
              .build();
      addRegionRules(ImmutableSortedMap.of("id-1", originalRegionRule));
      RegionRule updatedRegionRule =
          RegionRule.newBuilder()
              .setId("id-1")
              .setName("updated-name")
              .setDescription("desc-1")
              .setInternal(true)
              .setRuleScope(ruleScope)
              .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
              .setConditions(conditions)
              .addEffects(ruleEffectWithModifications)
              .build();
      UpdateRegionRuleRequest request =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id-1")
              .setName("updated-name")
              .setDescription("desc-1")
              .setInternal(true)
              .setRuleScope(ruleScope)
              .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
              .setConditions(conditions)
              .addEffects(ruleEffectWithModifications)
              .build();
      assertEquals(updatedRegionRule, rulesManager.updateRegionRule(requestContext, request).get());
    }

    @Test
    void shouldReturnEmptyOptionalIfRuleDoesntExist() {
      RegionRuleConditions conditions =
          RegionRuleConditions.newBuilder()
              .setIpReputation(
                  IpReputationCondition.newBuilder()
                      .setMinIpReputationSeverity(IpReputationSeverity.IP_REPUTATION_SEVERITY_HIGH))
              .build();

      UpdateRegionRuleRequest request =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id-1")
              .setName("updated-name")
              .setDescription("desc-1")
              .setInternal(true)
              .setRuleScope(ruleScope)
              .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
              .setConditions(conditions)
              .build();

      assertTrue(rulesManager.updateRegionRule(requestContext, request).isEmpty());
    }
  }

  @Nested
  class DeleteRegionRule {
    @Test
    void shouldDeleteRegionRule() {
      RegionRule regionRule = RegionRule.newBuilder().setId("id-1").setName("name-1").build();
      addRegionRules(ImmutableSortedMap.of("id-1", regionRule));
      assertDoesNotThrow(() -> rulesManager.deleteRegionRule(requestContext, "id-1"));
    }
  }

  @Test
  void testConfigChangeEventsGeneratedForCreateAndUpdate_AAP11627() {
    // Regression test for AAP-11627: Verify that config change events are generated
    // for create and update operations. Guards against regression where migration or other
    // code mutating RequestContext (e.g. withUserTrackingSuppressed()) causes subsequent
    // upsert operations to skip event generation.
    MockGenericConfigService localMockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    localMockConfigService.start();
    try {
      ConfigChangeEventGenerator spyEventGenerator = mock(ConfigChangeEventGenerator.class);
      ConfigServiceGrpc.ConfigServiceBlockingStub stub =
          ConfigServiceGrpc.newBlockingStub(localMockConfigService.channel());
      RegionRulesStore localStore =
          new RegionRulesStore(
              stub,
              spyEventGenerator,
              mock(ai.traceable.region.config.service.RegionConfigServiceConfig.class));
      UuidGenerator localUuidGenerator = mock(UuidGenerator.class);
      when(localUuidGenerator.generateRandomId()).thenReturn("event-test-id");
      RegionRulesManager managerWithUuid =
          new RegionRulesManager(clock, localStore, localUuidGenerator);
      RequestContext testContext = RequestContext.forTenantId("event-test-tenant");

      RegionRuleConditions conditions =
          RegionRuleConditions.newBuilder()
              .setIpReputation(
                  IpReputationCondition.newBuilder()
                      .setMinIpReputationSeverity(IpReputationSeverity.IP_REPUTATION_SEVERITY_HIGH))
              .build();
      CreateRegionRuleRequest createRequest =
          CreateRegionRuleRequest.newBuilder()
              .setName("event-test-rule")
              .setRuleScope(ruleScope)
              .setConditions(conditions)
              .build();
      managerWithUuid.createRegionRule(testContext, createRequest);
      verify(spyEventGenerator, atLeastOnce())
          .sendCreateNotification(
              any(RequestContext.class), any(String.class), any(String.class), any(Value.class));

      UpdateRegionRuleRequest updateRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("event-test-id")
              .setName("event-test-rule-updated")
              .setRuleScope(ruleScope)
              .setConditions(conditions)
              .build();
      managerWithUuid.updateRegionRule(testContext, updateRequest);
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

  private void addRegionRules(Map<String, RegionRule> regionRuleConfigs) {
    regionRuleConfigs.forEach(
        (id, regionRule) -> regionRulesStore.upsertObject(requestContext, regionRule));
  }
}
