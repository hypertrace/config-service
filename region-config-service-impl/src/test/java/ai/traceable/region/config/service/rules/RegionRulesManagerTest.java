package ai.traceable.region.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.utils.UuidGenerator;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RuleScope;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import com.google.common.collect.ImmutableSortedMap;
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
  private ConfigChangeEventGenerator configChangeEventGenerator;

  @BeforeEach
  void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    uuidGenerator = mock(UuidGenerator.class);
    requestContext = RequestContext.forTenantId("default tenant");
    configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    regionRulesStore = new RegionRulesStore(configServiceBlockingStub, configChangeEventGenerator);
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
      RuleScope ruleScope =
          RuleScope.newBuilder()
              .setEnvironmentScope(
                  EnvironmentScope.newBuilder().addEnvironmentIds("env1").addEnvironmentIds("env2"))
              .build();
      RegionRule mockRegionRule1 =
          RegionRule.newBuilder()
              .setRuleScope(ruleScope)
              .setId("id-1")
              .setName("name-1")
              .setDescription("desc-1")
              .setInternal(true)
              .build();
      RegionRule mockRegionRule2 =
          RegionRule.newBuilder().setId("id-2").setName("name-2").setDisabled(true).build();
      addRegionRules(ImmutableSortedMap.of("id-1", mockRegionRule1, "id-2", mockRegionRule2));

      RegionRule regionRule1 =
          RegionRule.newBuilder()
              .setId("id-1")
              .setName("name-1")
              .setRuleScope(ruleScope)
              .setDescription("desc-1")
              .setInternal(true)
              .build();
      RegionRule regionRule2 =
          RegionRule.newBuilder().setId("id-2").setName("name-2").setDisabled(true).build();
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
      when(uuidGenerator.generateId()).thenReturn("id-1");
      RegionRule regionRule =
          RegionRule.newBuilder()
              .setId("id-1")
              .setName("name-1")
              .setDescription("desc-1")
              .setRuleScope(ruleScope)
              .build();
      RegionRule createdRegionRule =
          rulesManager.createRegionRule(
              requestContext,
              CreateRegionRuleRequest.newBuilder()
                  .setName("name-1")
                  .setDescription("desc-1")
                  .setRuleScope(ruleScope)
                  .build());
      assertEquals(regionRule, createdRegionRule);
    }
  }

  @Nested
  class UpdateRegionRule {

    @Test
    void shouldUpdateRegionRule() {

      RegionRule originalRegionRule =
          RegionRule.newBuilder().setId("id-1").setName("name-1").build();
      addRegionRules(ImmutableSortedMap.of("id-1", originalRegionRule));
      RegionRule updatedRegionRule =
          RegionRule.newBuilder()
              .setId("id-1")
              .setName("updated-name")
              .setDescription("desc-1")
              .setInternal(true)
              .setRuleScope(ruleScope)
              .build();
      UpdateRegionRuleRequest request =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id-1")
              .setName("updated-name")
              .setDescription("desc-1")
              .setInternal(true)
              .setRuleScope(ruleScope)
              .build();
      assertEquals(updatedRegionRule, rulesManager.updateRegionRule(requestContext, request));
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

  private void addRegionRules(Map<String, RegionRule> regionRuleConfigs) {
    regionRuleConfigs.forEach(
        (id, regionRule) -> regionRulesStore.upsertObject(requestContext, regionRule));
  }
}
