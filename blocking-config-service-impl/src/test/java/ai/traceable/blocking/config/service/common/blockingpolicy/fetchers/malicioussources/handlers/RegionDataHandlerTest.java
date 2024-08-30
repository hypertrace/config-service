package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.malicioussources.config.service.v1.AgentModification;
import ai.traceable.malicioussources.config.service.v1.AgentRuleEffect;
import ai.traceable.malicioussources.config.service.v1.EventSeverity;
import ai.traceable.malicioussources.config.service.v1.FieldValue;
import ai.traceable.malicioussources.config.service.v1.HeaderInjection;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.PredicateLocation;
import ai.traceable.malicioussources.config.service.v1.Region;
import ai.traceable.malicioussources.config.service.v1.RegionCondition;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.malicioussources.config.service.v1.RuleEffectWithModifications;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionDataHandlerTest {

  private RegionDataHandler handler;

  @BeforeEach
  void setUp() {
    handler = new RegionDataHandler(new BlockingRulesUtils(Clock.systemUTC()));
  }

  @Test
  void testGetBlockingDetails_Active() {
    MaliciousSourcesRule rule = createMaliciousSourcesRule(RuleActionType.RULE_ACTION_TYPE_BLOCK);

    Optional<BlockingPolicyData> result = handler.getBlockingDetails(rule);

    assertTrue(result.isPresent());
    BlockingPolicyData data = result.get();
    assertEquals(RuleType.BLOCK, data.getRuleType());
    assertEquals(BlockingPolicyDataBucket.REGION_VIOLATIONS, data.getBucket());
    assertEquals(BlockingPolicyData.Category.CUSTOM_REGION_RULE, data.getCategory());
    assertNotNull(data.getBlockingDetails());
    assertTrue(data.getBlockingDetails() instanceof RegionBlockingDetails);

    RegionBlockingDetails regionBlockingDetails = (RegionBlockingDetails) data.getBlockingDetails();
    List<String> regions = regionBlockingDetails.getRegions();
    assertEquals(2, regions.size());
    assertTrue(regions.contains("US"));
    assertTrue(regions.contains("IN"));

    assertNull(data.getAction());
  }

  @Test
  void testGetBlockingDetails_Analytics() {
    MaliciousSourcesRule rule = createMaliciousSourcesRule(RuleActionType.RULE_ACTION_TYPE_ALERT);

    Optional<BlockingPolicyData> result = handler.getBlockingDetails(rule);

    assertTrue(result.isPresent());
    BlockingPolicyData data = result.get();
    assertEquals(RuleType.ANALYTICS, data.getRuleType());
    assertEquals(BlockingPolicyDataBucket.REGION_ANALYTICS, data.getBucket());
    assertEquals(BlockingPolicyData.Category.CUSTOM_REGION_RULE, data.getCategory());
    assertNotNull(data.getBlockingDetails());
    assertTrue(data.getBlockingDetails() instanceof RegionBlockingDetails);

    RegionBlockingDetails regionBlockingDetails = (RegionBlockingDetails) data.getBlockingDetails();
    List<String> regions = regionBlockingDetails.getRegions();
    assertEquals(2, regions.size());
    assertTrue(regions.contains("US"));
    assertTrue(regions.contains("IN"));

    assertNotNull(data.getAction());
    assertEquals(1, data.getAction().getInlineModificationsCount());
    assertEquals(
        "apple", data.getAction().getInlineModifications(0).getHeaderInjection().getHeaderName());
    assertEquals(
        "mango",
        data.getAction()
            .getInlineModifications(0)
            .getHeaderInjection()
            .getValue()
            .getStaticValue());
  }

  private MaliciousSourcesRule createMaliciousSourcesRule(RuleActionType actionType) {
    RegionCondition regionCondition =
        RegionCondition.newBuilder()
            .addRegions(Region.newBuilder().setCountryIsoCode("US").build())
            .addRegions(Region.newBuilder().setCountryIsoCode("IN").build())
            .build();

    MaliciousSourcesRuleCondition condition =
        MaliciousSourcesRuleCondition.newBuilder().setRegionCondition(regionCondition).build();

    MaliciousSourcesRuleAction action =
        MaliciousSourcesRuleAction.newBuilder()
            .setActionType(actionType)
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
            .build();
    if (actionType.equals(RuleActionType.RULE_ACTION_TYPE_ALERT)) {
      action =
          action.toBuilder()
              .addEffects(
                  RuleEffectWithModifications.newBuilder()
                      .setAgentRuleEffect(
                          AgentRuleEffect.newBuilder()
                              .addAgentModifications(
                                  AgentModification.newBuilder()
                                      .setHeaderInjection(
                                          HeaderInjection.newBuilder()
                                              .setHeaderLocation(
                                                  PredicateLocation.PREDICATE_LOCATION_REQUEST)
                                              .setHeaderName("apple")
                                              .setValue(
                                                  FieldValue.newBuilder()
                                                      .setStaticValue("mango"))))))
              .build();
    }
    MaliciousSourcesRuleInfo ruleInfo =
        MaliciousSourcesRuleInfo.newBuilder()
            .setName("Test Region Rule")
            .setRuleAction(action)
            .addConditions(condition)
            .build();

    return MaliciousSourcesRule.newBuilder()
        .setId("region-rule-id-1")
        .setRuleInfo(ruleInfo)
        .build();
  }
}
