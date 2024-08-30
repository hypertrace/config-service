package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.malicioussources.config.service.v1.AgentModification;
import ai.traceable.malicioussources.config.service.v1.AgentRuleEffect;
import ai.traceable.malicioussources.config.service.v1.EventSeverity;
import ai.traceable.malicioussources.config.service.v1.ExpirationDetails;
import ai.traceable.malicioussources.config.service.v1.FieldValue;
import ai.traceable.malicioussources.config.service.v1.HeaderInjection;
import ai.traceable.malicioussources.config.service.v1.IpAddressCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.PredicateLocation;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.malicioussources.config.service.v1.RuleEffectWithModifications;
import com.google.protobuf.Timestamp;
import java.time.Clock;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IpRangeDataHandlerTest {

  private IpRangeDataHandler handler;

  @BeforeEach
  void setUp() {
    handler = new IpRangeDataHandler(new BlockingRulesUtils(Clock.systemUTC()));
  }

  @Test
  void testGetBlockingDetails_Inactive() {
    MaliciousSourcesRule rule =
        createMaliciousSourcesRule(RuleActionType.RULE_ACTION_TYPE_BLOCK, 20L);
    Optional<BlockingPolicyData> result = handler.getBlockingDetails(rule);
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetBlockingDetails_Active() {
    MaliciousSourcesRule rule =
        createMaliciousSourcesRule(RuleActionType.RULE_ACTION_TYPE_BLOCK, 1756401052L);

    Optional<BlockingPolicyData> result = handler.getBlockingDetails(rule);

    assertTrue(result.isPresent());
    BlockingPolicyData data = result.get();
    assertEquals(BlockingPolicyData.RuleType.BLOCK, data.getRuleType());
    assertEquals(BlockingPolicyDataBucket.IP_RANGE_VIOLATIONS, data.getBucket());
    assertEquals(BlockingPolicyData.Category.CUSTOM_IP_RULE, data.getCategory());
    assertNotNull(data.getBlockingDetails());
    assertTrue(data.getBlockingDetails() instanceof IpBlockingDetails);

    IpBlockingDetails ipBlockingDetails = (IpBlockingDetails) data.getBlockingDetails();
    assertEquals(1, ipBlockingDetails.getIpAddresses().size());
    assertEquals("192.168.1.1", ipBlockingDetails.getIpAddresses().get(0));
    assertEquals(1, ipBlockingDetails.getIpRanges().size());
    assertEquals("192.168.1.0/24", ipBlockingDetails.getIpRanges().get(0));

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

  @Test
  void testGenerateBlockingDetails() {
    MaliciousSourcesRule rule =
        createMaliciousSourcesRule(RuleActionType.RULE_ACTION_TYPE_ALERT, 0L);

    Optional<BlockingPolicyData> result = handler.getBlockingDetails(rule);

    assertTrue(result.isPresent());
    BlockingPolicyData data = result.get();
    assertEquals(RuleType.ANALYTICS, data.getRuleType());
    assertEquals(BlockingPolicyDataBucket.IP_RANGE_ANALYTICS, data.getBucket());
    assertEquals(BlockingPolicyData.Category.CUSTOM_IP_RULE, data.getCategory());
    assertNotNull(data.getBlockingDetails());
    assertTrue(data.getBlockingDetails() instanceof IpBlockingDetails);

    IpBlockingDetails ipBlockingDetails = (IpBlockingDetails) data.getBlockingDetails();
    assertEquals(1, ipBlockingDetails.getIpAddresses().size());
    assertEquals("192.168.1.1", ipBlockingDetails.getIpAddresses().get(0));
    assertEquals(1, ipBlockingDetails.getIpRanges().size());
    assertEquals("192.168.1.0/24", ipBlockingDetails.getIpRanges().get(0));

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

  private MaliciousSourcesRule createMaliciousSourcesRule(
      RuleActionType actionType, long expirationTimestamp) {
    MaliciousSourcesRuleAction action =
        MaliciousSourcesRuleAction.newBuilder()
            .setActionType(actionType)
            .setExpirationDetails(
                ExpirationDetails.newBuilder()
                    .setExpirationTimestamp(
                        Timestamp.newBuilder().setSeconds(expirationTimestamp).build())
                    .build())
            .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
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
                                                FieldValue.newBuilder().setStaticValue("mango"))))))
            .build();

    IpAddressCondition ipAddressCondition =
        IpAddressCondition.newBuilder()
            .addIpAddresses("192.168.1.1")
            .addCidrIpRanges("192.168.1.0/24")
            .build();

    MaliciousSourcesRuleCondition condition =
        MaliciousSourcesRuleCondition.newBuilder().setIpRangeCondition(ipAddressCondition).build();

    return MaliciousSourcesRule.newBuilder()
        .setId("rule-id-1")
        .setRuleInfo(
            MaliciousSourcesRuleInfo.newBuilder()
                .setName("Test Rule")
                .setRuleAction(action)
                .addConditions(condition)
                .build())
        .build();
  }
}
