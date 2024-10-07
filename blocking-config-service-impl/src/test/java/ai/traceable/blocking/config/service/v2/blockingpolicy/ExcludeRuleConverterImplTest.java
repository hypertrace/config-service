package ai.traceable.blocking.config.service.v2.blockingpolicy;

import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_MODSECURITY;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_TRANSACTION_BASED_DLP;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALLOW;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_BLOCK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.blocking.config.service.v2.BlockingDetailsCombination;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCombination.ConditionsOperator;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;
import ai.traceable.blocking.config.service.v2.ExclusionRule;
import ai.traceable.blocking.config.service.v2.ExclusionRule.AnomalousAttributeCondition;
import ai.traceable.blocking.config.service.v2.IpDetails;
import ai.traceable.blocking.config.service.v2.IpType;
import ai.traceable.blocking.config.service.v2.IpTypeDetails;
import ai.traceable.blocking.config.service.v2.MatchOperator;
import ai.traceable.blocking.config.service.v2.RegionDetails;
import ai.traceable.blocking.config.service.v2.blockingpolicy.exclusion.ExcludeRuleConverterImpl;
import ai.traceable.blocking.config.service.v2.blockingpolicy.exclusion.ExclusionRuleConverter;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationType;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import com.google.protobuf.util.Values;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExcludeRuleConverterImplTest {

  @Test
  void testAnomalyAttributeConvert() {
    MatchCondition oldKeyMatchCondition =
        MatchCondition.newBuilder()
            .setOperator(
                ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                    .MATCH_OPERATOR_EQUALS)
            .setValue(Values.of("testKeyValue"))
            .build();

    MatchCondition oldValueMatchCondition =
        MatchCondition.newBuilder()
            .setOperator(
                ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                    .MATCH_OPERATOR_NOT_EQUAL)
            .setValue(Values.of("testValueValue"))
            .build();

    ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition oldCondition =
        ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition.newBuilder()
            .setKeyMatchCondition(oldKeyMatchCondition)
            .setValueMatchCondition(oldValueMatchCondition)
            .build();

    ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition
        onlyKeyCondition =
            ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition
                .newBuilder()
                .setKeyMatchCondition(oldKeyMatchCondition)
                .build();

    DetectionExclusionModsecRule detectionExclusionModsecRule =
        DetectionExclusionModsecRule.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setId("id-1")
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setAnomalousAttributeCondition(oldCondition))
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setAnomalousAttributeCondition(onlyKeyCondition))
                            .addExclusionTargets(EXCLUSION_TARGET_ALLOW)
                            .addExclusionTargets(EXCLUSION_TARGET_BLOCK)))
            .build();

    ExclusionRuleConverter exclusionRuleConverter = new ExcludeRuleConverterImpl();
    ExclusionRule exclusionRule = exclusionRuleConverter.convert(detectionExclusionModsecRule);

    assertEquals("id-1", exclusionRule.getRuleId());
    assertEquals(
        List.of(
            ExclusionRule.ExclusionTarget.EXCLUSION_TARGET_ALLOW,
            ExclusionRule.ExclusionTarget.EXCLUSION_TARGET_BLOCK),
        exclusionRule.getTargetsList());

    assertEquals(2, exclusionRule.getAnomalousAttributeConditionsCount());

    AnomalousAttributeCondition newCondition = exclusionRule.getAnomalousAttributeConditions(0);
    assertTrue(newCondition.hasKeyExpression());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_EQUALS, newCondition.getKeyExpression().getOperator());
    assertEquals("testKeyValue", newCondition.getKeyExpression().getValue().getStringValue());
    assertTrue(newCondition.hasValueExpression());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_NOT_EQUALS, newCondition.getValueExpression().getOperator());
    assertEquals("testValueValue", newCondition.getValueExpression().getValue().getStringValue());

    assertEquals(
        newCondition.getKeyExpression(),
        exclusionRule.getAnomalousAttributeConditions(1).getKeyExpression());
    assertFalse(exclusionRule.getAnomalousAttributeConditions(1).hasValueExpression());
  }

  @Test
  void testEventConditionConvert() {
    SystemDefinedEvent systemEvent1 =
        SystemDefinedEvent.newBuilder()
            .setEventFamily(SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_MODSEC)
            .setEventTypeId("crs_921")
            .build();

    SystemDefinedEvent systemEvent2 =
        SystemDefinedEvent.newBuilder()
            .setEventFamily(SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_MODSEC)
            .setEventSubTypeId("crs_92160")
            .build();

    CustomRuleEvent customRuleEvent1 =
        CustomRuleEvent.newBuilder()
            .setRuleFamily(CustomRuleFamily.CUSTOM_RULE_FAMILY_SIGNATURE)
            .setRuleId("rule1")
            .build();

    CustomRuleEvent customRuleEvent2 =
        CustomRuleEvent.newBuilder()
            .setRuleFamily(CustomRuleFamily.CUSTOM_RULE_FAMILY_RATE_LIMIT)
            .build();

    // Create old event condition
    EventCondition oldCondition =
        EventCondition.newBuilder()
            .addSystemDefinedEvents(systemEvent1)
            .addSystemDefinedEvents(systemEvent2)
            .addCustomRuleEvents(customRuleEvent1)
            .addCustomRuleEvents(customRuleEvent2)
            .build();

    EventCondition eventCondition2 =
        EventCondition.newBuilder()
            .addCustomRuleEvents(
                CustomRuleEvent.newBuilder()
                    .setRuleFamily(CustomRuleFamily.CUSTOM_RULE_FAMILY_DATA_LOSS_PREVENTION)
                    .setRuleId("dlp-rule-id"))
            .build();

    DetectionExclusionModsecRule detectionExclusionModsecRule =
        DetectionExclusionModsecRule.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setEventCondition(oldCondition))
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setEventCondition(eventCondition2))))
            .build();

    ExclusionRuleConverter exclusionRuleConverter = new ExcludeRuleConverterImpl();
    ExclusionRule exclusionRule = exclusionRuleConverter.convert(detectionExclusionModsecRule);

    assertEquals(4, exclusionRule.getEventConditionsCount());
    assertEquals(
        BLOCKING_CATEGORY_MODSECURITY, exclusionRule.getEventConditions(0).getBlockingCategory());
    assertEquals(List.of("crs_921"), exclusionRule.getEventConditions(0).getIdPrefixesList());
    assertEquals(List.of("crs_92160"), exclusionRule.getEventConditions(0).getIdsList());
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        exclusionRule.getEventConditions(1).getBlockingCategory());
    assertEquals(List.of("rule1"), exclusionRule.getEventConditions(1).getIdsList());
    assertEquals(
        BLOCKING_CATEGORY_RATE_LIMIT, exclusionRule.getEventConditions(2).getBlockingCategory());
    assertTrue(exclusionRule.getEventConditions(2).getIdsList().isEmpty());
    assertEquals(
        BLOCKING_CATEGORY_TRANSACTION_BASED_DLP,
        exclusionRule.getEventConditions(3).getBlockingCategory());
    assertEquals(List.of("dlp-rule-id"), exclusionRule.getEventConditions(3).getIdsList());
  }

  @Test
  void testConvertIpAddressCondition() {
    IpAddressCondition ipAddressCondition =
        IpAddressCondition.newBuilder()
            .addAllIpAddresses(Arrays.asList("192.168.1.1", "10.0.0.1"))
            .addAllCidrIpRanges(Arrays.asList("192.168.0.0/24", "10.0.0.0/8"))
            .build();

    IpDetails expectedIpDetails =
        IpDetails.newBuilder()
            .addAllIpAddresses(Arrays.asList("192.168.1.1", "10.0.0.1"))
            .addAllIpRanges(Arrays.asList("192.168.0.0/24", "10.0.0.0/8"))
            .build();

    BlockingDetailsCondition expectedCondition =
        BlockingDetailsCondition.newBuilder().setIpDetails(expectedIpDetails).build();

    BlockingDetailsCondition expectedConditionNot =
        BlockingDetailsCondition.newBuilder()
            .setDetailsCombination(
                BlockingDetailsCombination.newBuilder()
                    .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_NOT)
                    .addDetailsConditions(expectedCondition))
            .build();

    DetectionExclusionModsecRule detectionExclusionModsecRule =
        DetectionExclusionModsecRule.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setIpAddressCondition(ipAddressCondition))
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setIpAddressCondition(
                                        ipAddressCondition.toBuilder().setExclude(true)))
                            .addExclusionTargets(EXCLUSION_TARGET_BLOCK)))
            .addAssociatedModsecRuleIds("cs-1")
            .addAssociatedModsecRuleIds("cs-2")
            .build();

    ExclusionRuleConverter exclusionRuleConverter = new ExcludeRuleConverterImpl();
    ExclusionRule exclusionRule = exclusionRuleConverter.convert(detectionExclusionModsecRule);

    assertEquals(
        ConditionsOperator.CONDITIONS_OPERATOR_AND, exclusionRule.getDetails().getOperator());
    assertEquals(4, exclusionRule.getDetails().getDetailsConditionsCount());
    assertEquals(
        "cs-1",
        exclusionRule.getDetails().getDetailsConditions(0).getCustomSignatureDetails().getRuleId());
    assertEquals(
        "cs-2",
        exclusionRule.getDetails().getDetailsConditions(1).getCustomSignatureDetails().getRuleId());
    assertEquals(expectedCondition, exclusionRule.getDetails().getDetailsConditions(2));
    assertEquals(expectedConditionNot, exclusionRule.getDetails().getDetailsConditions(3));
    assertEquals(
        List.of(ExclusionRule.ExclusionTarget.EXCLUSION_TARGET_BLOCK),
        exclusionRule.getTargetsList());
  }

  @Test
  void testConvertRegionCondition() {
    RegionCondition regionCondition =
        RegionCondition.newBuilder()
            .addRegions(RegionCondition.Region.newBuilder().setCountryIsoCode("US").build())
            .build();

    RegionDetails expectedRegionDetails = RegionDetails.newBuilder().addRegions("US").build();

    BlockingDetailsCondition expectedCondition =
        BlockingDetailsCondition.newBuilder().setRegionDetails(expectedRegionDetails).build();

    BlockingDetailsCondition expectedConditionNot =
        BlockingDetailsCondition.newBuilder()
            .setDetailsCombination(
                BlockingDetailsCombination.newBuilder()
                    .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_NOT)
                    .addDetailsConditions(expectedCondition))
            .build();

    DetectionExclusionModsecRule detectionExclusionModsecRule =
        DetectionExclusionModsecRule.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setRegionCondition(regionCondition))
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setRegionCondition(
                                        regionCondition.toBuilder().setExclude(true)))))
            .addAssociatedModsecRuleIds("cs-1")
            .addAssociatedModsecRuleIds("cs-2")
            .build();

    ExclusionRuleConverter exclusionRuleConverter = new ExcludeRuleConverterImpl();
    ExclusionRule exclusionRule = exclusionRuleConverter.convert(detectionExclusionModsecRule);

    assertEquals(
        ConditionsOperator.CONDITIONS_OPERATOR_AND, exclusionRule.getDetails().getOperator());
    assertEquals(4, exclusionRule.getDetails().getDetailsConditionsCount());
    assertEquals(expectedCondition, exclusionRule.getDetails().getDetailsConditions(2));
    assertEquals(expectedConditionNot, exclusionRule.getDetails().getDetailsConditions(3));
  }

  @Test
  void testConvertIpLocationTypeCondition() {
    IpLocationTypeCondition ipTypeLocation =
        IpLocationTypeCondition.newBuilder()
            .addAllIpLocationTypes(
                Arrays.asList(
                    IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN,
                    IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE))
            .build();

    IpTypeDetails expectedIpTypeDetails =
        IpTypeDetails.newBuilder()
            .addAllIpTypes(Arrays.asList(IpType.IP_TYPE_VPN, IpType.IP_TYPE_TOR))
            .build();

    BlockingDetailsCondition expectedCondition =
        BlockingDetailsCondition.newBuilder().setIpTypeDetails(expectedIpTypeDetails).build();

    BlockingDetailsCondition expectedConditionNot =
        BlockingDetailsCondition.newBuilder()
            .setDetailsCombination(
                BlockingDetailsCombination.newBuilder()
                    .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_NOT)
                    .addDetailsConditions(expectedCondition))
            .build();

    DetectionExclusionModsecRule detectionExclusionModsecRule =
        DetectionExclusionModsecRule.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setIpLocationTypeCondition(ipTypeLocation))
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setIpLocationTypeCondition(
                                        ipTypeLocation.toBuilder().setExclude(true)))))
            .addAssociatedModsecRuleIds("cs-1")
            .build();

    ExclusionRuleConverter exclusionRuleConverter = new ExcludeRuleConverterImpl();
    ExclusionRule exclusionRule = exclusionRuleConverter.convert(detectionExclusionModsecRule);

    assertEquals(
        ConditionsOperator.CONDITIONS_OPERATOR_AND, exclusionRule.getDetails().getOperator());
    assertEquals(3, exclusionRule.getDetails().getDetailsConditionsCount());
    assertEquals(expectedCondition, exclusionRule.getDetails().getDetailsConditions(1));
    assertEquals(expectedConditionNot, exclusionRule.getDetails().getDetailsConditions(2));
  }
}
