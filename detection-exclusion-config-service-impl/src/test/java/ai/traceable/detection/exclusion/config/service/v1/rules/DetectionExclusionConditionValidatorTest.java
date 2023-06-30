package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.AttributeValueType.ATTRIBUTE_VALUE_TYPE_CREDIT_CARD;
import static ai.traceable.detection.exclusion.config.service.v1.AttributeValueType.ATTRIBUTE_VALUE_TYPE_DATE;
import static ai.traceable.detection.exclusion.config.service.v1.AttributeValueType.ATTRIBUTE_VALUE_TYPE_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.EntityScope;
import ai.traceable.detection.exclusion.config.service.v1.EntityType;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionType;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationType;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationSeverity;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadata;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadataMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.LabelScope;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchOperator;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.UserIdCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionConditionValidatorTest {

  private DetectionExclusionConditionValidator conditionValidator;

  @BeforeEach
  void setUp() {
    conditionValidator = new DetectionExclusionConditionValidator();
  }

  @Test
  void testValidateScopeCondition() {
    // condition not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setScopeCondition(ScopeCondition.getDefaultInstance())
              .build();

      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid scopeConditionCase"));
    }

    // entity scope not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setScopeCondition(
                  ScopeCondition.newBuilder()
                      .setEntityScope(EntityScope.getDefaultInstance())
                      .build())
              .build();

      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition));
    }

    // label scope not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setScopeCondition(
                  ScopeCondition.newBuilder()
                      .setLabelScope(LabelScope.getDefaultInstance())
                      .build())
              .build();

      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition));
    }

    // entity ids not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setScopeCondition(
                  ScopeCondition.newBuilder()
                      .setEntityScope(
                          EntityScope.newBuilder().setEntityType(EntityType.ENTITY_TYPE_API))
                      .build())
              .build();

      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition));
    }

    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setScopeCondition(
                  ScopeCondition.newBuilder()
                      .setEntityScope(
                          EntityScope.newBuilder()
                              .setEntityType(EntityType.ENTITY_TYPE_API)
                              .addEntityIds("id"))
                      .build())
              .build();

      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition));
    }
  }

  @Test
  void testValidateAttributeMatchCondition() {
    // condition not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(SpanAttributeMatchCondition.getDefaultInstance())
              .build();

      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid spanAttributeMatchCondition"));
    }

    // key metadata not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(KeyMetadataMatchCondition.getDefaultInstance()))
              .build();
      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition));
    }

    // value not set for match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_HOST)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS))))
              .build();
      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition));
    }

    // invalid value type for regex match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_HOST)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setNumberValue(2))))
                      .setValueMatchCondition(MatchCondition.getDefaultInstance()))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(
          throwable
              .getMessage()
              .contains("value should be string or list of strings for regex matching"));
    }

    // invalid list value for regex match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_HOST)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(
                                          Value.newBuilder()
                                              .setListValue(
                                                  ListValue.newBuilder()
                                                      .addValues(Value.getDefaultInstance())))))
                      .setValueMatchCondition(MatchCondition.getDefaultInstance()))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(
          throwable
              .getMessage()
              .contains("value should be string or list of strings for regex matching"));
    }

    // invalid regex for match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_HOST)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setStringValue("*"))))
                      .setValueMatchCondition(MatchCondition.getDefaultInstance()))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid Regex"));
    }

    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_HOST)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setStringValue("abc.*"))))
                      .setValueMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                              .setValue(Value.newBuilder().setStringValue("value"))))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition));
    }
  }

  @Test
  void testValidateIpLocationTypeCondition() {
    // condition not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpLocationTypeCondition(IpLocationTypeCondition.getDefaultInstance())
              .build();
      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition));
    }

    // invalid ip location type
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpLocationTypeCondition(
                  IpLocationTypeCondition.newBuilder()
                      .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_UNSPECIFIED))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid IP location type"));
    }

    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpLocationTypeCondition(
                  IpLocationTypeCondition.newBuilder()
                      .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_BOT))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition));
    }
  }

  @Test
  void validateIpReputationCondition() {
    // condition not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpReputationCondition(IpReputationCondition.getDefaultInstance())
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid ipReputationCondition"));
    }

    // ip rep score > 100
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpReputationCondition(
                  IpReputationCondition.newBuilder().setMaxIpReputationScore(200))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("IP reputation score should be >= 0 and <= 100"));
    }

    // invalid ip rep severity
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpReputationCondition(
                  IpReputationCondition.newBuilder()
                      .setMaxIpReputationSeverity(
                          IpReputationSeverity.IP_REPUTATION_SEVERITY_UNSPECIFIED))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid IP reputation severity"));
    }

    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpReputationCondition(
                  IpReputationCondition.newBuilder()
                      .setMaxIpReputationSeverity(
                          IpReputationSeverity.IP_REPUTATION_SEVERITY_MEDIUM))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition));
    }
  }

  @Test
  void testValidateUserIdCondition() {
    // condition not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setUserIdCondition(UserIdCondition.getDefaultInstance())
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid userIdCondition"));
    }

    // invalid user id regex
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setUserIdCondition(UserIdCondition.newBuilder().addUserIdRegexes("*"))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid Regex"));
    }

    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setUserIdCondition(UserIdCondition.newBuilder().addUserIdRegexes("abc.*"))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition));
    }
  }

  @Test
  void testValidateEventCondition() {
    // condition not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(EventCondition.getDefaultInstance())
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid eventCondition"));
    }

    // invalid custom rule family
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addCustomRuleEvents(
                          CustomRuleEvent.newBuilder()
                              .setRuleFamily(CustomRuleFamily.CUSTOM_RULE_FAMILY_UNSPECIFIED)))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid custom rule family"));
    }

    // empty ruleId for custom rule event
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addCustomRuleEvents(
                          CustomRuleEvent.newBuilder()
                              .setRuleFamily(CustomRuleFamily.CUSTOM_RULE_FAMILY_SIGNATURE)
                              .setRuleId("")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("RuleId provided is empty"));
    }

    // invalid system defined event family
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setEventFamily(
                                  SystemDefinedEventFamily
                                      .SYSTEM_DEFINED_EVENT_FAMILY_UNSPECIFIED)))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid system defined event family"));
    }

    // empty typeId for system defined event
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_MODSEC)
                              .setEventTypeId("")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("TypeId provided is empty"));
    }
    // value not set for match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setDescriptionMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS))
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventTypeId("integer")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Match condition should have a valid value"));
    }

    // invalid value type for regex match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setDescriptionMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setNumberValue(2)))
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventTypeId("integer")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(
          throwable
              .getMessage()
              .contains("value should be string or list of strings for regex matching"));
    }
    // invalid list value for regex match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setDescriptionMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(
                                          Value.newBuilder()
                                              .setListValue(
                                                  ListValue.newBuilder()
                                                      .addValues(Value.getDefaultInstance()))))
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventTypeId("integer")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(
          throwable
              .getMessage()
              .contains("value should be string or list of strings for regex matching"));
    }
    // invalid regex for match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setDescriptionMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setStringValue("*")))
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventTypeId("integer")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid Regex"));
    }
    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setDescriptionMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setStringValue("abc.*")))
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventTypeId("integer")))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition));
    }
  }

  @Test
  void testValidateAnomalousAttributeCondition() {
    // condition not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAnomalousAttributeCondition(AnomalousAttributeCondition.getDefaultInstance())
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));
      assertTrue(throwable.getMessage().contains("Invalid anomalousAttributeCondition"));
    }

    // invalid enum condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAnomalousAttributeCondition(
                  AnomalousAttributeCondition.newBuilder()
                      .addAllObservedTypes(List.of(ATTRIBUTE_VALUE_TYPE_UNSPECIFIED))
                      .addAllLearntTypes(
                          List.of(ATTRIBUTE_VALUE_TYPE_CREDIT_CARD, ATTRIBUTE_VALUE_TYPE_DATE))
                      .setKeyMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                              .setValue(Value.newBuilder().setStringValue("abc.*"))))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition));

      assertTrue(
          throwable
              .getMessage()
              .contains(String.format("Invalid type : ATTRIBUTE_VALUE_TYPE_UNSPECIFIED")));

      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setAnomalousAttributeCondition(
                  AnomalousAttributeCondition.newBuilder()
                      .addAllObservedTypes(List.of(ATTRIBUTE_VALUE_TYPE_CREDIT_CARD))
                      .addAllLearntTypes(
                          List.of(
                              ATTRIBUTE_VALUE_TYPE_CREDIT_CARD, ATTRIBUTE_VALUE_TYPE_UNSPECIFIED))
                      .setKeyMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                              .setValue(Value.newBuilder().setStringValue("abc.*"))))
              .build();
      throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(condition1));

      assertTrue(
          throwable
              .getMessage()
              .contains(String.format("Invalid type : ATTRIBUTE_VALUE_TYPE_UNSPECIFIED")));
    }
    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAnomalousAttributeCondition(
                  AnomalousAttributeCondition.newBuilder()
                      .addAllObservedTypes(List.of(ATTRIBUTE_VALUE_TYPE_CREDIT_CARD))
                      .addAllLearntTypes(
                          List.of(ATTRIBUTE_VALUE_TYPE_CREDIT_CARD, ATTRIBUTE_VALUE_TYPE_DATE))
                      .setKeyMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                              .setValue(Value.newBuilder().setStringValue("abc.*"))))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition));

      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setAnomalousAttributeCondition(
                  AnomalousAttributeCondition.newBuilder()
                      .setKeyMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                              .setValue(Value.newBuilder().setStringValue("abc.*"))))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition1));
    }
  }

  @Test
  void testValidateIpAddressCondition() {
    // condition not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(IpAddressCondition.getDefaultInstance())
              .build();
      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition));
    }

    // invalid ip address condition (either rawInputIpData or (any one of cidrRanges or ipAddress)
    // can be set)
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .addAllRawInputIpData(List.of("2.3.4.5", "1.2.3.4/23"))
                      .addIpAddresses("1.2.3.4"))
              .build();
      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition));

      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .addAllRawInputIpData(List.of("2.3.4.5", "1.2.3.4/23"))
                      .addCidrIpRanges("192.3.4.5/2"))
              .build();
      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition1));

      DetectionExclusionCondition condition2 =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .addAllRawInputIpData(List.of("2.3.4.5", "1.2.3.4/23"))
                      .addCidrIpRanges("192.3.4.5/2")
                      .addIpAddresses("1.2.3.4"))
              .build();
      assertThrows(
          StatusRuntimeException.class, () -> conditionValidator.validateRuleCondition(condition2));
    }

    // valid ip address condition (empty rawInputIpData and any one of cidrRanges or ip address is
    // present)
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(IpAddressCondition.newBuilder().addIpAddresses("1.2.3.4"))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition));
      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(IpAddressCondition.newBuilder().addCidrIpRanges("192.3.4.5/2"))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition1));
    }

    // valid ip address condition (raw input ip data is present)
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .addAllRawInputIpData(List.of("2.3.4.5", "1.2.3.4/23")))
              .build();
      assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition));
    }
  }

  @Test
  void testValidateIpConnectionTypeCondition() {
    DetectionExclusionCondition condition =
        DetectionExclusionCondition.newBuilder()
            .setIpConnectionTypeCondition(
                IpConnectionTypeCondition.newBuilder()
                    .addIpConnectionTypes(IpConnectionType.IP_CONNECTION_TYPE_UNSPECIFIED)
                    .build())
            .build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> conditionValidator.validateRuleCondition(condition));
    assertTrue(
        throwable
            .getMessage()
            .contains("Invalid IP Connection Type : IP_CONNECTION_TYPE_UNSPECIFIED"));

    // valid condition
    DetectionExclusionCondition condition1 =
        DetectionExclusionCondition.newBuilder()
            .setIpConnectionTypeCondition(
                IpConnectionTypeCondition.newBuilder()
                    .addIpConnectionTypes(IpConnectionType.IP_CONNECTION_TYPE_MOBILE)
                    .build())
            .build();
    assertDoesNotThrow(() -> conditionValidator.validateRuleCondition(condition1));
  }
}
