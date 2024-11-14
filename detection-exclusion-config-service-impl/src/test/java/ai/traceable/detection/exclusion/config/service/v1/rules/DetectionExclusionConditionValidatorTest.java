package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.AttributeValueType.ATTRIBUTE_VALUE_TYPE_CREDIT_CARD;
import static ai.traceable.detection.exclusion.config.service.v1.AttributeValueType.ATTRIBUTE_VALUE_TYPE_DATE;
import static ai.traceable.detection.exclusion.config.service.v1.AttributeValueType.ATTRIBUTE_VALUE_TYPE_UNSPECIFIED;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALERT;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALLOW;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_BLOCK;
import static ai.traceable.detection.exclusion.config.service.v1.ThreatActorIdentifier.THREAT_ACTOR_IDENTIFIER_ACTOR_ENTITY_ID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.EmailDomainCondition;
import ai.traceable.detection.exclusion.config.service.v1.EntityScope;
import ai.traceable.detection.exclusion.config.service.v1.EntityType;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAbuseVelocity;
import ai.traceable.detection.exclusion.config.service.v1.IpAbuseVelocityCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressConditionType;
import ai.traceable.detection.exclusion.config.service.v1.IpAsnCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionType;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationType;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpOrganisationCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationSeverity;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadata;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadataMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.LabelScope;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchOperator;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition;
import ai.traceable.detection.exclusion.config.service.v1.RequestScannerTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.ThreatActorEvent;
import ai.traceable.detection.exclusion.config.service.v1.UrlScope;
import ai.traceable.detection.exclusion.config.service.v1.UserAgentCondition;
import ai.traceable.detection.exclusion.config.service.v1.UserIdCondition;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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

      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    }

    // invalid url scope condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setScopeCondition(
                  ScopeCondition.newBuilder().setUrlScope(UrlScope.getDefaultInstance()).build())
              .build();
      assertThrows(
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    }

    // wide regex invalid url scope condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setScopeCondition(
                  ScopeCondition.newBuilder()
                      .setUrlScope(UrlScope.newBuilder().addUrlRegexes(".*"))
                      .build())
              .build();
      assertThrows(
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    }

    // valid url scope condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setScopeCondition(
                  ScopeCondition.newBuilder()
                      .setUrlScope(
                          UrlScope.newBuilder().addAllUrlRegexes(List.of(".*reg1.*", ".*reg2.*")))
                      .build())
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    }

    // invalid value type for regex match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_REQUEST_BODY_PARAMETER)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setNumberValue(2))))
                      .setValueMatchCondition(MatchCondition.getDefaultInstance()))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      assertTrue(throwable.getMessage().contains("value should be string for regex matching"));
    }

    // invalid regex for match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_QUERY_PARAMETER)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setStringValue("*"))))
                      .setValueMatchCondition(MatchCondition.getDefaultInstance()))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      assertTrue(throwable.getMessage().contains("Invalid Regex"));
    }

    // invalid numerical match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_RESPONSE_BODY_SIZE))
                      .setValueMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_GREATER_THAN)
                              .setValue(Value.newBuilder().setStringValue("abc"))))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      assertTrue(throwable.getMessage().contains("Numerical value should be present"));
    }

    // invalid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_RESPONSE_BODY_SIZE)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setStringValue("abc.*"))))
                      .setValueMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                              .setValue(Value.newBuilder().setStringValue("value"))))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  conditionValidator.validateRuleCondition(
                      List.of(
                          EXCLUSION_TARGET_ALERT, EXCLUSION_TARGET_BLOCK, EXCLUSION_TARGET_ALLOW),
                      condition));
      assertTrue(throwable.getMessage().contains("exclusion target block"));
    }

    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_QUERY_PARAMETER)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setStringValue("abc.*"))))
                      .setValueMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                              .setValue(Value.newBuilder().setStringValue("value"))))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));

      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setAttributeMatchCondition(
                  SpanAttributeMatchCondition.newBuilder()
                      .setKeyMatchCondition(
                          KeyMetadataMatchCondition.newBuilder()
                              .setMetadata(KeyMetadata.KEY_METADATA_REQUEST_HEADER)
                              .setMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                      .setValue(Value.newBuilder().setStringValue("abc.*"))))
                      .setValueMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                              .setValue(Value.newBuilder().setStringValue("value"))))
              .build();
      assertDoesNotThrow(
          () ->
              conditionValidator.validateRuleCondition(
                  List.of(EXCLUSION_TARGET_ALERT, EXCLUSION_TARGET_BLOCK, EXCLUSION_TARGET_ALLOW),
                  condition1));
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
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));

      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setIpLocationTypeCondition(
                  IpLocationTypeCondition.newBuilder()
                      .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_SCANNER))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      assertTrue(throwable.getMessage().contains("Invalid Regex"));
    }

    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setUserIdCondition(UserIdCondition.newBuilder().addUserIdRegexes("abc.*"))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      assertTrue(throwable.getMessage().contains("RuleId provided is empty"));
    }

    // invalid threat actor event condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addThreatActorEvents(
                          ThreatActorEvent.newBuilder()
                              .setThreatActorIdentifier(THREAT_ACTOR_IDENTIFIER_ACTOR_ENTITY_ID)
                              .setActorEntityId("id")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  conditionValidator.validateRuleCondition(
                      List.of(EXCLUSION_TARGET_ALERT), condition));
      assertTrue(
          throwable.getMessage().contains("Invalid alert exclusion target for threat actor"));
    }

    // valid threat actor event condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addThreatActorEvents(
                          ThreatActorEvent.newBuilder()
                              .setThreatActorIdentifier(THREAT_ACTOR_IDENTIFIER_ACTOR_ENTITY_ID)
                              .setActorEntityId("id")))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      assertTrue(throwable.getMessage().contains("TypeId provided is empty"));
    }
    // invalid value set for match condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setDescriptionMatchCondition(
                                  MatchCondition.newBuilder()
                                      .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                      .setValue(
                                          Value.newBuilder()
                                              .setListValue(ListValue.getDefaultInstance())))
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventTypeId("integer")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      assertTrue(throwable.getMessage().contains("Match condition should have a valid value"));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));

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
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));

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
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));

      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setAnomalousAttributeCondition(
                  AnomalousAttributeCondition.newBuilder()
                      .setKeyMatchCondition(
                          MatchCondition.newBuilder()
                              .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                              .setValue(Value.newBuilder().setStringValue("abc.*"))))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));
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
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));

      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .addAllRawInputIpData(List.of("2.3.4.5", "1.2.3.4/23"))
                      .addCidrIpRanges("192.3.4.5/2"))
              .build();
      assertThrows(
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));

      DetectionExclusionCondition condition2 =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .addAllRawInputIpData(List.of("2.3.4.5", "1.2.3.4/23"))
                      .addCidrIpRanges("192.3.4.5/2")
                      .addIpAddresses("1.2.3.4"))
              .build();
      assertThrows(
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition2));

      DetectionExclusionCondition condition3 =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .setIpAddressConditionType(
                          IpAddressConditionType.IP_ADDRESS_CONDITION_TYPE_ALL_EXTERNAL)
                      .addAllRawInputIpData(List.of("2.3.4.5", "1.2.3.4/23"))
                      .addCidrIpRanges("192.3.4.5/2")
                      .addIpAddresses("1.2.3.4"))
              .build();
      assertThrows(
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition3));
    }

    // valid ip address condition (empty rawInputIpData and any one of cidrRanges or ip address is
    // present)
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(IpAddressCondition.newBuilder().addIpAddresses("1.2.3.4"))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(IpAddressCondition.newBuilder().addCidrIpRanges("192.3.4.5/2"))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));
    }

    // valid ip address condition (raw input ip data is present)
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .addAllRawInputIpData(List.of("2.3.4.5", "1.2.3.4/23")))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    }

    // valid ip address condition (ip address condition type is all internal/external)
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .setIpAddressConditionType(
                          IpAddressConditionType.IP_ADDRESS_CONDITION_TYPE_ALL_INTERNAL))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      DetectionExclusionCondition condition1 =
          DetectionExclusionCondition.newBuilder()
              .setIpAddressCondition(
                  IpAddressCondition.newBuilder()
                      .setIpAddressConditionType(
                          IpAddressConditionType.IP_ADDRESS_CONDITION_TYPE_ALL_EXTERNAL))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));
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
            () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
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
    assertDoesNotThrow(
        () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));
  }

  @Test
  void testIpOrganisationCondition() {
    DetectionExclusionCondition condition =
        DetectionExclusionCondition.newBuilder()
            .setIpOrganisationCondition(
                IpOrganisationCondition.newBuilder()
                    .setExclude(true)
                    .addAllIpOrganisationRegexes(List.of("][")))
            .build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // valid condition
    DetectionExclusionCondition condition1 =
        DetectionExclusionCondition.newBuilder()
            .setIpOrganisationCondition(
                IpOrganisationCondition.newBuilder()
                    .setExclude(true)
                    .addAllIpOrganisationRegexes(List.of(".*reg")))
            .build();
    assertDoesNotThrow(
        () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));
  }

  @Test
  void testIpAsnCondition() {
    DetectionExclusionCondition condition =
        DetectionExclusionCondition.newBuilder()
            .setIpAsnCondition(
                IpAsnCondition.newBuilder().setExclude(true).addAllIpAsnRegexes(List.of("][")))
            .build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // valid condition
    DetectionExclusionCondition condition1 =
        DetectionExclusionCondition.newBuilder()
            .setIpAsnCondition(
                IpAsnCondition.newBuilder().setExclude(true).addAllIpAsnRegexes(List.of(".*reg")))
            .build();
    assertDoesNotThrow(
        () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));
  }

  @Test
  void testIpAbuseVelocityCondition() {
    DetectionExclusionCondition condition =
        DetectionExclusionCondition.newBuilder()
            .setIpAbuseVelocityCondition(
                IpAbuseVelocityCondition.newBuilder()
                    .setMaxIpAbuseVelocity(IpAbuseVelocity.IP_ABUSE_VELOCITY_UNSPECIFIED))
            .build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // valid condition
    DetectionExclusionCondition condition1 =
        DetectionExclusionCondition.newBuilder()
            .setIpAbuseVelocityCondition(
                IpAbuseVelocityCondition.newBuilder()
                    .setMaxIpAbuseVelocity(IpAbuseVelocity.IP_ABUSE_VELOCITY_LOW))
            .build();
    assertDoesNotThrow(
        () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));
  }

  @Test
  void testRegionCondition() {
    DetectionExclusionCondition condition =
        DetectionExclusionCondition.newBuilder()
            .setRegionCondition(RegionCondition.getDefaultInstance())
            .build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    // valid condition
    DetectionExclusionCondition condition1 =
        DetectionExclusionCondition.newBuilder()
            .setRegionCondition(
                RegionCondition.newBuilder()
                    .setExclude(false)
                    .addRegions(RegionCondition.Region.newBuilder().setCountryIsoCode("iso")))
            .build();
    assertDoesNotThrow(
        () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));
  }

  @Test
  void testEmailDomainCondition() {
    DetectionExclusionCondition condition =
        DetectionExclusionCondition.newBuilder()
            .setEmailDomainCondition(
                EmailDomainCondition.newBuilder()
                    .setExclude(true)
                    .addAllEmailRegexes(List.of("e1", "e2"))
                    .addAllEmailDomains(List.of("r1", "r2")))
            .build();
    assertDoesNotThrow(
        () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));

    DetectionExclusionCondition condition1 =
        DetectionExclusionCondition.newBuilder()
            .setEmailDomainCondition(
                EmailDomainCondition.newBuilder()
                    .setExclude(true)
                    .addAllEmailRegexes(List.of("}{", "]["))
                    .addAllEmailDomains(List.of("e1", "e2")))
            .build();

    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));

    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    DetectionExclusionCondition condition2 =
        DetectionExclusionCondition.newBuilder()
            .setEmailDomainCondition(EmailDomainCondition.newBuilder().setExclude(true).build())
            .build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition2));

    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testUserAgentCondition() {
    DetectionExclusionCondition condition =
        DetectionExclusionCondition.newBuilder()
            .setUserAgentCondition(
                UserAgentCondition.newBuilder()
                    .setExclude(true)
                    .addAllUserAgents(List.of("u1", "u2"))
                    .addAllUserAgentRegexes(List.of(".*u1", ".*u2")))
            .build();
    assertDoesNotThrow(
        () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));

    DetectionExclusionCondition condition1 =
        DetectionExclusionCondition.newBuilder()
            .setUserAgentCondition(
                UserAgentCondition.newBuilder()
                    .setExclude(true)
                    .addAllUserAgents(List.of("u1", "u2"))
                    .addAllUserAgentRegexes(List.of("}{", "][")))
            .build();
    Throwable throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition1));

    Status status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());

    DetectionExclusionCondition condition2 =
        DetectionExclusionCondition.newBuilder()
            .setUserAgentCondition(UserAgentCondition.newBuilder().setExclude(true))
            .build();
    throwable =
        assertThrows(
            StatusRuntimeException.class,
            () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition2));

    status = Status.fromThrowable(throwable);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }

  @Test
  void testValidateRequestScannerTypeTypeCondition() {
    // condition not set
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setRequestScannerTypeCondition(RequestScannerTypeCondition.getDefaultInstance())
              .build();
      assertThrows(
          StatusRuntimeException.class,
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    }

    // invalid request scanner type condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setRequestScannerTypeCondition(
                  RequestScannerTypeCondition.newBuilder()
                      .addAllScannerTypes(List.of("", "Scanner1")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
      assertTrue(throwable.getMessage().contains("should not contain blank string "));
    }

    // valid condition
    {
      DetectionExclusionCondition condition =
          DetectionExclusionCondition.newBuilder()
              .setRequestScannerTypeCondition(
                  RequestScannerTypeCondition.newBuilder()
                      .addAllScannerTypes(List.of("Scanner1", "Scanner2")))
              .build();
      assertDoesNotThrow(
          () -> conditionValidator.validateRuleCondition(Collections.emptyList(), condition));
    }
  }
}
