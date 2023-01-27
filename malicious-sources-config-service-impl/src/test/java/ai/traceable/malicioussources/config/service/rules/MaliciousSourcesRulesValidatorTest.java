package ai.traceable.malicioussources.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.EmailDomainCondition;
import ai.traceable.malicioussources.config.service.v1.EmailFraudScore;
import ai.traceable.malicioussources.config.service.v1.EmailFraudScoreLevel;
import ai.traceable.malicioussources.config.service.v1.EnvironmentScope;
import ai.traceable.malicioussources.config.service.v1.EventSeverity;
import ai.traceable.malicioussources.config.service.v1.ExpirationDetails;
import ai.traceable.malicioussources.config.service.v1.IpAddressCondition;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.IpReputationCondition;
import ai.traceable.malicioussources.config.service.v1.IpReputationSeverity;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleStatus;
import ai.traceable.malicioussources.config.service.v1.Region;
import ai.traceable.malicioussources.config.service.v1.RegionCondition;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MaliciousSourcesRulesValidatorTest {
  @Mock private Supplier<List<MaliciousSourcesRule>> blockAllExceptRulesSupplier;
  @InjectMocks private MaliciousSourcesRulesValidator rulesValidator;

  @Nested
  class ValidateCreateMaliciousSourcesRuleRequest {
    @Test
    @DisplayName("Should return OK status when a given valid create Malicious Sources rule")
    void validateCreateMaliciousSourcesRuleRequest_correct_1() {
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
                              .setExpirationTimestamp(Timestamp.newBuilder().setSeconds(20).build())
                              .build())
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpLocationTypeCondition(
                          IpLocationTypeCondition.newBuilder()
                              .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN)
                              .build())
                      .build())
              .build();
      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleScope(
                  MaliciousSourcesRuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder()
                              .addAllEnvironmentIds(List.of("e1", "e2"))
                              .build())
                      .build())
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return OK status when a given valid create MaliciousSources rule")
    void validateCreateMaliciousSourcesRuleRequest_correct_2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder().setMinIpReputationScore(100).build())
                      .build())
              .build();
      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to unspecified rule action type")
    void validateCreateMaliciousSourcesRuleRequest_rule_action_type_unspecified() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_UNSPECIFIED)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder().setMinIpReputationScore(100).build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to ALERT action not having a valid severity")
    void validateCreateMaliciousSourcesRuleRequest_rule_action_type() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALERT)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_UNSPECIFIED)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder().setMinIpReputationScore(100).build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to unspecified ip location type")
    void validateCreateMaliciousSourcesRuleRequest_ip_location1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpLocationTypeCondition(
                          IpLocationTypeCondition.newBuilder()
                              .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_UNSPECIFIED)
                              .build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to empty ip location type")
    void validateCreateMaliciousSourcesRuleRequest_ip_location2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpLocationTypeCondition(IpLocationTypeCondition.newBuilder().build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return not found as email fraud score is empty")
    void validateCreateMaliciousSourcesRuleRequest_email_fraud_score1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(
                          EmailDomainCondition.newBuilder()
                              .setEmailFraudScore(EmailFraudScore.getDefaultInstance())))
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to unspecified email fraud score level")
    void validateCreateMaliciousSourcesRuleRequest_email_fraud_score2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(
                          EmailDomainCondition.newBuilder()
                              .setEmailFraudScore(
                                  EmailFraudScore.newBuilder()
                                      .setMinEmailFraudScoreLevel(
                                          EmailFraudScoreLevel
                                              .EMAIL_FRAUD_SCORE_LEVEL_UNSPECIFIED))))
              .build();
      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to min email fraud score <=0")
    void validateCreateMaliciousSourcesRuleRequest_email_fraud_score3() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(
                          EmailDomainCondition.newBuilder()
                              .setEmailFraudScore(
                                  EmailFraudScore.newBuilder().setMinEmailFraudScore(-5))))
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to invalid email regex")
    void validateCreateMaliciousSourcesRuleRequest_email_fraud_score4() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(
                          EmailDomainCondition.newBuilder().addEmailRegexes("in**valid")))
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found email domain condition cannot be empty")
    void validateCreateMaliciousSourcesRuleRequest_email_fraud_score5() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(EmailDomainCondition.newBuilder()))
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return not found as ip reputation severity is empty")
    void validateCreateMaliciousSourcesRuleRequest_ip_reputation1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(IpReputationCondition.newBuilder().build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to unspecified ip reputation severity")
    void validateCreateMaliciousSourcesRuleRequest_ip_reputation2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder()
                              .setMinIpReputationSeverity(
                                  IpReputationSeverity.IP_REPUTATION_SEVERITY_UNSPECIFIED)
                              .build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to min ip reputation score <=0")
    void validateCreateMaliciousSourcesRuleRequest_ip_reputation3() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder().setMinIpReputationScore(-5).build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found as region condition have empty region list")
    void validateCreateMaliciousSourcesRuleRequest_region_condition1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setRegionCondition(RegionCondition.newBuilder().build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument as region does not have a valid ISO code")
    void validateCreateMaliciousSourcesRuleRequest_region_condition2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setRegionCondition(
                          RegionCondition.newBuilder()
                              .addRegions(Region.newBuilder().setCountryIsoCode("QQQ").build())
                              .build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found due to no ip Address")
    void validateCreateMaliciousSourcesRuleRequest_ip_range1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(IpAddressCondition.newBuilder().build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to invalid ip address")
    void validateCreateMaliciousSourcesRuleRequest_ip_range2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder()
                              .addIpAddresses("2555.2555.2555.0")
                              .build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();
      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to invalid CIDR address")
    void validateCreateMaliciousSourcesRuleRequest_ip_range3() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder()
                              .addCidrIpRanges("255.255.255.0/100")
                              .build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();
      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found due to no rule condition")
    void validateCreateMaliciousSourcesRuleRequest_ip_rule_condition() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due unspecified condition")
    void validateCreateMaliciousSourcesRuleRequest_valid_rule_condition() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .addConditions(MaliciousSourcesRuleCondition.getDefaultInstance())
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid status because of empty env id list")
    void validateCreateMaliciousSourcesRuleRequest_empty_env_scope() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleScope(
                  MaliciousSourcesRuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of()))
                      .build())
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid status because of empty env id")
    void validateCreateMaliciousSourcesRuleRequest_empty_env_id() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleScope(
                  MaliciousSourcesRuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("")))
                      .build())
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when missing name in create Malicious Sources rule")
    void validateCreateMaliciousSourcesRuleRequest_missing_name() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when same name Malicious Sources rule exist")
    void validateCreateMaliciousSourcesRuleRequest_same_name() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test With Same Name")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALERT)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo1 =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test With Same Name Exist")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                      .build())
              .build();

      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo1)
              .build();

      Status status =
          rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of(maliciousSourcesRule));
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return ALREADY_EXISTS status when creating a duplicate block all except rule")
    void validateCreateMaliciousSourcesRuleRequest_incorrect_duplicateBlockAllExcept() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo1 =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-2")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();
      MaliciousSourcesRule rule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();
      CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
          CreateMaliciousSourcesRuleRequest.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo1)
              .build();
      Status status = rulesValidator.validate(createMaliciousSourcesRuleRequest, List.of(rule));
      assertEquals(Status.Code.ALREADY_EXISTS, status.getCode());
    }
  }

  @Nested
  class ValidateUpdateMaliciousSourcesRuleRequest {
    @Test
    @DisplayName("Should return OK status when a given valid update Malicious Sources rule")
    void validateUpdateMaliciousSourcesRuleRequest_correct_1() {
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
                              .setExpirationTimestamp(Timestamp.newBuilder().setSeconds(20).build())
                              .build())
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpLocationTypeCondition(
                          IpLocationTypeCondition.newBuilder()
                              .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN)
                              .build())
                      .build())
              .build();
      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(MaliciousSourcesRuleStatus.newBuilder().setInternal(false).build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return OK status when a given valid update Malicious Sources rule")
    void validateUpdateMaliciousSourcesRuleRequest_correct_2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();
      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when missing id in update Malicious Sources rule")
    void validateUpdateMaliciousSourcesRuleRequest_missing_id() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to unspecified rule action type")
    void validateUpdateMaliciousSourcesRuleRequest_rule_action_type_unspecified() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_UNSPECIFIED)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder().setMinIpReputationScore(100).build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();

      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to unspecified ip location type")
    void validateUpdateMaliciousSourcesRuleRequest_ip_location1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpLocationTypeCondition(
                          IpLocationTypeCondition.newBuilder()
                              .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_UNSPECIFIED)
                              .build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to empty ip location type list")
    void validateUpdateMaliciousSourcesRuleRequest_ip_location2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpLocationTypeCondition(IpLocationTypeCondition.newBuilder().build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to ALERT action not having a valid severity")
    void validateUpdateMaliciousSourcesRuleRequest_rule_action_type() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALERT)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_UNSPECIFIED)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder().setMinIpReputationScore(100).build())
                      .build())
              .build();
      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found as email fraud score is empty")
    void validateUpdateMaliciousSourcesRuleRequest_email_fraud_score1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(
                          EmailDomainCondition.newBuilder()
                              .setEmailFraudScore(EmailFraudScore.getDefaultInstance())))
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder().setInternal(false).setDisabled(true))
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to unspecified email fraud score level ")
    void validateUpdateMaliciousSourcesRuleRequest_email_fraud_score2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(
                          EmailDomainCondition.newBuilder()
                              .setEmailFraudScore(
                                  EmailFraudScore.newBuilder()
                                      .setMinEmailFraudScoreLevel(
                                          EmailFraudScoreLevel
                                              .EMAIL_FRAUD_SCORE_LEVEL_UNSPECIFIED))))
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder().setInternal(false).setDisabled(true))
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found due to min email fraud score <=0")
    void validateUpdateMaliciousSourcesRuleInfoRequest_email_fraud_score3() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(
                          EmailDomainCondition.newBuilder()
                              .setEmailFraudScore(
                                  EmailFraudScore.newBuilder().setMinEmailFraudScore(-5))))
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder().setInternal(false).setDisabled(true))
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument as email regex is not valid")
    void validateUpdateMaliciousSourcesRuleRequest_email_fraud_score4() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(
                          EmailDomainCondition.newBuilder().addEmailRegexes("in**valid")))
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder().setInternal(false).setDisabled(true))
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found as email domain condition cannot be empty")
    void validateUpdateMaliciousSourcesRuleRequest_email_fraud_score5() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW))
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setEmailDomainCondition(
                          EmailDomainCondition.newBuilder()
                              .setDisposableEmailDomain(false)
                              .setDataLeakedEmail(false)))
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder().setInternal(false).setDisabled(true))
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return not found as ip reputation is empty")
    void validateUpdateMaliciousSourcesRuleRequest_ip_reputation1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(IpReputationCondition.newBuilder().build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to unspecified ip reputation severity")
    void validateUpdateMaliciousSourcesRuleRequest_ip_reputation2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder()
                              .setMinIpReputationSeverity(
                                  IpReputationSeverity.IP_REPUTATION_SEVERITY_UNSPECIFIED)
                              .build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found as region condition have empty region list")
    void validateUpdateMaliciousSourcesRuleInfoRequest_region_condition1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setRegionCondition(RegionCondition.newBuilder().build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument as region does not have a valid iso code")
    void validateUpdateMaliciousSourcesRuleInfoRequest_region_condition2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setRegionCondition(
                          RegionCondition.newBuilder()
                              .addRegions(Region.newBuilder().setCountryIsoCode("QQQ").build())
                              .build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found due to min ip reputation score <=0")
    void validateUpdateMaliciousSourcesRuleInfoRequest_ip_reputation3() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpReputationCondition(
                          IpReputationCondition.newBuilder().setMinIpReputationScore(-5).build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found due to no ip Address")
    void validateUpdateMaliciousSourcesRuleRequest_ip_range1() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(IpAddressCondition.newBuilder().build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to invalid ip address")
    void validateUpdateMaliciousSourcesRuleRequest_ip_range2() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder()
                              .addIpAddresses("2555.2555.2555.0")
                              .build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to invalid CIDR address")
    void validateUpdateMaliciousSourcesRuleRequest_ip_range3() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder()
                              .addCidrIpRanges("255.255.255.0/100")
                              .build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return not found due to no rule condition")
    void validateUpdateMaliciousSourcesRuleRequest_ip_rule_condition() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument due to unspecified rule condition")
    void validateUpdateMaliciousSourcesRuleRequest_rule_condition() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .addConditions(MaliciousSourcesRuleCondition.getDefaultInstance())
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid status because of empty env id list")
    void validateUpdateMaliciousSourcesRuleRequest_empty_env_scope() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleScope(
                  MaliciousSourcesRuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of()))
                      .build())
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid status because of empty env id")
    void validateUpdateMaliciousSourcesRuleRequest_empty_env_id() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleScope(
                  MaliciousSourcesRuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("")))
                      .build())
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when missing name in update Malicious Sources rule")
    void validateUpdateMaliciousSourcesRuleRequest_missing_name() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALLOW)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule).build();
      Status status = rulesValidator.validate(updateMaliciousSourcesRuleRequest, List.of());
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when same name exist on update Malicious Sources rule")
    void validateUpdateMaliciousSourcesRuleRequest_same_name() {

      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test with Same Name")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_ALERT)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL)
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo1 =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-2")
              .setDescription("Malicious Sources Rule Test with Same Name")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_LOW)
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule1 =
          MaliciousSourcesRule.newBuilder()
              .setId("Second-test")
              .setRuleInfo(maliciousSourcesRuleInfo1)
              .build();

      MaliciousSourcesRule maliciousSourcesRule2 =
          MaliciousSourcesRule.newBuilder()
              .setId("Second-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .build();

      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule2).build();

      Status status =
          rulesValidator.validate(
              updateMaliciousSourcesRuleRequest,
              List.of(maliciousSourcesRule, maliciousSourcesRule1));

      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return ALREADY_EXISTS status when creating a duplicate block all except rule")
    void validateUpdateMaliciousSourcesRuleRequest_incorrect_duplicateBlockAllExcept() {
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-1")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule =
          MaliciousSourcesRule.newBuilder()
              .setId("First-test")
              .setRuleInfo(maliciousSourcesRuleInfo)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo1 =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-2")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo2 =
          MaliciousSourcesRuleInfo.newBuilder()
              .setName("Tester-3")
              .setDescription("Malicious Sources Rule Test")
              .setRuleAction(
                  MaliciousSourcesRuleAction.newBuilder()
                      .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
                      .build())
              .addConditions(
                  MaliciousSourcesRuleCondition.newBuilder()
                      .setIpRangeCondition(
                          IpAddressCondition.newBuilder().addIpAddresses("255.255.255.0").build())
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule1 =
          MaliciousSourcesRule.newBuilder()
              .setId("Second-test")
              .setRuleInfo(maliciousSourcesRuleInfo1)
              .setRuleStatus(
                  MaliciousSourcesRuleStatus.newBuilder()
                      .setInternal(false)
                      .setDisabled(true)
                      .build())
              .build();

      MaliciousSourcesRule maliciousSourcesRule2 =
          MaliciousSourcesRule.newBuilder()
              .setId("Second-test")
              .setRuleInfo(maliciousSourcesRuleInfo2)
              .build();

      UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
          UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(maliciousSourcesRule2).build();

      Status status =
          rulesValidator.validate(
              updateMaliciousSourcesRuleRequest,
              List.of(maliciousSourcesRule, maliciousSourcesRule1));
      assertEquals(Status.Code.ALREADY_EXISTS, status.getCode());
    }
  }

  @Nested
  class ValidateDeleteMaliciousSourcesRuleRequest {
    @Test
    @DisplayName("Should return OK  status when a given valid delete Malicious Sources rule")
    void validateDeleteMaliciousSourcesRuleRequest_correct() {
      DeleteMaliciousSourcesRuleRequest deleteMaliciousSourcesRuleRequest =
          DeleteMaliciousSourcesRuleRequest.newBuilder().setId("Tester0").build();

      Status status = rulesValidator.validate(deleteMaliciousSourcesRuleRequest);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when missing id in delete Malicious sources rule")
    void validateDeleteIpRangeRuleRequest_missing_id() {
      DeleteMaliciousSourcesRuleRequest deleteMaliciousSourcesRuleRequest =
          DeleteMaliciousSourcesRuleRequest.newBuilder().build();

      Status status = rulesValidator.validate(deleteMaliciousSourcesRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }
  }
}
