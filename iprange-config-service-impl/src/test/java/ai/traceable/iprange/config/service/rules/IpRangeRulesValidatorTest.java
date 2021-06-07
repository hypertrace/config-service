package ai.traceable.iprange.config.service.rules;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.iprange.config.service.v1.*;
import io.grpc.Status;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class IpRangeRulesValidatorTest {
  private IpRangeRulesValidator rulesValidator;

  @BeforeEach
  void setUp() {
    this.rulesValidator = new IpRangeRulesValidator();
  }

  @Nested
  class ValidateCreateIpRangeRuleRequest {
    @Test
    @DisplayName("Should return OK status when a given valid create IP Range rule")
    void validateCreateIpRangeRuleRequest_correct_1() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status = rulesValidator.validate(createIpRangeRuleRequest);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return OK status when a given valid create IP Range rule")
    void validateCreateIpRangeRuleRequest_correct_2() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status = rulesValidator.validate(createIpRangeRuleRequest);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when missing name in create IP Range rule")
    void validateCreateIpRangeRuleRequest_missing_name() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status = rulesValidator.validate(createIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when missing name in create IP Range rule")
    void validateCreateIpRangeRuleRequest_missing_ip_range() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Test")
              .setDescription("Range rule test")
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status = rulesValidator.validate(createIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when rule action not specified")
    void validateCreateIpRangeRuleRequest_invalid_rule_action() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Test")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.1.1.1/16"))
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status = rulesValidator.validate(createIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when expiration duration is given in wrong format")
    void validateCreateIpRangeRuleRequest_invalid_expiration_duration() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("30").build())
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status = rulesValidator.validate(createIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }
  }

  @Nested
  class ValidateUpdateIpRangeRuleRequest {
    @Test
    @DisplayName("Should return OK status when a given valid update IP Range rule")
    void validateUpdateIpRangeRuleRequest_correct_1() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("Tester0")
              .setRuleDetails(iprangeRuleDetails)
              .setDisabled(true)
              .setInternal(false)
              .build();

      Status status = rulesValidator.validate(updateIpRangeRuleRequest);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return OK status when a given valid update IP Range rule")
    void validateUpdateIpRangeRuleRequest_correct_2() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("Tester0")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      Status status = rulesValidator.validate(updateIpRangeRuleRequest);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when missing id in update IP Range rule")
    void validateUpdateIpRangeRuleRequest_missing_id() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status = rulesValidator.validate(updateIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when missing name in update IP Range rule")
    void validateUpdateIpRangeRuleRequest_missing_name() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("tester")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      Status status = rulesValidator.validate(updateIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when missing Ip Ranges in update IP Range rule")
    void validateUpdateIpRangeRuleRequest_missing_ip_range() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Test")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList())
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("tester")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      Status status = rulesValidator.validate(updateIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when rule action not specified")
    void validateUpdateIpRangeRuleRequest_invalid_rule_action() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Test")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.1.1.1/16"))
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("tester")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      Status status = rulesValidator.validate(updateIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when expiration duration is given in wrong format")
    void validateUpdateIpRangeRuleRequest_invalid_expiration_duration() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("345").build())
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("Tester0")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      Status status = rulesValidator.validate(updateIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }
  }

  @Nested
  class ValidateDeleteIpRangeRuleRequest {
    @Test
    @DisplayName("Should return OK  status when a given valid delete IP Range rule")
    void validateDeleteIpRangeRuleRequest_correct() {
      DeleteIpRangeRuleRequest deleteIpRangeRuleRequest =
          DeleteIpRangeRuleRequest.newBuilder().setId("Tester0").build();

      Status status = rulesValidator.validate(deleteIpRangeRuleRequest);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when missing id in delete IP Range rule")
    void validateDeleteIpRangeRuleRequest_missing_id() {
      DeleteIpRangeRuleRequest deleteIpRangeRuleRequest =
          DeleteIpRangeRuleRequest.newBuilder().build();

      Status status = rulesValidator.validate(deleteIpRangeRuleRequest);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }
  }
}
