package ai.traceable.iprange.config.service.rules;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.iprange.config.service.v1.*;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class IpRangeRulesValidatorTest {
  private IpRangeRulesValidator rulesValidator;
  private Supplier<List<IpRangeRule>> blockAllExceptRulesSupplier;

  @BeforeEach
  void setUp() {
    this.rulesValidator = new IpRangeRulesValidator();
    this.blockAllExceptRulesSupplier = Mockito.mock(Supplier.class);
    Mockito.when(blockAllExceptRulesSupplier.get()).thenReturn(Collections.emptyList());
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
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder()
              .setRuleDetails(iprangeRuleDetails)
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("e1", "e2")))
                      .build())
              .build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return OK status when a given valid create IP Range rule")
    void validateCreateIpRangeRuleRequest_correct_2() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setConditions(
                  IpRangeRuleConditions.newBuilder()
                      .setStatusCodeMatchCondition(
                          StatusCodeMatchCondition.newBuilder()
                              .setMatchType(
                                  StatusCodeMatchType.STATUS_CODE_MATCH_TYPE_MATCHES_REGEX)
                              .setMatchValue("^a")
                              .build()))
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid status due to bad regex")
    void validateIpRangeRuleDetailsRequest_invalid_regex() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setConditions(
                  IpRangeRuleConditions.newBuilder()
                      .setStatusCodeMatchCondition(
                          StatusCodeMatchCondition.newBuilder()
                              .setMatchType(
                                  StatusCodeMatchType.STATUS_CODE_MATCH_TYPE_MATCHES_REGEX)
                              .setMatchValue("*")
                              .build()))
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid status because of empty env id list")
    void validateCreateIpRangeRuleRequest_empty_env_scope() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder()
              .setRuleDetails(iprangeRuleDetails)
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of()))
                      .build())
              .build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid status because of empty env id")
    void validateCreateIpRangeRuleRequest_empty_env_id() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder()
              .setRuleDetails(iprangeRuleDetails)
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("")))
                      .build())
              .build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when missing name in create IP Range rule")
    void validateCreateIpRangeRuleRequest_missing_name() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
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

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when rule action not specified")
    void validateCreateIpRangeRuleRequest_invalid_rule_action() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Test")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.1.0.0/16"))
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when severity not specified")
    void validateCreateIpRangeRuleRequest_invalid_severity() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Test")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      // severity not required for any action other than alert
      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.OK, status.getCode());

      createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder()
              .setRuleDetails(
                  iprangeRuleDetails.toBuilder().setRuleAction(RuleAction.RULE_ACTION_ALERT))
              .build();

      status = rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
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
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("30").build())
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return OK status when first rule of type block all except")
    void validateCreateIpRangeRuleRequest_correct_firstBlockAllExcept() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT)
              .build();

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return ALREADY_EXISTS status when creating a duplicate block all except rule")
    void validateCreateIpRangeRuleRequest_incorrect_duplicateBlockAllExcept() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT)
              .build();
      Mockito.when(blockAllExceptRulesSupplier.get())
          .thenReturn(List.of(IpRangeRule.getDefaultInstance()));

      CreateIpRangeRuleRequest createIpRangeRuleRequest =
          CreateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status =
          rulesValidator.validate(createIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Code.ALREADY_EXISTS, status.getCode());
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
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
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
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("e1", "e2")))
                      .build())
              .build();

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return OK status when a given valid update IP Range rule")
    void validateUpdateIpRangeRuleRequest_correct_2() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("Tester0")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when missing id in update IP Range rule")
    void validateUpdateIpRangeRuleRequest_missing_id() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder().setRuleDetails(iprangeRuleDetails).build();

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when env list is empty")
    void validateUpdateIpRangeRuleRequest_empty_env_scope() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
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
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of()))
                      .build())
              .build();

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when env id is empty")
    void validateUpdateIpRangeRuleRequest_empty_env_id() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
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
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("")))
                      .build())
              .build();

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when missing name in update IP Range rule")
    void validateUpdateIpRangeRuleRequest_missing_name() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("tester")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
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

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when rule action not specified")
    void validateUpdateIpRangeRuleRequest_invalid_rule_action() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Test")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.1.0.0/16"))
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("tester")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("Should return invalid argument status when severity not specified")
    void validateUpdateIpRangeRuleRequest_invalid_severity() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Test")
              .setDescription("Range rule test")
              .addAllRawInputIpData(Arrays.asList("1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("tester")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      // severity not required for any action other than alert
      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.OK, status.getCode());

      updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("tester")
              .setRuleDetails(
                  iprangeRuleDetails.toBuilder().setRuleAction(RuleAction.RULE_ACTION_ALERT))
              .build();

      status = rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return invalid argument status when expiration duration is given in wrong format")
    void validateUpdateIpRangeRuleRequest_invalid_expiration_duration() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
              .setExpirationDetails(
                  ExpirationDetails.newBuilder().setExpirationDuration("345").build())
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("Tester0")
              .setRuleDetails(iprangeRuleDetails)
              .build();

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return OK status when trying to update rule with action already as block all except")
    void validateUpdateIpRangeRuleRequest_correct_sameRuleId() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("Tester0")
              .setRuleDetails(iprangeRuleDetails)
              .build();
      Mockito.when(blockAllExceptRulesSupplier.get())
          .thenReturn(List.of(IpRangeRule.newBuilder().setId("Tester0").build()));

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return ALREADY_EXISTS status when trying to update rule with action block all except, "
            + "and rule of such action already exists")
    void validateUpdateIpRangeRuleRequest_correct_differentRuleId() {
      IpRangeRuleDetails iprangeRuleDetails =
          IpRangeRuleDetails.newBuilder()
              .setName("Tester")
              .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.0.0/16"))
              .setRuleAction(RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT)
              .build();

      UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
          UpdateIpRangeRuleRequest.newBuilder()
              .setId("Tester0")
              .setRuleDetails(iprangeRuleDetails)
              .build();
      Mockito.when(blockAllExceptRulesSupplier.get())
          .thenReturn(List.of(IpRangeRule.newBuilder().setId("Tester1").build()));

      Status status =
          rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
      assertEquals(Code.ALREADY_EXISTS, status.getCode());
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

  @Test
  @DisplayName("Should return invalid argument status when host bits are not zero in a cidr")
  void validateRawInputIpAddressesTest() {
    IpRangeRuleDetails iprangeRuleDetails =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester")
            .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.0/16"))
            .setRuleAction(RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT)
            .build();
    UpdateIpRangeRuleRequest updateIpRangeRuleRequest =
        UpdateIpRangeRuleRequest.newBuilder()
            .setId("Tester0")
            .setRuleDetails(iprangeRuleDetails)
            .build();
    Mockito.when(blockAllExceptRulesSupplier.get())
        .thenReturn(List.of(IpRangeRule.getDefaultInstance()));

    Status status = rulesValidator.validate(updateIpRangeRuleRequest, blockAllExceptRulesSupplier);
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }
}
