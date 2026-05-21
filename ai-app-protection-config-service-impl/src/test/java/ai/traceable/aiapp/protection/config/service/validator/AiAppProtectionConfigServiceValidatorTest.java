package ai.traceable.aiapp.protection.config.service.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleToDelete;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleType;
import ai.traceable.aiapp.protection.config.service.v1.AiAppOotbSubRuleUpdate;
import ai.traceable.aiapp.protection.config.service.v1.AiAppRuleUpdate;
import ai.traceable.aiapp.protection.config.service.v1.CreateAiAppCustomRuleRequest;
import ai.traceable.aiapp.protection.config.service.v1.DeleteAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.ResetToDefault;
import ai.traceable.aiapp.protection.config.service.v1.RuleAction;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatusChange;
import ai.traceable.aiapp.protection.config.service.v1.UpdateAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.UpsertAiAppCustomRuleRequest;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AiAppProtectionConfigServiceValidatorTest {

  private AiAppProtectionConfigServiceValidator validator;

  @BeforeEach
  void setUp() {
    validator = new AiAppProtectionConfigServiceValidator();
  }

  @Nested
  @DisplayName("GetAiAppRulesRequest Validation")
  class GetAiAppRulesRequestValidation {

    @Test
    @DisplayName("Should pass validation with valid environment scope")
    void shouldPassValidationWithValidEnvironmentScope() {
      // Given
      EnvironmentScope environmentScope =
          EnvironmentScope.newBuilder()
              .addEnvironmentIds("env-1")
              .addEnvironmentIds("env-2")
              .build();

      RuleScope ruleScope = RuleScope.newBuilder().setEnvironmentScope(environmentScope).build();

      GetAiAppRulesRequest request =
          GetAiAppRulesRequest.newBuilder().setRuleScope(ruleScope).build();

      // When
      Status result = validator.validateGetAiAppRulesRequest(request);

      // Then
      assertTrue(result.isOk());
    }

    @Test
    @DisplayName("Should fail validation with empty rule scope")
    void shouldFailValidationWithEmptyRuleScope() {
      // Given
      RuleScope ruleScope = RuleScope.newBuilder().build();

      GetAiAppRulesRequest request =
          GetAiAppRulesRequest.newBuilder().setRuleScope(ruleScope).build();

      // When
      Status result = validator.validateGetAiAppRulesRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("Rule scope is required", result.getDescription());
    }

    @Test
    @DisplayName("Should fail validation with environment scope but empty environment IDs")
    void shouldFailValidationWithEmptyEnvironmentIds() {
      // Given
      EnvironmentScope environmentScope = EnvironmentScope.newBuilder().build();

      RuleScope ruleScope = RuleScope.newBuilder().setEnvironmentScope(environmentScope).build();

      GetAiAppRulesRequest request =
          GetAiAppRulesRequest.newBuilder().setRuleScope(ruleScope).build();

      // When
      Status result = validator.validateGetAiAppRulesRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("Environment IDs are required in scope", result.getDescription());
    }
  }

  @Nested
  @DisplayName("CreateAiAppCustomRuleRequest Validation")
  class CreateAiAppCustomRuleRequestValidation {

    @Test
    @DisplayName("Should fail validation when rule data is missing")
    void shouldFailValidationWhenRuleDataIsMissing() {
      // Given
      CreateAiAppCustomRuleRequest request = CreateAiAppCustomRuleRequest.newBuilder().build();

      // When
      Status result = validator.validateCreateAiAppCustomRuleRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("AI app custom rule data is required", result.getDescription());
    }

    @Test
    @DisplayName("Should fail validation when rule name is empty")
    void shouldFailValidationWhenRuleNameIsEmpty() {
      // Given
      AiAppCustomRuleData ruleData = AiAppCustomRuleData.newBuilder().setRuleName("").build();

      CreateAiAppCustomRuleRequest request =
          CreateAiAppCustomRuleRequest.newBuilder().setAiAppCustomRuleData(ruleData).build();

      // When
      Status result = validator.validateCreateAiAppCustomRuleRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("Rule name is required", result.getDescription());
    }

    @Test
    @DisplayName("Should fail validation when no rule type is specified")
    void shouldFailValidationWhenNoRuleTypeIsSpecified() {
      // Given
      AiAppCustomRuleData ruleData =
          AiAppCustomRuleData.newBuilder().setRuleName("Test Rule").build();

      CreateAiAppCustomRuleRequest request =
          CreateAiAppCustomRuleRequest.newBuilder().setAiAppCustomRuleData(ruleData).build();

      // When
      Status result = validator.validateCreateAiAppCustomRuleRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals(
          "Rule data must contain one of: PII detection, rate limiting, model governance, input explosion, or sensitive data protection configuration",
          result.getDescription());
    }
  }

  @Nested
  @DisplayName("UpsertAiAppCustomRuleRequest Validation")
  class UpsertAiAppCustomRuleRequestValidation {

    @Test
    @DisplayName("Should fail validation when custom rule is missing")
    void shouldFailValidationWhenCustomRuleIsMissing() {
      // Given
      UpsertAiAppCustomRuleRequest request = UpsertAiAppCustomRuleRequest.newBuilder().build();

      // When
      Status result = validator.validateUpsertAiAppCustomRuleRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("AI app custom rule is required", result.getDescription());
    }

    @Test
    @DisplayName("Should fail validation when rule ID is empty")
    void shouldFailValidationWhenRuleIdIsEmpty() {
      // Given
      AiAppCustomRule customRule = AiAppCustomRule.newBuilder().setRuleId("").build();

      UpsertAiAppCustomRuleRequest request =
          UpsertAiAppCustomRuleRequest.newBuilder().setAiAppCustomRule(customRule).build();

      // When
      Status result = validator.validateUpsertAiAppCustomRuleRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("Rule ID is required for upsert operation", result.getDescription());
    }

    @Test
    @DisplayName("Should fail validation when rule data is missing")
    void shouldFailValidationWhenRuleDataIsMissing() {
      // Given
      AiAppCustomRule customRule = AiAppCustomRule.newBuilder().setRuleId("rule-123").build();

      UpsertAiAppCustomRuleRequest request =
          UpsertAiAppCustomRuleRequest.newBuilder().setAiAppCustomRule(customRule).build();

      // When
      Status result = validator.validateUpsertAiAppCustomRuleRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("AI app custom rule data is required", result.getDescription());
    }
  }

  @Nested
  @DisplayName("UpdateAiAppRulesRequest Validation")
  class UpdateAiAppRulesRequestValidation {

    @Test
    @DisplayName("Should pass validation with valid rule updates")
    void shouldPassValidationWithValidRuleUpdates() {
      // Given
      EnvironmentScope environmentScope =
          EnvironmentScope.newBuilder().addEnvironmentIds("env-1").build();

      RuleScope ruleScope = RuleScope.newBuilder().setEnvironmentScope(environmentScope).build();

      AiAppRuleUpdate ruleUpdate =
          AiAppRuleUpdate.newBuilder()
              .setRuleId("rule-123")
              .setRuleStatusChange(RuleStatusChange.newBuilder().setDisabled(true).build())
              .addOotbSubRuleUpdates(
                  AiAppOotbSubRuleUpdate.newBuilder()
                      .setRuleId("sub-rule-1")
                      .setRuleAction(RuleAction.RULE_ACTION_MONITOR)
                      .build())
              .build();

      UpdateAiAppRulesRequest request =
          UpdateAiAppRulesRequest.newBuilder()
              .setRuleScope(ruleScope)
              .addAiAppRuleUpdates(ruleUpdate)
              .build();

      // When
      Status result = validator.validateUpdateAiAppRulesRequest(request);

      // Then
      assertTrue(result.isOk());
    }

    @Test
    @DisplayName("Should fail validation with invalid rule scope")
    void shouldFailValidationWithInvalidRuleScope() {
      // Given
      RuleScope ruleScope = RuleScope.newBuilder().build();

      UpdateAiAppRulesRequest request =
          UpdateAiAppRulesRequest.newBuilder().setRuleScope(ruleScope).build();

      // When
      Status result = validator.validateUpdateAiAppRulesRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("Rule scope is required", result.getDescription());
    }

    @Test
    @DisplayName("Should fail validation when rule update has empty rule ID")
    void shouldFailValidationWhenRuleUpdateHasEmptyRuleId() {
      // Given
      EnvironmentScope environmentScope =
          EnvironmentScope.newBuilder().addEnvironmentIds("env-1").build();

      RuleScope ruleScope = RuleScope.newBuilder().setEnvironmentScope(environmentScope).build();

      AiAppRuleUpdate ruleUpdate = AiAppRuleUpdate.newBuilder().setRuleId("").build();

      UpdateAiAppRulesRequest request =
          UpdateAiAppRulesRequest.newBuilder()
              .setRuleScope(ruleScope)
              .addAiAppRuleUpdates(ruleUpdate)
              .build();

      // When
      Status result = validator.validateUpdateAiAppRulesRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("Rule ID is required for update operation", result.getDescription());
    }
  }

  @Nested
  @DisplayName("DeleteAiAppRulesRequest Validation")
  class DeleteAiAppRulesRequestValidation {

    @Test
    @DisplayName("Should pass validation with reset to default")
    void shouldPassValidationWithResetToDefault() {
      // Given
      ResetToDefault resetToDefault =
          ResetToDefault.newBuilder()
              .addRuleIds("rule-1")
              .addRuleIds("rule-2")
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addEnvironmentIds("env-1").build())
                      .build())
              .build();

      DeleteAiAppRulesRequest request =
          DeleteAiAppRulesRequest.newBuilder().setResetToDefault(resetToDefault).build();

      // When
      Status result = validator.validateDeleteAiAppRulesRequest(request);

      // Then
      assertTrue(result.isOk());
    }

    @Test
    @DisplayName("Should pass validation with custom rule to delete")
    void shouldPassValidationWithCustomRuleToDelete() {
      // Given
      AiAppCustomRuleToDelete customRuleToDelete =
          AiAppCustomRuleToDelete.newBuilder()
              .setRuleId("rule-123")
              .setCustomRuleType(AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_PII_DETECTED_IN_PROMPT)
              .build();

      DeleteAiAppRulesRequest request =
          DeleteAiAppRulesRequest.newBuilder().setCustomRuleToDelete(customRuleToDelete).build();

      // When
      Status result = validator.validateDeleteAiAppRulesRequest(request);

      // Then
      assertTrue(result.isOk());
    }

    @Test
    @DisplayName("Should fail validation when no delete option is specified")
    void shouldFailValidationWhenNoDeleteOptionIsSpecified() {
      // Given
      DeleteAiAppRulesRequest request = DeleteAiAppRulesRequest.newBuilder().build();

      // When
      Status result = validator.validateDeleteAiAppRulesRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("Delete option must be specified", result.getDescription());
    }

    @Test
    @DisplayName("Should fail validation when custom rule to delete has empty rule ID")
    void shouldFailValidationWhenCustomRuleToDeleteHasEmptyRuleId() {
      // Given
      AiAppCustomRuleToDelete customRuleToDelete =
          AiAppCustomRuleToDelete.newBuilder()
              .setRuleId("")
              .setCustomRuleType(AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_PII_DETECTED_IN_PROMPT)
              .build();

      DeleteAiAppRulesRequest request =
          DeleteAiAppRulesRequest.newBuilder().setCustomRuleToDelete(customRuleToDelete).build();

      // When
      Status result = validator.validateDeleteAiAppRulesRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("Rule ID is required for custom rule deletion", result.getDescription());
    }

    @Test
    @DisplayName("Should fail validation when custom rule to delete has unspecified rule type")
    void shouldFailValidationWhenCustomRuleToDeleteHasUnspecifiedRuleType() {
      // Given
      AiAppCustomRuleToDelete customRuleToDelete =
          AiAppCustomRuleToDelete.newBuilder()
              .setRuleId("rule-123")
              .setCustomRuleType(AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_UNSPECIFIED)
              .build();

      DeleteAiAppRulesRequest request =
          DeleteAiAppRulesRequest.newBuilder().setCustomRuleToDelete(customRuleToDelete).build();

      // When
      Status result = validator.validateDeleteAiAppRulesRequest(request);

      // Then
      assertFalse(result.isOk());
      assertEquals(Status.Code.INVALID_ARGUMENT, result.getCode());
      assertEquals("Custom rule type must be specified for deletion", result.getDescription());
    }
  }
}
