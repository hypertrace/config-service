package ai.traceable.region.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.GetRegionRequest;
import ai.traceable.region.config.service.v1.RegionIdentifier;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.RegionsFilter;
import ai.traceable.region.config.service.v1.RuleScope;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RegionRulesValidatorTest {
  private RegionRulesValidator rulesValidator;
  private Supplier<List<RegionRule>> getAllRegionRulesSupplier;

  @BeforeEach
  void setup() {
    this.rulesValidator = new RegionRulesValidator();
    getAllRegionRulesSupplier = mock(Supplier.class);
    when(getAllRegionRulesSupplier.get()).thenReturn(Collections.emptyList());
  }

  @Nested
  class ValidateCreateRegionRuleRequest {
    @Test
    @DisplayName("should return invalid argument status missing region ids")
    void should_fail_createRegionRule_missingRegionIds() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      Status status = rulesValidator.validate(createRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should return invalid argument unspecified action type")
    void should_fail_createRegionRule_invalidActionType() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder().addRegionId("region-1").setName("name").build();

      Status status = rulesValidator.validate(createRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should return invalid argument unspecified severity")
    void should_fail_createRegionRule_invalidSeverity() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      // severity not required for any action other than alert
      Status status = rulesValidator.validate(createRegionRuleRequest, getAllRegionRulesSupplier);
      assertEquals(Code.OK, status.getCode());

      createRegionRuleRequest =
          createRegionRuleRequest.toBuilder()
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALERT)
              .build();

      status = rulesValidator.validate(createRegionRuleRequest, getAllRegionRulesSupplier);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should return invalid argument invalid name")
    void should_fail_createRegionRule_invalidName() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      Status status = rulesValidator.validate(createRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should return ok for first block all except action type")
    void should_pass_createRegionRule_firstBlockAllExcept() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setName("name")
              .build();

      Status status = rulesValidator.validate(createRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.OK, status.getCode());
    }

    @Test
    @DisplayName("should fail as empty list in env scope")
    void should_fail_createRegionRule_empty_env_scope() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setName("name")
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of()))
                      .build())
              .build();

      Status status = rulesValidator.validate(createRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should fail as empty id in env scope")
    void should_fail_createRegionRule_empty_env_id() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setName("name")
              .setRuleScope(
                  RuleScope.newBuilder()
                      .setEnvironmentScope(
                          EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("")))
                      .build())
              .build();

      Status status = rulesValidator.validate(createRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should return ALREADY_EXISTS for duplicate block all except action type")
    void should_pass_createRegionRule_duplicateBlockAllExcept() {
      CreateRegionRuleRequest createRegionRuleRequest =
          CreateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setName("name")
              .build();
      when(getAllRegionRulesSupplier.get())
          .thenReturn(
              List.of(
                  RegionRule.newBuilder()
                      .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .build()));

      Status status = rulesValidator.validate(createRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.ALREADY_EXISTS, status.getCode());
    }
  }

  @Nested
  class ValidateUpdateRegionRuleRequest {
    @Test
    @DisplayName("should return invalid argument status missing id")
    void should_fail_updateRegionRule_missingId() {
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should return invalid argument status missing region ids")
    void should_fail_updateRegionRule_missingRegionIds() {
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should return invalid argument unspecified action type")
    void should_fail_updateRegionRule_invalidActionType() {
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setName("name")
              .build();

      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should return invalid argument unspecified severity")
    void should_fail_createRegionRule_invalidSeverity() {
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setName("name")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
              .build();

      // severity not required for any action other than alert
      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);
      assertEquals(Code.OK, status.getCode());

      updateRegionRuleRequest =
          updateRegionRuleRequest.toBuilder()
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALERT)
              .build();

      status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should return invalid argument invalid name")
    void should_fail_updateRegionRule_invalidName() {
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder().setId("id").addRegionId("region-1").build();

      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should pass as updating the existing block all except rule type")
    void should_pass_updateRegionRule_sameId() {
      RuleScope ruleScope =
          RuleScope.newBuilder()
              .setEnvironmentScope(
                  EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("e1", "e2")))
              .build();
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setName("name")
              .setRuleScope(ruleScope)
              .build();

      when(getAllRegionRulesSupplier.get())
          .thenReturn(
              List.of(
                  RegionRule.newBuilder()
                      .setId("id")
                      .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .setRuleScope(ruleScope)
                      .build()));
      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.OK, status.getCode());
    }

    @Test
    @DisplayName("should fail as env scope has empty list")
    void should_fail_updateRegionRule_empty_list_env_scope() {
      RuleScope ruleScope =
          RuleScope.newBuilder()
              .setEnvironmentScope(EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of()))
              .build();
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setName("name")
              .setRuleScope(ruleScope)
              .build();

      when(getAllRegionRulesSupplier.get())
          .thenReturn(
              List.of(
                  RegionRule.newBuilder()
                      .setId("id")
                      .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .setRuleScope(ruleScope)
                      .build()));
      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should fail as env scope has empty id")
    void should_fail_updateRegionRule_empty_id_env_scope() {
      RuleScope ruleScope =
          RuleScope.newBuilder()
              .setEnvironmentScope(EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("")))
              .build();
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setName("name")
              .setRuleScope(ruleScope)
              .build();

      when(getAllRegionRulesSupplier.get())
          .thenReturn(
              List.of(
                  RegionRule.newBuilder()
                      .setId("id")
                      .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .setRuleScope(ruleScope)
                      .build()));
      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }

    @Test
    @DisplayName("should pass as updating the existing block all except rule type")
    void should_fail_updateRegionRule_differentId() {
      UpdateRegionRuleRequest updateRegionRuleRequest =
          UpdateRegionRuleRequest.newBuilder()
              .setId("id")
              .addRegionId("region-1")
              .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setName("name")
              .build();

      when(getAllRegionRulesSupplier.get())
          .thenReturn(
              List.of(
                  RegionRule.newBuilder()
                      .setId("id2")
                      .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .build()));
      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.ALREADY_EXISTS, status.getCode());
    }
  }

  @Nested
  class ValidateDeleteRegionRuleRequest {
    @Test
    @DisplayName("should return invalid argument status missing id")
    void should_fail_deleteRegionRule_missingId() {
      DeleteRegionRuleRequest deleteRegionRuleRequest =
          DeleteRegionRuleRequest.getDefaultInstance();
      Status status = rulesValidator.validate(deleteRegionRuleRequest);

      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }
  }

  @Nested
  class ValidateRegionsFilter {
    @Test
    @DisplayName("should return invalid argument when both id and region identifier set")
    void should_fail_validate_filter_invalid_input() {
      RegionsFilter filter =
          RegionsFilter.newBuilder()
              .addAllId(List.of("id-1"))
              .addAllRegionIdentifier(
                  List.of(RegionIdentifier.newBuilder().setCountryIsoCode("iso-1").build()))
              .build();
      Status status = rulesValidator.validate(filter);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
      assertTrue(
          status.getDescription().contains("Both id and region identifier should not be present"));
    }

    @Test
    @DisplayName("should pass when no id or region identifier")
    void should_pass_validate_filter_valid_empty_filter() {
      RegionsFilter filter =
          RegionsFilter.newBuilder().addAllId(List.of()).addAllRegionIdentifier(List.of()).build();
      assertEquals(Code.OK, rulesValidator.validate(filter).getCode());
    }

    @Test
    @DisplayName("should pass when only id")
    void should_pass_validate_filter_valid_filter_with_id() {
      RegionsFilter filter = RegionsFilter.newBuilder().addAllId(List.of("id-1")).build();
      assertEquals(Code.OK, rulesValidator.validate(filter).getCode());
    }

    @Test
    @DisplayName("should pass when only valid identifier")
    void should_pass_validate_filter_valid_filter_with_identifier() {
      RegionsFilter filter =
          RegionsFilter.newBuilder()
              .addAllRegionIdentifier(
                  List.of(RegionIdentifier.newBuilder().setCountryIsoCode("iso-1").build()))
              .build();
      assertEquals(Code.OK, rulesValidator.validate(filter).getCode());
    }

    @Test
    @DisplayName("should return invalid argument when invalid identifier")
    void should_fail_validate_filter_invalid_filter_with_identifier() {
      RegionsFilter filter =
          RegionsFilter.newBuilder()
              .addAllRegionIdentifier(List.of(RegionIdentifier.newBuilder().build()))
              .build();
      Status status = rulesValidator.validate(filter);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
      assertTrue(status.getDescription().contains("Invalid case"));
    }

    @Test
    @DisplayName("should return invalid argument when empty iso")
    void should_fail_validate_filter_invalid_filter_with_empty_iso() {
      RegionsFilter filter =
          RegionsFilter.newBuilder()
              .addAllRegionIdentifier(
                  List.of(RegionIdentifier.newBuilder().setCountryIsoCode("").build()))
              .build();
      Status status = rulesValidator.validate(filter);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
      assertTrue(status.getDescription().contains("Country iso code cannot be empty"));
    }
  }

  @Nested
  class ValidateGetRegionRequest {
    @Test
    @DisplayName("should return invalid argument when both id and region identifier set")
    void should_fail_validate_request_invalid_input() {
      GetRegionRequest request =
          GetRegionRequest.newBuilder()
              .setId("id-1")
              .setRegionIdentifier(RegionIdentifier.newBuilder().setCountryIsoCode("iso-1").build())
              .build();
      Status status = rulesValidator.validate(request);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
      assertTrue(
          status.getDescription().contains("Both id and region identifier should not be present"));
    }

    @Test
    @DisplayName("should return invalid argument when both id and region identifier not set")
    void should_fail_validate_request_invalid_input2() {
      GetRegionRequest request = GetRegionRequest.newBuilder().build();
      Status status = rulesValidator.validate(request);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
      assertTrue(
          status.getDescription().contains("Both id and region identifier should not be absent"));
    }

    @Test
    @DisplayName("should pass when only id set")
    void should_pass_validate_request_valid_input() {
      GetRegionRequest request = GetRegionRequest.newBuilder().setId("id-1").build();
      assertEquals(Code.OK, rulesValidator.validate(request).getCode());
    }

    @Test
    @DisplayName("should pass when only identifier set")
    void should_pass_validate_request_valid_input2() {
      GetRegionRequest request =
          GetRegionRequest.newBuilder()
              .setRegionIdentifier(RegionIdentifier.newBuilder().setCountryIsoCode("iso-1").build())
              .build();
      assertEquals(Code.OK, rulesValidator.validate(request).getCode());
    }
  }
}
