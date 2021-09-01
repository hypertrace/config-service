package ai.traceable.region.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
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
                      .setId("id")
                      .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                      .build()));
      Status status = rulesValidator.validate(updateRegionRuleRequest, getAllRegionRulesSupplier);

      assertEquals(Code.OK, status.getCode());
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
}
