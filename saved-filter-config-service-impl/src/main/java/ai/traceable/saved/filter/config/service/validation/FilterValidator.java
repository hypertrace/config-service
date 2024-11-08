package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.FilterCriteria.FilterConditionCase.FILTERCONDITION_NOT_SET;
import static ai.traceable.saved.filter.config.service.v1.FilterCriteria.FilterConditionCase.LOGICAL_FILTER;
import static ai.traceable.saved.filter.config.service.v1.FilterCriteria.FilterConditionCase.RELATIONAL_FILTER;

import ai.traceable.saved.filter.config.service.v1.FilterCriteria;
import ai.traceable.saved.filter.config.service.v1.FilterCriteria.FilterConditionCase;
import ai.traceable.saved.filter.config.service.v1.LogicalFilterCondition;
import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import io.grpc.Status;
import jakarta.inject.Inject;

public class FilterValidator implements SavedFilterValidator<FilterCriteria> {

  private final SavedFilterValidator<LogicalFilterCondition> logicalFilterConditionValidator;
  private final SavedFilterValidator<RelationalFilterCondition> relationalFilterConditionValidator;
  private final EnumSwitcher<FilterConditionCase, FilterCriteria, ValidationContext, Void>
      enumSwitcher;

  @Inject
  public FilterValidator(
      SavedFilterValidator<LogicalFilterCondition> logicalFilterConditionValidator,
      SavedFilterValidator<RelationalFilterCondition> relationalFilterConditionValidator) {
    this.logicalFilterConditionValidator = logicalFilterConditionValidator;
    this.relationalFilterConditionValidator = relationalFilterConditionValidator;
    this.enumSwitcher = buildSwitcher();
  }

  @Override
  public void validate(FilterCriteria filterCriteria, ValidationContext validationContext) {

    enumSwitcher.apply(filterCriteria.getFilterConditionCase(), filterCriteria, validationContext);
  }

  private EnumSwitcher<FilterConditionCase, FilterCriteria, ValidationContext, Void>
      buildSwitcher() {

    return EnumSwitcher.<FilterConditionCase, FilterCriteria, ValidationContext, Void>builder(
            FilterConditionCase.class)
        .addCase(LOGICAL_FILTER, this::validateLogicalFilter)
        .addCase(RELATIONAL_FILTER, this::validateRelationalFilter)
        .addExclusion(FILTERCONDITION_NOT_SET)
        .exceptionSupplier(
            (expression, context, caseEnum) ->
                Status.UNIMPLEMENTED
                    .withDescription("Filter type " + caseEnum + " is not supported")
                    .asRuntimeException())
        .build();
  }

  private void validateRelationalFilter(
      final FilterCriteria filterCriteria, final ValidationContext validationContext) {
    relationalFilterConditionValidator.validate(
        filterCriteria.getRelationalFilter(), validationContext);
  }

  private void validateLogicalFilter(
      final FilterCriteria filterCriteria, final ValidationContext validationContext) {
    logicalFilterConditionValidator.validate(filterCriteria.getLogicalFilter(), validationContext);
  }
}
