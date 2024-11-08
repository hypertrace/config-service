package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.LogicalOperator.LOGICAL_OPERATOR_UNSPECIFIED;

import ai.traceable.saved.filter.config.service.v1.FilterCriteria;
import ai.traceable.saved.filter.config.service.v1.LogicalFilterCondition;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class LogicalFilterValidator implements SavedFilterValidator<LogicalFilterCondition> {

  private final SavedFilterValidator<FilterCriteria> filterValidator;

  @Override
  public void validate(
      LogicalFilterCondition logicalFilterCondition, ValidationContext validationContext) {

    if (LOGICAL_OPERATOR_UNSPECIFIED.equals(logicalFilterCondition.getOperator())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Logical operator is not specified")
          .asRuntimeException();
    }
    logicalFilterCondition
        .getFilterCriteriaList()
        .forEach(filterCriteria -> filterValidator.validate(filterCriteria, validationContext));
  }
}
