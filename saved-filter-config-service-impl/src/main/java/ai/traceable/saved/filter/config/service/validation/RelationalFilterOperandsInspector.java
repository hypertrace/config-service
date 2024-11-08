package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.NullValueStrategy;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalFilterOperandsInspector implements RelationalFilterInspector {

  private final ExpressionValidator expressionValidator;

  @Override
  public void inspect(RelationalFilterCondition filter, ValidationContext validationContext) {
    validateLhsExpression(filter, validationContext);
    validateRhsExpression(filter, validationContext);
  }

  private void validateLhsExpression(
      RelationalFilterCondition filter, ValidationContext validationContext) {

    if (!filter.getFieldName().isEmpty()) {
      return;
    }

    expressionValidator.validate(
        filter.getLhsExpression(),
        validationContext,
        NullValueStrategy.forRelationalOperator(filter.getOperator()));
  }

  private void validateRhsExpression(
      RelationalFilterCondition filter, ValidationContext validationContext) {

    if (!Value.getDefaultInstance().equals(filter.getFieldValue())) {
      return;
    }

    expressionValidator.validate(
        filter.getRhsExpression(),
        validationContext,
        NullValueStrategy.forRelationalOperator(filter.getOperator()));
  }
}
