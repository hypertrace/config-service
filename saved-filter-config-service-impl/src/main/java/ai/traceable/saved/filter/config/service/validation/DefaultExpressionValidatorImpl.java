package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.FIELD;
import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.TYPE_NOT_SET;
import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.VALUE;

import ai.traceable.saved.filter.config.service.v1.Expression;
import ai.traceable.saved.filter.config.service.v1.Expression.TypeCase;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.NullValueStrategy;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.Builder;
import lombok.Value;

public class DefaultExpressionValidatorImpl implements ExpressionValidator {

  private final FieldValidator fieldValidator;
  private final ValueValidator valueValidator;
  private final EnumSwitcher<TypeCase, Expression, ExpressionValidatorContext, Void> switcher;

  @Inject
  public DefaultExpressionValidatorImpl(
      FieldValidator fieldValidator, ValueValidator valueValidator) {
    this.fieldValidator = fieldValidator;
    this.valueValidator = valueValidator;
    this.switcher = buildSwitcher();
  }

  @Override
  public void validate(
      Expression expression,
      ValidationContext validationContext,
      NullValueStrategy nullValueStrategy) {
    switcher.apply(
        expression.getTypeCase(),
        expression,
        ExpressionValidatorContext.builder()
            .validationContext(validationContext)
            .nullValueStrategy(nullValueStrategy)
            .build());
  }

  private EnumSwitcher<TypeCase, Expression, ExpressionValidatorContext, Void> buildSwitcher() {

    return EnumSwitcher.<TypeCase, Expression, ExpressionValidatorContext, Void>builder(
            TypeCase.class)
        .addCase(FIELD, this::validateField)
        .addCase(VALUE, this::validateValue)
        .addExclusion(TYPE_NOT_SET)
        .exceptionSupplier(
            (expression, context, caseEnum) ->
                Status.INVALID_ARGUMENT
                    .withDescription(String.format("Invalid expression type %s", caseEnum))
                    .asRuntimeException())
        .build();
  }

  private void validateValue(
      final Expression expression, final ExpressionValidatorContext expressionValidatorContext) {
    valueValidator.validate(expression.getValue(), expressionValidatorContext.nullValueStrategy);
  }

  private void validateField(
      final Expression expression, final ExpressionValidatorContext expressionValidatorContext) {
    fieldValidator.validate(expression.getField(), expressionValidatorContext.validationContext);
  }

  @Value
  @Builder
  private static class ExpressionValidatorContext {
    ValidationContext validationContext;
    NullValueStrategy nullValueStrategy;
  }
}
