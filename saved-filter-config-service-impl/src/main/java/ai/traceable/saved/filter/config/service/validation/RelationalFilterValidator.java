package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.FIELD;
import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.VALUE;

import ai.traceable.saved.filter.config.service.v1.Expression;
import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import ai.traceable.saved.filter.config.service.validation.RelationalFilterInspector.RelationalFilterInspectionContext;
import com.google.protobuf.Value;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalFilterValidator implements SavedFilterValidator<RelationalFilterCondition> {

  private final Set<RelationalFilterInspector> relationalFilterInspectors;
  private final ExpressionValidator expressionValidator;
  private final AttributeKindExtractor attributeKindExtractor;

  @Override
  public void validate(
      RelationalFilterCondition relationalFilterCondition, ValidationContext validationContext) {
    if (!relationalFilterCondition.getFieldName().isEmpty()
        || !Value.getDefaultInstance().equals(relationalFilterCondition.getFieldValue())) {
      // No need to perform any validation for the old flow
      return;
    }

    validateLhsExpression(relationalFilterCondition, validationContext);
    validateRhsExpression(relationalFilterCondition, validationContext);

    collectFieldFromExpression(relationalFilterCondition.getLhsExpression(), validationContext);
    collectFieldFromExpression(relationalFilterCondition.getRhsExpression(), validationContext);

    if (areBothSidesConstant(relationalFilterCondition)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "LHS and RHS both cannot be a constant in filter: %s", relationalFilterCondition))
          .asRuntimeException();
    }

    final AttributeKind lhsAttributeKind =
        extractAttributeKind(relationalFilterCondition.getLhsExpression(), validationContext);

    final AttributeKind rhsAttributeKind =
        extractAttributeKind(relationalFilterCondition.getRhsExpression(), validationContext);

    final RelationalFilterInspectionContext inspectionContext =
        RelationalFilterInspectionContext.builder()
            .lhsAttributeKind(lhsAttributeKind)
            .rhsAttributeKind(rhsAttributeKind)
            .operator(relationalFilterCondition.getOperator())
            .loggingContext(relationalFilterCondition.toString())
            .build();

    relationalFilterInspectors.forEach(
        inspector -> inspector.inspect(inspectionContext, validationContext));
  }

  private void validateLhsExpression(
      final RelationalFilterCondition filter, final ValidationContext validationContext) {
    expressionValidator.validate(
        filter.getLhsExpression(),
        validationContext,
        NullValueStrategy.forRelationalOperator(filter.getOperator()));
  }

  private void validateRhsExpression(
      final RelationalFilterCondition filter, final ValidationContext validationContext) {
    expressionValidator.validate(
        filter.getRhsExpression(),
        validationContext,
        NullValueStrategy.forRelationalOperator(filter.getOperator()));
  }

  private boolean areBothSidesConstant(final RelationalFilterCondition filter) {
    return VALUE.equals(filter.getLhsExpression().getTypeCase())
        && VALUE.equals(filter.getRhsExpression().getTypeCase());
  }

  private AttributeKind extractAttributeKind(
      Expression expression, ValidationContext validationContext) {
    try {
      return attributeKindExtractor.extractAttributeKind(expression, validationContext);
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Unable to extract attributeKind from expression: %s", expression))
          .asRuntimeException();
    }
  }

  private void collectFieldFromExpression(
      final Expression expression, final ValidationContext validationContext) {
    if (FIELD.equals(expression.getTypeCase())) {
      validationContext.addFilterVariable(expression.getField());
    }
  }
}
