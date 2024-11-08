package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.Expression.TypeCase.VALUE;

import ai.traceable.saved.filter.config.service.v1.Expression;
import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import com.google.protobuf.Value;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalFilterOperandsCompatibilityInspector implements RelationalFilterInspector {

  private final AttributeKindCompatibilityChecker attributeKindCompatibilityChecker;
  private final AttributeKindExtractor attributeKindExtractor;

  @Override
  public void inspect(RelationalFilterCondition filter, ValidationContext validationContext) {

    if (!filter.getFieldName().isEmpty()
        || !Value.getDefaultInstance().equals(filter.getFieldValue())) {
      return;
    }
    if (areBothSidesConstant(filter)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("LHS and RHS both cannot be a constant in filter: %s", filter))
          .asRuntimeException();
    }
    AttributeKind lhsAttributeKind =
        extractAttributeKind(filter.getLhsExpression(), validationContext);

    AttributeKind rhsAttributeKind =
        extractAttributeKind(filter.getRhsExpression(), validationContext);

    checkAttributeKindCompatibility(lhsAttributeKind, rhsAttributeKind, filter);
  }

  private boolean areBothSidesConstant(RelationalFilterCondition filter) {
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

  private void checkAttributeKindCompatibility(
      AttributeKind lhsAttributeKind,
      AttributeKind rhsAttributeKind,
      RelationalFilterCondition filter) {
    try {
      attributeKindCompatibilityChecker.checkCompatibility(
          lhsAttributeKind, rhsAttributeKind, filter.getOperator());
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "The LHS attributeKind (%s) is not compatible with the RHS attributeKind (%s) in (%s)",
                  lhsAttributeKind, rhsAttributeKind, filter))
          .asRuntimeException();
    }
  }
}
