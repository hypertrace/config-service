package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalFilterOperatorInspector implements RelationalFilterInspector {

  private final AttributeKindAndOperatorValidator attributeKindAndOperatorValidator;

  @Override
  public void inspect(
      final RelationalFilterInspectionContext inspectionContext,
      final ValidationContext validationContext) {
    final AttributeKind lhsAttributeKind = inspectionContext.lhsAttributeKind();
    final RelationalOperator operator = inspectionContext.operator();

    try {
      attributeKindAndOperatorValidator.validate(lhsAttributeKind, operator);
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "LHS of filter (%s) has attributeKind (%s) which is not compatible with operator (%s) ",
                  inspectionContext.loggingContext(), lhsAttributeKind, operator))
          .asRuntimeException();
    }
  }
}
