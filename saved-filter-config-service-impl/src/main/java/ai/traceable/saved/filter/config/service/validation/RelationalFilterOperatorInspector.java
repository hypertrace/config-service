package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalFilterOperatorInspector implements RelationalFilterInspector {

  private final AttributeKindAndOperatorValidator attributeKindAndOperatorValidator;
  private final AttributeKindExtractor attributeKindExtractor;

  @Override
  public void inspect(RelationalFilterCondition filter, ValidationContext validationContext) {

    if (!filter.getFieldName().isEmpty()) {
      return;
    }

    AttributeKind lhsAttributeKind;

    try {
      lhsAttributeKind =
          attributeKindExtractor.extractAttributeKind(filter.getLhsExpression(), validationContext);
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT.withDescription("Invalid LHS type").asRuntimeException();
    }

    try {
      attributeKindAndOperatorValidator.validate(lhsAttributeKind, filter.getOperator());
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "LHS of filter (%s) has attributeKind (%s) which is not compatible with operator (%s) ",
                  filter, lhsAttributeKind, filter.getOperator()))
          .asRuntimeException();
    }
  }
}
