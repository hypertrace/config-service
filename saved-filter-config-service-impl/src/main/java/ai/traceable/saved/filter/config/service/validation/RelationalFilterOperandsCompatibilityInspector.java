package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalFilterOperandsCompatibilityInspector implements RelationalFilterInspector {

  private final AttributeKindCompatibilityChecker attributeKindCompatibilityChecker;

  @Override
  public void inspect(
      final RelationalFilterInspectionContext inspectionContext,
      final ValidationContext validationContext) {
    final AttributeKind lhsAttributeKind = inspectionContext.lhsAttributeKind();
    final AttributeKind rhsAttributeKind = inspectionContext.rhsAttributeKind();

    try {
      attributeKindCompatibilityChecker.checkCompatibility(
          lhsAttributeKind, rhsAttributeKind, inspectionContext.operator());
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "The LHS attributeKind (%s) is not compatible with the RHS attributeKind (%s) in (%s)",
                  lhsAttributeKind, rhsAttributeKind, inspectionContext.loggingContext()))
          .asRuntimeException();
    }
  }
}
