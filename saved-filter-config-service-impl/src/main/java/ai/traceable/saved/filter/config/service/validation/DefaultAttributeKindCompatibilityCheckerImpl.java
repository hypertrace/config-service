package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import com.google.inject.Inject;
import io.grpc.Status;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultAttributeKindCompatibilityCheckerImpl
    implements AttributeKindCompatibilityChecker {

  @Override
  public void checkCompatibility(
      AttributeKind lhs, AttributeKind rhs, RelationalOperator operator) {

    // TODO: Consume the converters here from the library once added
    if (lhs != rhs) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Incompatible attribute kind for operator '%s':LHS (%s) and RHS (%s)",
                  operator, lhs, rhs))
          .asRuntimeException();
    }
  }
}
