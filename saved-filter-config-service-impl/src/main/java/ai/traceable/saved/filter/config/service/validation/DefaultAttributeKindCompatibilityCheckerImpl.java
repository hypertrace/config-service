package ai.traceable.saved.filter.config.service.validation;

import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_IN;
import static ai.traceable.saved.filter.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_NOT_IN;

import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultAttributeKindCompatibilityCheckerImpl
    implements AttributeKindCompatibilityChecker {
  private static final Set<RelationalOperator> MULTI_VALUE_OPERATORS =
      Set.of(RELATIONAL_OPERATOR_IN, RELATIONAL_OPERATOR_NOT_IN);

  private ArrayContentTypeFetcher arrayContentTypeFetcher;

  @Override
  public void checkCompatibility(
      AttributeKind lhs, AttributeKind rhs, RelationalOperator operator) {
    final AttributeKind rhsTypeToCompare;

    if (MULTI_VALUE_OPERATORS.contains(operator)) {
      rhsTypeToCompare = arrayContentTypeFetcher.getContentType(rhs);
    } else {
      rhsTypeToCompare = rhs;
    }

    // TODO: Consume the converters here from the library once added
    if (lhs != rhsTypeToCompare) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Incompatible attribute kind for operator '%s': LHS is of type %s and derived RHS is of type %s",
                  operator, lhs, rhsTypeToCompare))
          .asRuntimeException();
    }
  }
}
