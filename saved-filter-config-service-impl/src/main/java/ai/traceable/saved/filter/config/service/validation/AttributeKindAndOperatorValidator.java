package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import com.google.inject.ImplementedBy;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@ImplementedBy(AttributeKindAndOperatorValidatorImpl.class)
public interface AttributeKindAndOperatorValidator {
  void validate(AttributeKind lhsAttributeKind, RelationalOperator operator);
}
