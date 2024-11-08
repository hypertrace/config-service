package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.Expression;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.NullValueStrategy;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import com.google.inject.ImplementedBy;

@ImplementedBy(DefaultExpressionValidatorImpl.class)
public interface ExpressionValidator {
  void validate(
      Expression expression,
      ValidationContext validationContext,
      NullValueStrategy nullValueStrategy);
}
