package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.Field;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import com.google.inject.ImplementedBy;

@ImplementedBy(DefaultFieldValidatorImpl.class)
public interface FieldValidator {
  void validate(Field field, ValidationContext validationContext);
}
