package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.NullValueStrategy;
import com.google.inject.ImplementedBy;
import com.google.protobuf.Value;

@ImplementedBy(ValueValidatorImpl.class)
public interface ValueValidator {
  void validate(Value value, NullValueStrategy nullValueStrategy);
}
