package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.Field;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultFieldValidatorImpl implements FieldValidator {
  private final FieldMetadataGetter fieldMetadataGetter;

  @Override
  public void validate(Field field, ValidationContext validationContext) {

    fieldMetadataGetter.getOrThrow(field, validationContext);
  }
}
