package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.Field;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import com.google.inject.ImplementedBy;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;

@ImplementedBy(DefaultFieldMetadataGetterImpl.class)
public interface FieldMetadataGetter {
  AttributeMetadata getOrThrow(Field field, ValidationContext validationContext);
}
