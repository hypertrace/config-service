package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.Expression;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import com.google.inject.ImplementedBy;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@ImplementedBy(DefaultAttributeKindExtractorImpl.class)
public interface AttributeKindExtractor {

  AttributeKind extractAttributeKind(Expression expression, ValidationContext validationContext);
}
