package ai.traceable.saved.filter.config.service.validation;

import com.google.inject.ImplementedBy;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@ImplementedBy(ArrayContentTypeFetcherImpl.class)
public interface ArrayContentTypeFetcher {
  AttributeKind getContentType(final AttributeKind arrayType);
}
