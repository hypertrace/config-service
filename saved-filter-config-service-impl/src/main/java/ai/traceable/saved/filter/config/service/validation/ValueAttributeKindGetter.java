package ai.traceable.saved.filter.config.service.validation;

import com.google.inject.ImplementedBy;
import com.google.protobuf.Value;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

@ImplementedBy(ValueAttributeKindGetterImpl.class)
public interface ValueAttributeKindGetter {
  AttributeKind getForLiteralValue(Value value);
}
