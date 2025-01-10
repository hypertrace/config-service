package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.RelationalOperator;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.attribute.service.v1.AttributeKind;

public interface RelationalFilterInspector {
  void inspect(
      final RelationalFilterInspectionContext inspectionContext,
      ValidationContext validationContext);

  @Value
  @Builder
  @Accessors(fluent = true, chain = true)
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  class RelationalFilterInspectionContext {
    AttributeKind lhsAttributeKind;
    AttributeKind rhsAttributeKind;
    RelationalOperator operator;
    String loggingContext;
  }
}
