package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;

public interface RelationalFilterInspector {
  void inspect(RelationalFilterCondition filter, ValidationContext validationContext);
}
