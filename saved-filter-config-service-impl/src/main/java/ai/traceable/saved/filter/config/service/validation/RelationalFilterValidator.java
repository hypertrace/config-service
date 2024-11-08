package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import jakarta.inject.Inject;
import java.util.Set;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalFilterValidator implements SavedFilterValidator<RelationalFilterCondition> {

  private final Set<RelationalFilterInspector> relationalFilterInspectors;

  @Override
  public void validate(
      RelationalFilterCondition relationalFilterCondition, ValidationContext validationContext) {
    relationalFilterInspectors.forEach(
        inspector -> inspector.inspect(relationalFilterCondition, validationContext));
  }
}
