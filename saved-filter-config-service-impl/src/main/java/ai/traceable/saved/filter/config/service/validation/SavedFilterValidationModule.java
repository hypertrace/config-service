package ai.traceable.saved.filter.config.service.validation;

import ai.traceable.saved.filter.config.service.v1.ArrayFilterCondition;
import ai.traceable.saved.filter.config.service.v1.FilterCriteria;
import ai.traceable.saved.filter.config.service.v1.LogicalFilterCondition;
import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

public class SavedFilterValidationModule extends AbstractModule {

  @Override
  protected void configure() {
    bindFilterValidators();
    bindRelationalFilterInspectors();
  }

  private void bindFilterValidators() {

    bind(new TypeLiteral<SavedFilterValidator<FilterCriteria>>() {}).to(FilterValidator.class);
    bind(new TypeLiteral<SavedFilterValidator<LogicalFilterCondition>>() {})
        .to(LogicalFilterValidator.class);
    bind(new TypeLiteral<SavedFilterValidator<RelationalFilterCondition>>() {})
        .to(RelationalFilterValidator.class);
    bind(new TypeLiteral<SavedFilterValidator<ArrayFilterCondition>>() {})
        .to(ArrayFilterValidator.class);
  }

  private void bindRelationalFilterInspectors() {
    final Multibinder<RelationalFilterInspector> relationalFilterInspector =
        Multibinder.newSetBinder(binder(), RelationalFilterInspector.class);
    relationalFilterInspector.addBinding().to(RelationalFilterOperandsCompatibilityInspector.class);
    relationalFilterInspector.addBinding().to(RelationalFilterOperatorInspector.class);
  }
}
