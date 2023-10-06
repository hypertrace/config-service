package ai.traceable.modsecurity.rule.conversion;

import com.google.inject.AbstractModule;

public class ModsecRuleConversionModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ModsecRuleConverter.class).to(ModsecRuleConverterImpl.class);
  }
}
