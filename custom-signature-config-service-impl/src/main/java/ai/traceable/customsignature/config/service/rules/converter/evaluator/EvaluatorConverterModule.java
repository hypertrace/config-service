package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;

@Singleton
public class EvaluatorConverterModule extends AbstractModule {

  @Override
  protected void configure() {
    final Multibinder<CustomSignatureRuleDefinitionConverter> multiBinder =
        Multibinder.newSetBinder(binder(), CustomSignatureRuleDefinitionConverter.class);
    multiBinder.addBinding().to(MatchExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(KeyValueExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(IpAbuseVelocityExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(IpAddressExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(IpAsnExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(IpConnectionTypeExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(IpOrganisationExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(IpReputationExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(IpTypeExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(RegionExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(UserAgentExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(EmailDomainExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(UserIdExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(ScopeExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(LhsRhsKeysExpressionRuleDefinitionConverter.class);
    multiBinder.addBinding().to(CustomSecRuleRuleDefinitionConverter.class);
  }
}
