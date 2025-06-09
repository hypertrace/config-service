package ai.traceable.customsignature.config.service.rules.converter.expression;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;

@Singleton
public class ExpressionConverterModule extends AbstractModule {

  @Override
  protected void configure() {
    final Multibinder<CustomSignatureExpressionConverter> multiBinder =
        Multibinder.newSetBinder(binder(), CustomSignatureExpressionConverter.class);
    multiBinder.addBinding().to(MatchExpressionConverter.class);
    multiBinder.addBinding().to(KeyValueExpressionConverter.class);
    multiBinder.addBinding().to(IpAbuseVelocityExpressionConverter.class);
    multiBinder.addBinding().to(IpAddressExpressionConverter.class);
    multiBinder.addBinding().to(IpAsnExpressionConverter.class);
    multiBinder.addBinding().to(IpConnectionTypeExpressionConverter.class);
    multiBinder.addBinding().to(IpOrganisationExpressionConverter.class);
    multiBinder.addBinding().to(IpReputationExpressionConverter.class);
    multiBinder.addBinding().to(IpTypeExpressionConverter.class);
    multiBinder.addBinding().to(RegionExpressionConverter.class);
    multiBinder.addBinding().to(UserAgentExpressionConverter.class);
    multiBinder.addBinding().to(EmailDomainExpressionConverter.class);
    multiBinder.addBinding().to(UserIdExpressionConverter.class);
    multiBinder.addBinding().to(ScopeExpressionConverter.class);
    multiBinder.addBinding().to(LhsRhsKeysExpressionConverter.class);
  }
}
