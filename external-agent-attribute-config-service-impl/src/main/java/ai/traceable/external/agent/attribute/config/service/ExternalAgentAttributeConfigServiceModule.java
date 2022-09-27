package ai.traceable.external.agent.attribute.config.service;

import ai.traceable.external.agent.attribute.config.service.translator.BasicAuthRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.CustomTokenRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.JwtRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.RequestHeaderRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.ResponseBodyRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.RuleTranslator;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class ExternalAgentAttributeConfigServiceModule extends AbstractModule {
  private final Channel channel;

  ExternalAgentAttributeConfigServiceModule(Channel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ExternalAgentAttributeConfigServiceImpl.class);

    Multibinder<RuleTranslator> multibinder =
        Multibinder.newSetBinder(binder(), RuleTranslator.class);
    multibinder.addBinding().to(BasicAuthRuleTranslator.class);
    multibinder.addBinding().to(CustomTokenRuleTranslator.class);
    multibinder.addBinding().to(JwtRuleTranslator.class);
    multibinder.addBinding().to(RequestHeaderRuleTranslator.class);
    multibinder.addBinding().to(ResponseBodyRuleTranslator.class);
  }

  @Provides
  UserAttributionConfigServiceBlockingStub providesUserAttributionStub() {
    return UserAttributionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
