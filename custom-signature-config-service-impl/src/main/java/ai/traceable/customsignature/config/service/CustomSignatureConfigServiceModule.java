package ai.traceable.customsignature.config.service;

import ai.traceable.customsignature.config.service.modsec.ModsecRulesManagerModule;
import ai.traceable.customsignature.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

class CustomSignatureConfigServiceModule extends AbstractModule {
  private final Config config;
  private final ManagedChannel channel;

  CustomSignatureConfigServiceModule(ManagedChannel channel, Config config) {
    this.channel = channel;
    this.config = config.getConfig("custom.signature.config.service");
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(CustomSignatureConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    install(new RulesManagerModule());
    install(new ModsecRulesManagerModule());
  }

  @Provides
  CustomSignatureConfigServiceConfig providesCustomSignatureServiceConfig() {
    return new CustomSignatureConfigServiceConfig(this.config);
  }
}
