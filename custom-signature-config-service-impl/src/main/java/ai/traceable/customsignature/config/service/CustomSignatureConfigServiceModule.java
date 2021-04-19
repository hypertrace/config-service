package ai.traceable.customsignature.config.service;

import ai.traceable.customsignature.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

class CustomSignatureConfigServiceModule extends AbstractModule {
  private final ManagedChannel channel;

  CustomSignatureConfigServiceModule(ManagedChannel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(CustomSignatureConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    install(new RulesManagerModule());
  }
}
