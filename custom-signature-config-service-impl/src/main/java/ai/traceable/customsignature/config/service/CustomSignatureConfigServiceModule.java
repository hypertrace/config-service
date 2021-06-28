package ai.traceable.customsignature.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManagerModule;
import ai.traceable.customsignature.config.service.rules.RulesManagerModule;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;

class CustomSignatureConfigServiceModule extends AbstractModule {
  private final CustomSignatureConfigServiceConfig config;
  private final ManagedChannel channel;
  private final ActivityEventProducer activityEventProducer;

  CustomSignatureConfigServiceModule(
      ManagedChannel channel, Config config, ActivityEventProducer activityEventProducer) {
    this.channel = channel;
    this.config = new CustomSignatureConfigServiceConfig(config);
    this.activityEventProducer = activityEventProducer;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(CustomSignatureConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);
    bind(ActivityEventProducer.class).toInstance(activityEventProducer);
    bind(CustomSignatureConfigServiceConfig.class).toInstance(this.config);
    install(new RulesManagerModule());
    install(new ModsecRulesManagerModule());
  }
}
