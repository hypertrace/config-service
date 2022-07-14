package ai.traceable.customsignature.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;

public class CustomSignatureConfigServiceFactory {
  public static BindableService build(
      Channel channel, Config config, ActivityEventProducer activityEventProducer) {
    Injector injector =
        Guice.createInjector(
            new CustomSignatureConfigServiceModule(channel, config, activityEventProducer));
    return injector.getInstance(BindableService.class);
  }
}
