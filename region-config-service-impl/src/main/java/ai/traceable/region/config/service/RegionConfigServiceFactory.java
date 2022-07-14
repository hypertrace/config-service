package ai.traceable.region.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;

public class RegionConfigServiceFactory {
  public static BindableService build(
      Channel channel, Config config, ActivityEventProducer activityEventProducer) {
    Injector injector =
        Guice.createInjector(new RegionConfigServiceModule(channel, config, activityEventProducer));
    return injector.getInstance(BindableService.class);
  }
}
