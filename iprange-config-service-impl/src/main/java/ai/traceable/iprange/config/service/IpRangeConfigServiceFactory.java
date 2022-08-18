package ai.traceable.iprange.config.service;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class IpRangeConfigServiceFactory {
  private IpRangeConfigServiceFactory() {}

  public static BindableService build(
      Channel channel,
      Config config,
      ActivityEventProducer activityEventProducer,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new IpRangeConfigServiceModule(
                channel, config, activityEventProducer, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
