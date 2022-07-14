package ai.traceable.ratelimiting.service.v2;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;

public class RateLimitingConfigServiceFactory {
  public static BindableService build(
      Channel channel, Config config, ActivityEventProducer activityEventProducer) {
    Injector injector =
        Guice.createInjector(
            new RateLimitingConfigServiceModule(channel, config, activityEventProducer));
    return injector.getInstance(BindableService.class);
  }
}
