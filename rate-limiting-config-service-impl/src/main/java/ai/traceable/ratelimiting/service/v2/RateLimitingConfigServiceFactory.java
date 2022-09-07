package ai.traceable.ratelimiting.service.v2;

import ai.traceable.activity.event.producer.ActivityEventProducer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class RateLimitingConfigServiceFactory {
  public static BindableService build(
      Channel channel,
      Config config,
      ActivityEventProducer activityEventProducer,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new RateLimitingConfigServiceModule(
                channel, config, activityEventProducer, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
