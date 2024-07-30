package ai.traceable.saved.query.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class SavedQueryConfigServiceFactory {

  private SavedQueryConfigServiceFactory() {}

  public static BindableService build(
      Config config,
      Channel channel,
      ConfigChangeEventGenerator changeEventGenerator,
      GrpcChannelRegistry channelRegistry) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new SavedQueryConfigServiceModule(
                config, channel, changeEventGenerator, channelRegistry));
    return injector.getInstance(BindableService.class);
  }
}
