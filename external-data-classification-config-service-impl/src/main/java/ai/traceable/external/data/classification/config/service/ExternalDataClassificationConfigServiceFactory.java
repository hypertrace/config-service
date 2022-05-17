package ai.traceable.external.data.classification.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class ExternalDataClassificationConfigServiceFactory {
  public static BindableService build(
      Channel channel, Config config, GrpcChannelRegistry channelRegistry) {
    Injector injector =
        Guice.createInjector(
            new ExternalDataClassificationConfigServiceModule(channel, config, channelRegistry));
    return injector.getInstance(BindableService.class);
  }
}
