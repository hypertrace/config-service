package ai.traceable.licensestatus.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class LicenseStatusConfigServiceFactory {

  public static BindableService build(
      GrpcChannelRegistry channelRegistry, Channel channel, Config config) {
    final Injector injector =
        Guice.createInjector(
            new LicenseStatusConfigServiceModule(channelRegistry, channel, config));
    return injector.getInstance(BindableService.class);
  }
}
