package ai.traceable.waf.provider.integration.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class WafIntegrationConfigServiceFactory {
  private WafIntegrationConfigServiceFactory() {}

  public static BindableService build(
      final Channel channel,
      final Config config,
      final ConfigChangeEventGenerator configChangeEventGenerator,
      final GrpcChannelRegistry grpcChannelRegistry) {
    final Injector injector =
        Guice.createInjector(
            new WafIntegrationConfigServiceModule(
                config, channel, configChangeEventGenerator, grpcChannelRegistry));
    return injector.getInstance(BindableService.class);
  }
}
