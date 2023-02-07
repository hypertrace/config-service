package ai.traceable.blocking.config.service.v1;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class BlockingConfigServiceFactory {
  public static BindableService build(
      Channel channel, Config config, GrpcChannelRegistry grpcChannelRegistry) {
    Injector injector =
        Guice.createInjector(new BlockingConfigServiceModule(channel, config, grpcChannelRegistry));
    return injector.getInstance(BindableService.class);
  }
}
