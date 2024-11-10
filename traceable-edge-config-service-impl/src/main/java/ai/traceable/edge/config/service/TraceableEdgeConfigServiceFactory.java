package ai.traceable.edge.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.util.Collections;
import java.util.Set;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class TraceableEdgeConfigServiceFactory {
  public static Set<BindableService> build(
      Channel channel, Config config, GrpcChannelRegistry grpcChannelRegistry) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new TraceableEdgeConfigServiceModule(channel, config, grpcChannelRegistry));
    return Collections.singleton(injector.getInstance(BindableService.class));
  }
}
