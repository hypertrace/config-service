package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.v1.BlockingConfigServiceModuleV1;
import ai.traceable.blocking.config.service.v2.BlockingConfigServiceModuleV2;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Stage;
import com.google.inject.TypeLiteral;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.util.Collections;
import java.util.Set;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class BlockingConfigServiceFactory {
  public static Set<BindableService> build(
      Channel channel, Config config, GrpcChannelRegistry grpcChannelRegistry) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new BlockingConfigServiceModuleV1(),
            new BlockingConfigServiceModuleV2(),
            new BlockingConfigServiceModule(channel, config, grpcChannelRegistry));
    return Collections.unmodifiableSet(
        injector.getInstance(Key.get(new TypeLiteral<Set<BindableService>>() {})));
  }
}
