package ai.traceable.bot.categorized.policy.service.v1;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class CategorizedBotConfigPolicyServiceFactory {

  private CategorizedBotConfigPolicyServiceFactory() {}

  public static BindableService build(
      final Channel channel,
      final ConfigChangeEventGenerator changeEventGenerator,
      final GrpcChannelRegistry grpcChannelRegistry,
      final Config config) {
    final Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new CategorizedBotConfigPolicyServiceModule(
                channel, changeEventGenerator, grpcChannelRegistry, config));
    return injector.getInstance(BindableService.class);
  }
}
