package ai.traceable.fraud.policy.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class FraudPolicyConfigServiceFactory {

  private FraudPolicyConfigServiceFactory() {}

  public static BindableService build(
      Channel channel,
      Config config,
      ConfigChangeEventGenerator changeEventGenerator,
      GrpcChannelRegistry grpcChannelRegistry) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new FraudPolicyConfigServiceModule(
                channel, config, changeEventGenerator, grpcChannelRegistry));
    return injector.getInstance(BindableService.class);
  }
}
