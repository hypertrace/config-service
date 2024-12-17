package ai.traceable.edge.decision.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class EdgeDecisionConfigServiceFactory {

  private EdgeDecisionConfigServiceFactory() {}

  public static BindableService build(
      Config config, Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new EdgeDecisionConfigServiceModule(config, channel, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
