package ai.traceable.runner.logs.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class RunnerLogsConfigServiceFactory {

  private RunnerLogsConfigServiceFactory() {}

  public static BindableService build(
      final Config config,
      final Channel channel,
      final ConfigChangeEventGenerator changeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new RunnerLogsConfigServiceModule(config, channel, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
