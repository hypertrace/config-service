package ai.traceable.runner.logs.config.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import lombok.SneakyThrows;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class RunnerLogsConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;
  private final RunnerLogsConfigServiceConfig runnerLogsConfigServiceConfig;

  @SneakyThrows
  RunnerLogsConfigServiceModule(
      final Config config,
      final Channel channel,
      final ConfigChangeEventGenerator changeEventGenerator) {
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
    this.runnerLogsConfigServiceConfig = new RunnerLogsConfigServiceConfig(config);
  }

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(RunnerLogsConfigServiceImpl.class);
    bind(RunnerLogsConfigServiceConfig.class).toInstance(runnerLogsConfigServiceConfig);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
