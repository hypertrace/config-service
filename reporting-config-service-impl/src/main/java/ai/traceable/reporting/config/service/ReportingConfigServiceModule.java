package ai.traceable.reporting.config.service;

import com.google.inject.AbstractModule;
import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class ReportingConfigServiceModule extends AbstractModule {
  private final ManagedChannel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  public ReportingConfigServiceModule(
      ManagedChannel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(BindableService.class).to(ReportingConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(Channel.class).toInstance(channel);
  }
}
