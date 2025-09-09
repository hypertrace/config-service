package ai.traceable.data.parsing.config.service.v1;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class DataParsingConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;

  public DataParsingConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(DataParsingConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
