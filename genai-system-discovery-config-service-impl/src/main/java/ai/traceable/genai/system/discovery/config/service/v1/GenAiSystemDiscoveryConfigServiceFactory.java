package ai.traceable.genai.system.discovery.config.service.v1;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class GenAiSystemDiscoveryConfigServiceFactory {
  private GenAiSystemDiscoveryConfigServiceFactory() {}

  public static BindableService build(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            new GenAiSystemDiscoveryConfigServiceModule(channel, configChangeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
