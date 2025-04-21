package ai.traceable.genai.config.service.v1;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class GenAiConfigServiceFactory {

  private GenAiConfigServiceFactory() {}

  public static BindableService build(
      Channel channel, ConfigChangeEventGenerator changeEventGenerator, Config config) {
    Injector injector =
        Guice.createInjector(new GenAiConfigServiceModule(channel, config, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
