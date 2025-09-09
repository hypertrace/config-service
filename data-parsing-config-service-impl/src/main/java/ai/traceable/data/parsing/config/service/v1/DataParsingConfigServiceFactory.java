package ai.traceable.data.parsing.config.service.v1;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class DataParsingConfigServiceFactory {

  public static BindableService build(
      Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    Injector injector =
        Guice.createInjector(new DataParsingConfigServiceModule(channel, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
