package ai.traceable.saved.filter.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class SavedFilterConfigServiceFactory {

  private SavedFilterConfigServiceFactory() {}

  public static BindableService build(
      Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION, new SavedFilterConfigServiceModule(channel, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
