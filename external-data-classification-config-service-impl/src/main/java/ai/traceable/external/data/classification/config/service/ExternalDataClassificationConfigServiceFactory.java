package ai.traceable.external.data.classification.config.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;

public class ExternalDataClassificationConfigServiceFactory {
  public static BindableService build(Channel channel, Config config) {
    Injector injector =
        Guice.createInjector(new ExternalDataClassificationConfigServiceModule(channel, config));
    return injector.getInstance(BindableService.class);
  }
}
