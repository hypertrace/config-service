package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

public class EntityDerivationConfigServiceFactory {

  private EntityDerivationConfigServiceFactory() {}

  public static BindableService build(
      Config config, Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    Injector injector =
        Guice.createInjector(
            Stage.PRODUCTION,
            new EntityDerivationConfigServiceModule(config, channel, changeEventGenerator));
    return injector.getInstance(BindableService.class);
  }
}
