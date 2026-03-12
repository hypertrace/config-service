package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.event.kind.EventKindConfigServiceModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class EntityDerivationConfigServiceModule extends AbstractModule {
  private final Config config;
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;

  EntityDerivationConfigServiceModule(
      Config config, Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    this.config = config;
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    install(new EventKindConfigServiceModule());
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(EntityDerivationConfigServiceImpl.class);
    bind(Config.class).toInstance(config);
    bind(UuidGenerator.class).toInstance(new UuidGenerator());
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
