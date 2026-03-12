package ai.traceable.fraud.policy.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.event.kind.EventKindConfigServiceModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class FraudPolicyConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;

  FraudPolicyConfigServiceModule(Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    install(new EventKindConfigServiceModule());
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(UuidGenerator.class).toInstance(new UuidGenerator());
    bind(BindableService.class).to(FraudPolicyConfigServiceImpl.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub
      provideEntityDerivationConfigServiceStub() {
    return EntityDerivationConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
