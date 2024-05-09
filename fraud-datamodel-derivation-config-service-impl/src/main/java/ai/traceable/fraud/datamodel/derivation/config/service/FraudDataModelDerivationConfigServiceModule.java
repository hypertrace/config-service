package ai.traceable.fraud.datamodel.derivation.config.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class FraudDataModelDerivationConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;

  FraudDataModelDerivationConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(FraudDataModelDerivationConfigServiceImpl.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
