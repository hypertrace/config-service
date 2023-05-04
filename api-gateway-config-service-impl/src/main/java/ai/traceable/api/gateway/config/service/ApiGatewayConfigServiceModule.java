package ai.traceable.api.gateway.config.service;

import ai.traceable.api.gateway.config.service.delegate.ApiGatewayDelegateModule;
import ai.traceable.api.gateway.config.service.filter.ApiGatewayFilterModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import lombok.AllArgsConstructor;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

@AllArgsConstructor
public class ApiGatewayConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  @Override
  protected void configure() {
    bind(BindableService.class).to(ApiGatewayConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    install(new ApiGatewayDelegateModule());
    install(new ApiGatewayFilterModule());
  }

  @SuppressWarnings("unused")
  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
