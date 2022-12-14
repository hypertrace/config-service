package ai.traceable.ast.scan.profile.config.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class AstScanProfileConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final Config config;

  public AstScanProfileConfigServiceModule(Channel channel, Config config) {
    this.channel = channel;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(AstScanProfileConfigServiceImpl.class);
    bind(Config.class).toInstance(config);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
