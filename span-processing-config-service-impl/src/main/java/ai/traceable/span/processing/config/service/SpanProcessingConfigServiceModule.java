package ai.traceable.span.processing.config.service;

import ai.traceable.span.processing.config.service.licensestatus.LicenseStatusConfigManagerModule;
import ai.traceable.span.processing.config.service.samplingconfigs.SamplingConfigManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class SpanProcessingConfigServiceModule extends AbstractModule {

  private final ManagedChannel channel;
  private final Config config;

  public SpanProcessingConfigServiceModule(ManagedChannel channel, Config config) {
    this.channel = channel;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(SpanProcessingConfigServiceImpl.class);
    bind(ManagedChannel.class).toInstance(channel);

    install(new LicenseStatusConfigManagerModule());
    install(new SamplingConfigManagerModule());
  }

  @Provides
  public Config providesConfig() {
    return this.config;
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
