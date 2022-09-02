package org.hypertrace.span.processing.config.service;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.span.processing.config.service.apinamingrules.ApiNamingRulesManagerModule;

public class SpanProcessingConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final Config config;

  SpanProcessingConfigServiceModule(Channel channel, Config config) {
    this.channel = channel;
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(SpanProcessingConfigServiceImpl.class);
    bind(Config.class).toInstance(config);

    install(new ApiNamingRulesManagerModule());
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
