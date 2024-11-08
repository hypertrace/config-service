package ai.traceable.saved.filter.config.service;

import ai.traceable.saved.filter.config.service.validation.SavedFilterValidationModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.client.config.AttributeServiceCachedClientConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class SavedFilterConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;
  private final Config config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  SavedFilterConfigServiceModule(
      Channel channel,
      ConfigChangeEventGenerator changeEventGenerator,
      Config config,
      GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(SavedFilterConfigServiceImpl.class);
    install(new SavedFilterValidationModule());
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  @Singleton
  AttributeServiceCachedClient provideAttributeServiceCachedClient() {
    return new AttributeServiceCachedClient(
        this.grpcChannelRegistry.forPlaintextAddress(
            this.config.getString("attribute.service.config.host"),
            this.config.getInt("attribute.service.config.port")),
        AttributeServiceCachedClientConfig.from(this.config.getConfig("attribute.service.config")));
  }
}
