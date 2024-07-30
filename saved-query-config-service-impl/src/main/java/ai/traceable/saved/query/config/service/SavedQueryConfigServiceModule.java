package ai.traceable.saved.query.config.service;

import ai.traceable.iam.v2.IamServiceGrpc;
import ai.traceable.saved.query.config.service.store.DefaultMongoDatabase;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.OptionalBinder;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

@Slf4j
public class SavedQueryConfigServiceModule extends AbstractModule {
  private final Config config;
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;
  private final GrpcChannelRegistry grpcChannelRegistry;

  SavedQueryConfigServiceModule(
      Config config,
      Channel channel,
      ConfigChangeEventGenerator changeEventGenerator,
      GrpcChannelRegistry grpcChannelRegistry) {
    this.config = config;
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(SavedQueryConfigServiceImpl.class);
    bind(Config.class).toInstance(config);
    OptionalBinder.newOptionalBinder(binder(), DefaultMongoDatabase.class);
    Optional.ofNullable(DefaultMongoDatabase.from(config))
        .ifPresentOrElse(
            defaultMongoDatabase ->
                bind(DefaultMongoDatabase.class).toInstance(defaultMongoDatabase),
            () -> log.error("Failed to create the instance of defaultMongoDatabase class"));
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  public IamServiceGrpc.IamServiceBlockingStub providesIamV2ServiceBlockingStub(
      SavedQueryDataMigrationConfig config) {
    return IamServiceGrpc.newBlockingStub(
            grpcChannelRegistry.forPlaintextAddress(
                config.getIamV2ServiceHost(), config.getIamV2ServiceGrpcPort()))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
