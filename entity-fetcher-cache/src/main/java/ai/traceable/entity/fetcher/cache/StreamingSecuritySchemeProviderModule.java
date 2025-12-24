package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.config.EntityQueryServiceConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.typesafe.config.Config;
import java.time.Clock;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceBlockingStub;
import org.hypertrace.entity.data.service.v2.EntityRelationshipServiceGrpc;
import org.hypertrace.entity.data.service.v2.EntityRelationshipServiceGrpc.EntityRelationshipServiceBlockingStub;

public class StreamingSecuritySchemeProviderModule extends AbstractModule {
  private final GrpcChannelRegistry grpcChannelRegistry;
  private final Config config;
  private final KafkaLiveEventListener<EntityChangeEventKey, EntityChangeEventValue>
      kafkaLiveEventListener;

  public StreamingSecuritySchemeProviderModule(
      GrpcChannelRegistry grpcChannelRegistry,
      Config config,
      KafkaLiveEventListener<EntityChangeEventKey, EntityChangeEventValue> kafkaLiveEventListener) {
    this.grpcChannelRegistry = grpcChannelRegistry;
    this.config = config;
    this.kafkaLiveEventListener = kafkaLiveEventListener;
  }

  @Override
  protected void configure() {
    bind(StreamingSecuritySchemeProvider.class).to(DefaultStreamingSecuritySchemeProvider.class);
    bind(SecuritySchemeProvider.class).to(SecuritySchemeProviderImpl.class);
    bind(new TypeLiteral<KafkaLiveEventListener<EntityChangeEventKey, EntityChangeEventValue>>() {})
        .toInstance(kafkaLiveEventListener);
    bind(Clock.class).toInstance(Clock.systemUTC());
  }

  @Provides
  EntityRelationshipServiceBlockingStub providesEntityRelationshipServiceStub(
      EntityQueryServiceConfig entityQueryServiceConfig) {
    return EntityRelationshipServiceGrpc.newBlockingStub(
            grpcChannelRegistry.forPlaintextAddress(
                entityQueryServiceConfig.getEntityServiceHost(),
                entityQueryServiceConfig.getEntityServicePort()))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  EntityDataServiceBlockingStub providesEntityDataServiceStub(
      EntityQueryServiceConfig entityQueryServiceConfig) {
    return EntityDataServiceGrpc.newBlockingStub(
            grpcChannelRegistry.forPlaintextAddress(
                entityQueryServiceConfig.getEntityServiceHost(),
                entityQueryServiceConfig.getEntityServicePort()))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
