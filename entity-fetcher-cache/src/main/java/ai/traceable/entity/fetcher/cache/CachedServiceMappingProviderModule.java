package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.config.CachedEntityFetcherConfig;
import ai.traceable.entity.fetcher.cache.config.EntityQueryServiceConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.typesafe.config.Config;
import java.time.Clock;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;

public class CachedServiceMappingProviderModule extends AbstractModule {
  public static final String SERVICE_MAPPING_CACHE_NAME = "serviceMappingCache";
  private final GrpcChannelRegistry grpcChannelRegistry;
  private final Config config;
  private final String serviceMappingCacheName;

  public CachedServiceMappingProviderModule(
      GrpcChannelRegistry grpcChannelRegistry, Config config, String serviceMappingCacheName) {
    this.grpcChannelRegistry = grpcChannelRegistry;
    this.config = config;
    this.serviceMappingCacheName = serviceMappingCacheName;
  }

  @Override
  protected void configure() {
    bind(CachedServiceMappingProvider.class).to(DefaultCachedServiceMappingProvider.class);
    bind(CachedApiMappingProvider.class).to(DefaultCachedApiMappingProvider.class);
    bind(StreamingApiMappingProvider.class).to(DefaultStreamingApiMappingProvider.class);
    bind(Clock.class).toInstance(Clock.systemUTC());
  }

  @Provides
  @Named(SERVICE_MAPPING_CACHE_NAME)
  String providesServiceMappingCacheName() {
    return this.serviceMappingCacheName;
  }

  @Provides
  EntityQueryServiceConfig providesEntityDataServiceConfig() {
    return new EntityQueryServiceConfig(this.config);
  }

  @Provides
  CachedEntityFetcherConfig providesCachedEntityFetcherConfig() {
    return new CachedEntityFetcherConfig(this.config);
  }

  @Provides
  public EntityQueryServiceBlockingStub providesEntityQueryServiceBlockingStub(
      EntityQueryServiceConfig entityQueryServiceConfig) {
    return EntityQueryServiceGrpc.newBlockingStub(
            grpcChannelRegistry.forPlaintextAddress(
                entityQueryServiceConfig.getEntityServiceHost(),
                entityQueryServiceConfig.getEntityServicePort()))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
