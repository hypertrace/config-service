package ai.traceable.fraud.policy.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProviderModule;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.event.kind.EventKindConfigServiceModule;
import ai.traceable.fraud.policy.config.service.converter.AbusePolicyEdgeDecisionConverter;
import ai.traceable.fraud.policy.config.service.converter.ApiScopeResolver;
import ai.traceable.fraud.policy.config.service.converter.DetectionFilterConverter;
import ai.traceable.fraud.policy.config.service.converter.EntityJexlResolver;
import ai.traceable.fraud.policy.config.service.converter.SimpleAggregationTemplateConverter;
import ai.traceable.fraud.policy.config.service.converter.TemplateEdgeDecisionConverter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class FraudPolicyConfigServiceModule extends AbstractModule {
  private static final String CACHED_SERVICE_MAPPING_NAME =
      "serviceMappingCache-fraudPolicyConfigService";

  private final Channel channel;
  private final Config config;
  private final ConfigChangeEventGenerator changeEventGenerator;
  private final GrpcChannelRegistry grpcChannelRegistry;

  FraudPolicyConfigServiceModule(
      Channel channel,
      Config config,
      ConfigChangeEventGenerator changeEventGenerator,
      GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.config = config;
    this.changeEventGenerator = changeEventGenerator;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    install(new EventKindConfigServiceModule());
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(UuidGenerator.class).toInstance(new UuidGenerator());
    bind(EntityJexlResolver.class).asEagerSingleton();
    bind(DetectionFilterConverter.class).asEagerSingleton();
    install(
        new CachedServiceMappingProviderModule(
            grpcChannelRegistry, config, CACHED_SERVICE_MAPPING_NAME));
    bind(ApiScopeResolver.class).asEagerSingleton();
    MapBinder<AbusePolicyData.TemplateConfigCase, TemplateEdgeDecisionConverter>
        templateConverterBinder =
            MapBinder.newMapBinder(
                binder(),
                AbusePolicyData.TemplateConfigCase.class,
                TemplateEdgeDecisionConverter.class);
    templateConverterBinder
        .addBinding(AbusePolicyData.TemplateConfigCase.SIMPLE_AGGREGATION_TEMPLATE)
        .to(SimpleAggregationTemplateConverter.class);
    bind(AbusePolicyEdgeDecisionConverter.class).asEagerSingleton();
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
