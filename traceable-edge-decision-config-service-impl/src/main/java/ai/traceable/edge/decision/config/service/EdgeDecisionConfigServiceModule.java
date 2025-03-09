package ai.traceable.edge.decision.config.service;

import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;

import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.aggregator.attributes.UserAttributionVariableEnricher;
import ai.traceable.edge.decision.config.service.aggregator.attributes.VariableEnricherBase;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.StoredUserAttributionFetcher;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.UserAttributionFetcher;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.StoredEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.actor.ActorEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.actor.config.ActorServiceConfig;
import ai.traceable.edge.decision.config.service.supplier.customsignature.CustomSignatureEdgeDecisionConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.detectionexclusion.DetectionExclusionEdgeDecisionConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.jwt.JwtExtractionEdgeDecisionConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.ratelimiting.RateLimitingEdgeDecisionEngineConfigSupplier;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionConfigServiceGrpc;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionConfigServiceGrpc.JwtExtractionConfigServiceBlockingStub;
import ai.traceable.platform.actor.v1.ActorServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v2.UserAttributionConfigServiceGrpc;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class EdgeDecisionConfigServiceModule extends AbstractModule {
  private final Config config;
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;

  EdgeDecisionConfigServiceModule(
      Config config, Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    this.config = config;
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(Channel.class).toInstance(channel);
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(Config.class).toInstance(config);
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(BindableService.class).to(EdgeDecisionConfigService.class);
    bind(UserAttributionFetcher.class).to(StoredUserAttributionFetcher.class);
    bind(ActorServiceConfig.class).toInstance(new ActorServiceConfig(config));

    MapBinder<VariableConstants, VariableEnricherBase> mapBinder =
        MapBinder.newMapBinder(binder(), VariableConstants.class, VariableEnricherBase.class);
    mapBinder.addBinding(USER_ATTRIBUTION_VARIABLE_NAME).to(UserAttributionVariableEnricher.class);

    Multibinder<EdgeDecisionEngineConfigSupplier> configBinder =
        Multibinder.newSetBinder(binder(), EdgeDecisionEngineConfigSupplier.class);
    configBinder.addBinding().to(StoredEdgeDecisionEngineConfigSupplier.class);
    configBinder.addBinding().to(ActorEdgeDecisionEngineConfigSupplier.class);
    configBinder.addBinding().to(RateLimitingEdgeDecisionEngineConfigSupplier.class);
    configBinder.addBinding().to(DetectionExclusionEdgeDecisionConfigSupplier.class);
    configBinder.addBinding().to(CustomSignatureEdgeDecisionConfigSupplier.class);
    configBinder.addBinding().to(JwtExtractionEdgeDecisionConfigSupplier.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  ActorServiceGrpc.ActorServiceBlockingStub providesActorServiceBlockingStub(
      ActorServiceConfig actorServiceConfig, GrpcChannelRegistry channelRegistry) {
    return ActorServiceGrpc.newBlockingStub(
            channelRegistry.forPlaintextAddress(
                actorServiceConfig.getHost(), actorServiceConfig.getPort()))
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub
      providesRateLimitingConfigServiceBlockingStub() {
    return RateLimitingConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub
      provideDetectionExclusionConfigServiceBlockingStub() {
    return DetectionExclusionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub
      providesUserAttributionConfigServiceBlockingStub() {
    return UserAttributionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  CustomSignatureConfigServiceBlockingStub providesCustomSignatureConfigServiceBlockingStub() {
    return CustomSignatureConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  JwtExtractionConfigServiceBlockingStub providesJwtExtractionConfigServiceBlockingStub() {
    return JwtExtractionConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
