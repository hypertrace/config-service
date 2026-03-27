package ai.traceable.edge.decision.config.service;

import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfigServiceGrpc;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.StoredUserAttributionRuleFetcher;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.UserAttributionRuleFetcher;
import ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.enricher.VariableConstantRuleEnricher;
import ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.enricher.VariableEnricher;
import ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.fetcher.UserAttributionVariableFetcher;
import ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.fetcher.VariableFetcher;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.StoredEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.abusepolicy.AbusePolicyEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.actor.ActorEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.actor.config.ActorServiceConfig;
import ai.traceable.edge.decision.config.service.supplier.categorized.bots.CategorizedBotsEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.customsignature.CustomSignatureEdgeDecisionConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.detectionexclusion.DetectionExclusionEdgeDecisionConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.jwt.JwtExtractionEdgeDecisionConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.ratelimiting.RateLimitingEdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.supplier.userattribution.UserAttributionEdgeDecisionConfigSupplier;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
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
    bind(UserAttributionRuleFetcher.class).to(StoredUserAttributionRuleFetcher.class);
    bind(ActorServiceConfig.class).toInstance(new ActorServiceConfig(config));

    MapBinder<VariableConstants, VariableFetcher> mapBinder =
        MapBinder.newMapBinder(binder(), VariableConstants.class, VariableFetcher.class);
    mapBinder.addBinding(USER_ATTRIBUTION_VARIABLE_NAME).to(UserAttributionVariableFetcher.class);

    Multibinder<EdgeDecisionEngineConfigSupplier> configBinder =
        Multibinder.newSetBinder(binder(), EdgeDecisionEngineConfigSupplier.class);
    configBinder.addBinding().to(StoredEdgeDecisionEngineConfigSupplier.class);
    configBinder.addBinding().to(ActorEdgeDecisionEngineConfigSupplier.class);
    configBinder.addBinding().to(RateLimitingEdgeDecisionEngineConfigSupplier.class);
    configBinder.addBinding().to(DetectionExclusionEdgeDecisionConfigSupplier.class);
    configBinder.addBinding().to(CustomSignatureEdgeDecisionConfigSupplier.class);
    configBinder.addBinding().to(CategorizedBotsEdgeDecisionEngineConfigSupplier.class);
    configBinder.addBinding().to(AbusePolicyEdgeDecisionEngineConfigSupplier.class);

    Multibinder<VariableEnricher> variableEnricherMultibinder =
        Multibinder.newSetBinder(binder(), VariableEnricher.class);
    variableEnricherMultibinder.addBinding().to(VariableConstantRuleEnricher.class);
    configBinder.addBinding().to(JwtExtractionEdgeDecisionConfigSupplier.class);
    configBinder.addBinding().to(UserAttributionEdgeDecisionConfigSupplier.class);
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
  CategorizedBotConfigPolicyServiceGrpc.CategorizedBotConfigPolicyServiceBlockingStub
      providesCategorizedBotConfigPolicyServiceBlockingStub() {
    return CategorizedBotConfigPolicyServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  CategorizedBotConfigServiceGrpc.CategorizedBotConfigServiceBlockingStub
      providesCategorizedBotConfigServiceBlockingStub() {
    return CategorizedBotConfigServiceGrpc.newBlockingStub(this.channel)
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

  @Provides
  FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub
      providesFraudPolicyConfigServiceBlockingStub() {
    return FraudPolicyConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
