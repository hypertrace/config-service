package ai.traceable.edge.config.service;

import ai.traceable.edge.bot.config.service.v1.BotConfigServiceGrpc;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionConfigServiceGrpc.EdgeDecisionConfigServiceBlockingStub;
import ai.traceable.policy.config.service.v1.TraceablePolicyConfigServiceGrpc;
import ai.traceable.policy.config.service.v1.TraceablePolicyConfigServiceGrpc.TraceablePolicyConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class TraceableEdgeConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final TraceableEdgeConfig config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  public TraceableEdgeConfigServiceModule(
      Channel channel, Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.channel = channel;
    this.config = new TraceableEdgeConfig(config);
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(Channel.class).toInstance(channel);
    bind(GrpcChannelRegistry.class).toInstance(grpcChannelRegistry);
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(TraceableEdgeConfig.class).toInstance(config);
    bind(BindableService.class).to(TraceableEdgeConfigService.class);
  }

  @Provides
  TraceablePolicyConfigServiceBlockingStub providesTraceablePolicyConfigServiceStub(
      Channel channel) {
    return TraceablePolicyConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  BotConfigServiceGrpc.BotConfigServiceBlockingStub providesCaptchaSiteKeyConfigServiceStub(
      Channel channel) {
    return BotConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  EdgeDecisionConfigServiceBlockingStub providesEdgeDecisionConfigServiceStub(Channel channel) {
    return EdgeDecisionConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
