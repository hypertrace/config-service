package ai.traceable.sessionattribution.config.service;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionRule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class SessionAttributionConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  SessionAttributionConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(SessionAttributionConfigServiceImpl.class);
    bind(new TypeLiteral<RankCalculator<SessionAttributionRule, String>>() {})
        .toInstance(
            new RankCalculator<>(
                new RankCalculator.RankConfig<>(
                    SessionAttributionRule::getRank,
                    SessionAttributionRule::getId,
                    (rule, rank) -> rule.toBuilder().setRank(rank).build())));
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
