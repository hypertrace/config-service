package ai.traceable.userattribution.config.service.v1;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.config.utils.RankCalculator.RankConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class UserAttributionConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  UserAttributionConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(UserAttributionConfigServiceImpl.class);
    bind(new TypeLiteral<RankCalculator<UserAttributionRule, String>>() {})
        .toInstance(
            new RankCalculator<>(
                new RankConfig<>(
                    UserAttributionRule::getRank,
                    UserAttributionRule::getId,
                    (rule, rank) -> rule.toBuilder().setRank(rank).build())));
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
