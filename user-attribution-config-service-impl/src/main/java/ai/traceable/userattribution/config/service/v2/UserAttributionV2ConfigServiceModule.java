package ai.traceable.userattribution.config.service.v2;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.RankCalculator;
import ai.traceable.userattribution.config.service.v2.migration.LegacyUserAttributionRuleTranslatingDao;
import ai.traceable.userattribution.config.service.v2.migration.LegacyUserAttributionRuleTranslatingDaoImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class UserAttributionV2ConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final FeatureCachingClient featureCachingClient;

  UserAttributionV2ConfigServiceModule(
      Channel channel,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient) {
    this.channel = channel;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  protected void configure() {
    bind(FeatureCachingClient.class).toInstance(this.featureCachingClient);
    bind(BindableService.class).to(UserAttributionV2ConfigServiceImpl.class);
    bind(new TypeLiteral<RankCalculator<UserAttributionRule, String>>() {})
        .toInstance(
            new RankCalculator<>(
                new RankCalculator.RankConfig<>(
                    UserAttributionRule::getRank,
                    UserAttributionRule::getId,
                    (rule, rank) -> rule.toBuilder().setRank(rank).build())));
    bind(new TypeLiteral<
            RankCalculator<
                ai.traceable.userattribution.config.service.v1.UserAttributionRule, String>>() {})
        .toInstance(
            new RankCalculator<>(
                new RankCalculator.RankConfig<>(
                    ai.traceable.userattribution.config.service.v1.UserAttributionRule::getRank,
                    ai.traceable.userattribution.config.service.v1.UserAttributionRule::getId,
                    (rule, rank) -> rule.toBuilder().setRank(rank).build())));
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(LegacyUserAttributionRuleTranslatingDao.class)
        .to(LegacyUserAttributionRuleTranslatingDaoImpl.class);
  }

  @Provides
  ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
