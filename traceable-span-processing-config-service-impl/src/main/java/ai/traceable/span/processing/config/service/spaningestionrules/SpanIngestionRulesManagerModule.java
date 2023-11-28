package ai.traceable.span.processing.config.service.spaningestionrules;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.span.processing.config.service.impl.v1.PersistedKeyValueRetentionRule;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.typesafe.config.Config;

public class SpanIngestionRulesManagerModule extends AbstractModule {

  private final Config config;

  public SpanIngestionRulesManagerModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(SpanIngestionRulesManager.class).to(SpanIngestionRulesManagerImpl.class);
    bind(SpanIngestionRulesConfig.class).toInstance(new SpanIngestionRulesConfig(config));
    bind(new TypeLiteral<RankCalculator<PersistedKeyValueRetentionRule, String>>() {})
        .toInstance(
            new RankCalculator<>(
                new RankCalculator.RankConfig<>(
                    PersistedKeyValueRetentionRule::getRank,
                    PersistedKeyValueRetentionRule::getId,
                    (rule, rank) -> rule.toBuilder().setRank(rank).build())));
  }
}
