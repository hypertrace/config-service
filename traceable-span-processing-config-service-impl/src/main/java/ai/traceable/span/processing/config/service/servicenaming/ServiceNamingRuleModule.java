package ai.traceable.span.processing.config.service.servicenaming;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.config.utils.RankCalculator.RankConfig;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class ServiceNamingRuleModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ServiceNamingRulesManager.class).to(ServiceNamingRuleManagerImpl.class);
    bind(new TypeLiteral<RankCalculator<ServiceNamingRule, String>>() {})
        .toInstance(
            new RankCalculator<>(
                new RankConfig<>(
                    ServiceNamingRule::getRank,
                    ServiceNamingRule::getId,
                    (rule, rank) -> rule.toBuilder().setRank(rank).build())));
    requireBinding(ConfigChangeEventGenerator.class);
    requireBinding(ConfigServiceBlockingStub.class);
  }
}
