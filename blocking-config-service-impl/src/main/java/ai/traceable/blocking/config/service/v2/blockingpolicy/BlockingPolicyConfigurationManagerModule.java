package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator.BlockingDetailsConverterBase;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetailsVisitor;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;
import ai.traceable.blocking.config.service.v2.blockingpolicy.exclusion.ExcludeRuleConverterImpl;
import ai.traceable.blocking.config.service.v2.blockingpolicy.exclusion.ExclusionRuleConverter;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

public class BlockingPolicyConfigurationManagerModule extends AbstractModule {
  static final String ACTOR_DETAILS_FOR_THREAT_ACTOR_VISITOR = "ActorDetailsForThreatActorVisitor";
  static final String IP_DETAILS_FOR_THREAT_ACTOR_VISITOR = "IpDetailsForThreatActorVisitor";

  @Override
  protected void configure() {
    bind(new TypeLiteral<BlockingDetailsVisitor<BlockingDetailsCondition>>() {})
        .annotatedWith(Names.named(ACTOR_DETAILS_FOR_THREAT_ACTOR_VISITOR))
        .to(ActorDetailsForThreatActorVisitor.class);

    bind(new TypeLiteral<BlockingDetailsVisitor<BlockingDetailsCondition>>() {})
        .annotatedWith(Names.named(IP_DETAILS_FOR_THREAT_ACTOR_VISITOR))
        .to(IpDetailsForThreatActorVisitor.class);

    bind(new TypeLiteral<BlockingDetailsConverterBase<BlockingDetails>>() {})
        .to(BlockingDetailsConverter.class);

    bind(ExclusionRuleConverter.class).to(ExcludeRuleConverterImpl.class);
  }
}
