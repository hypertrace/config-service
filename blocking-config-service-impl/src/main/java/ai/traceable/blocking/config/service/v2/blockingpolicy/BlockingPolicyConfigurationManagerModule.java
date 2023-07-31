package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator.BlockingDetailsConverterBase;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetailsVisitor;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

public class BlockingPolicyConfigurationManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(new TypeLiteral<BlockingDetailsVisitor<BlockingDetailsCondition>>() {})
        .to(BlockingDetailsVisitorImpl.class);
    bind(new TypeLiteral<BlockingDetailsConverterBase<BlockingDetails>>() {})
        .to(BlockingDetailsConverter.class);
  }
}
