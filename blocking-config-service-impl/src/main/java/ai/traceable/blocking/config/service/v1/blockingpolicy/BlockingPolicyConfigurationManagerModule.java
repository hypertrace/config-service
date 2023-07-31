package ai.traceable.blocking.config.service.v1.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator.BlockingDetailsConverterBase;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetailsVisitor;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingDetails.Builder;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

public class BlockingPolicyConfigurationManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(BlockingPolicyConfigurationManager.class)
        .to(DefaultBlockingPolicyConfigurationManager.class);
    bind(new TypeLiteral<BlockingDetailsVisitor<Builder>>() {})
        .to(BlockingDetailsVisitorImpl.class);
    bind(new TypeLiteral<BlockingDetailsConverterBase<BlockingDetails>>() {})
        .to(BlockingDetailsConverter.class);
  }
}
