package ai.traceable.blocking.config.service.v2.blockingpolicy;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator.BlockingDetailsConverterBase;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

public class BlockingPolicyConfigurationManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(BlockingPolicyConfigurationManager.class)
        .to(DefaultBlockingPolicyConfigurationManager.class);
    bind(new TypeLiteral<BlockingDetailsConverterBase<BlockingDetails>>() {})
        .to(BlockingDetailsConverter.class);
  }
}
