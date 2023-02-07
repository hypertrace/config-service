package ai.traceable.blocking.config.service.v1.iptype;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase.GenericIpTypeRuleConverter;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

public class IpTypeBlockingManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(IpTypeBlockingManager.class).to(DefaultIpTypeBlockingManager.class);
    bind(new TypeLiteral<GenericIpTypeRuleConverter<IpTypeRule>>() {})
        .to(IpTypeRuleConverter.class);
  }
}
